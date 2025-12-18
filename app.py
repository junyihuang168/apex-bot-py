import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_market_price,
    get_fill_summary,
    get_open_position_for_symbol,
    _get_symbol_rules,
    _snap_quantity,
    start_private_ws,
    start_order_rest_poller,
    pop_order_event,
    create_trigger_order,
    snap_price_for_order,
    cancel_order,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
    get_symbol_open_directions,
    clear_lock_level_pct,
    is_signal_processed,
    mark_signal_processed,
    # ✅ protective orders
    set_protective_orders,
    get_protective_orders,
    clear_protective_orders,
    find_protective_owner_by_order_id,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ✅ 只让 worker 进程启用 WS/Fills（supervisord.conf 里给 worker 设置 ENABLE_WS="1"，web 设置 "0"）
ENABLE_WS = str(os.getenv("ENABLE_WS", "0")).strip() == "1"

# 退出互斥窗口（秒）：防止重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}

# ✅ 固定止损/止盈（百分比）
FIXED_SL_PCT = Decimal(os.getenv("FIXED_SL_PCT", "0.5"))  # -0.5%
FIXED_TP_PCT = Decimal(os.getenv("FIXED_TP_PCT", "1.0"))  # +1.0%

# ✅ 保护单模式：MARKET（默认稳） or LIMIT
PROTECTIVE_ORDER_MODE = str(os.getenv("PROTECTIVE_ORDER_MODE", "MARKET")).upper().strip()
PROTECTIVE_SLIPPAGE_PCT = Decimal(os.getenv("PROTECTIVE_SLIPPAGE_PCT", "0.15"))  # LIMIT 模式滑点 0.15%

# ✅ TP/SL 订单有效期（秒时间戳），默认 30 天
PROTECTIVE_EXPIRE_SEC = int(os.getenv("PROTECTIVE_EXPIRE_SEC", str(30 * 24 * 3600)))

# 本地 cache（仅辅助）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ✅ 退出互斥：bot+symbol 粒度 cooldown
_EXIT_LOCK = threading.Lock()
_LAST_EXIT_TS: Dict[Tuple[str, str], float] = {}

# ✅ WS fills 处理线程
_FILLS_THREAD_STARTED = False
_FILLS_LOCK = threading.Lock()


def _require_token() -> bool:
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip().upper() for b in env_val.split(",") if b.strip()}


def _canon_bot_id(bot_id: str) -> str:
    s = str(bot_id or "").strip().upper().replace(" ", "").replace("-", "_")
    if not s:
        return "BOT_0"
    if s.startswith("BOT_"):
        core = s[4:]
    elif s.startswith("BOT"):
        core = s[3:]
    else:
        core = s
    core = core.replace("_", "")
    if core.isdigit():
        return f"BOT_{int(core)}"
    return s


def _bot_num(bot_id: str) -> int:
    b = _canon_bot_id(bot_id)
    try:
        return int(b.split("_", 1)[1])
    except Exception:
        return 0


# ----------------------------
# ✅ BOT 分组（按你要求）
# ----------------------------
LONG_TPSL_BOTS = _parse_bot_list(os.getenv("LONG_TPSL_BOTS", ",".join([f"BOT_{i}" for i in range(1, 11)])))
SHORT_TPSL_BOTS = _parse_bot_list(os.getenv("SHORT_TPSL_BOTS", ",".join([f"BOT_{i}" for i in range(11, 21)])))
LONG_PNL_ONLY_BOTS = _parse_bot_list(os.getenv("LONG_PNL_ONLY_BOTS", ",".join([f"BOT_{i}" for i in range(21, 31)])))
SHORT_PNL_ONLY_BOTS = _parse_bot_list(os.getenv("SHORT_PNL_ONLY_BOTS", ",".join([f"BOT_{i}" for i in range(31, 41)])))


def _bot_expected_entry_side(bot_id: str) -> Optional[str]:
    b = _canon_bot_id(bot_id)
    if b in LONG_TPSL_BOTS or b in LONG_PNL_ONLY_BOTS:
        return "BUY"
    if b in SHORT_TPSL_BOTS or b in SHORT_PNL_ONLY_BOTS:
        return "SELL"
    return None


def _bot_has_exchange_brackets(bot_id: str) -> bool:
    b = _canon_bot_id(bot_id)
    return (b in LONG_TPSL_BOTS) or (b in SHORT_TPSL_BOTS)


def _exit_guard_allow(bot_id: str, symbol: str) -> bool:
    key = (_canon_bot_id(bot_id), str(symbol or "").upper().strip())
    now = time.time()
    with _EXIT_LOCK:
        last = _LAST_EXIT_TS.get(key, 0.0)
        if now - last < EXIT_COOLDOWN_SEC:
            return False
        _LAST_EXIT_TS[key] = now
        return True


# ----------------------------
# 预算提取
# ----------------------------
def _extract_budget_usdt(body: dict) -> Decimal:
    size_field = (
        body.get("position_size_usdt")
        or body.get("size_usdt")
        or body.get("size")
    )
    if size_field is None:
        raise ValueError("missing position_size_usdt / size_usdt / size")

    budget = Decimal(str(size_field))
    if budget <= 0:
        raise ValueError("size_usdt must be > 0")
    return budget


# ----------------------------
# 预算 -> snapped qty
# ----------------------------
def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    ref_price_dec = Decimal(get_market_price(symbol, side, str(min_qty)))
    theoretical_qty = budget / ref_price_dec
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    if snapped_qty <= 0:
        raise ValueError(f"snapped_qty <= 0, symbol={symbol}, budget={budget}")

    return snapped_qty


def _order_status_and_reason(order: dict):
    data = (order or {}).get("data", {}) or {}
    status = str(data.get("status", "")).upper()
    cancel_reason = str(
        data.get("cancelReason")
        or data.get("rejectReason")
        or data.get("errorMessage")
        or ""
    )
    return status, cancel_reason


# ----------------------------
# 幂等 Signal ID（webhook）
# ----------------------------
def _get_signal_id(body: dict, mode: str, bot_id: str, symbol: str) -> str:
    for k in ("signal_id", "alert_id", "id", "tv_id", "client_id"):
        v = body.get(k)
        if v:
            return f"{mode}:{bot_id}:{symbol}:{str(v)}"

    ts = body.get("ts") or body.get("time") or int(time.time())
    try:
        ts_int = int(float(str(ts)))
    except Exception:
        ts_int = int(time.time())

    sig = str(body.get("signal_type") or body.get("action") or "").lower().strip()
    side = str(body.get("side") or "").upper().strip()

    return f"{mode}:{bot_id}:{symbol}:{sig}:{side}:{ts_int}"


# ----------------------------
# ✅ 交易所 TP/SL（固定百分比）下单
# ----------------------------
def _compute_fixed_bracket_prices(direction: str, entry_price: Decimal) -> Tuple[Decimal, Decimal]:
    """
    return (sl_trigger, tp_trigger)
    """
    if entry_price <= 0:
        return Decimal("0"), Decimal("0")

    sl_pct = FIXED_SL_PCT / Decimal("100")
    tp_pct = FIXED_TP_PCT / Decimal("100")

    if direction.upper() == "LONG":
        sl = entry_price * (Decimal("1") - sl_pct)
        tp = entry_price * (Decimal("1") + tp_pct)
    else:
        sl = entry_price * (Decimal("1") + sl_pct)
        tp = entry_price * (Decimal("1") - tp_pct)

    return sl, tp


def _apply_limit_slippage(direction: str, trigger: Decimal) -> Decimal:
    """
    LIMIT 模式：给“更差一点”的限价，提升成交概率。
    LONG 平仓 SELL：更低一点
    SHORT 平仓 BUY：更高一点
    """
    slip = (PROTECTIVE_SLIPPAGE_PCT / Decimal("100"))
    d = direction.upper()
    if d == "LONG":
        return trigger * (Decimal("1") - slip)
    else:
        return trigger * (Decimal("1") + slip)


def _cancel_existing_brackets(bot_id: str, symbol: str, direction: str):
    po = get_protective_orders(bot_id, symbol, direction)
    if not po:
        return
    sl_oid = po.get("sl_order_id")
    tp_oid = po.get("tp_order_id")
    if sl_oid:
        try:
            cancel_order(str(sl_oid))
        except Exception:
            pass
    if tp_oid:
        try:
            cancel_order(str(tp_oid))
        except Exception:
            pass
    try:
        clear_protective_orders(bot_id, symbol, direction)
    except Exception:
        pass


def _place_fixed_brackets(bot_id: str, symbol: str, direction: str, qty: Decimal, entry_price: Decimal):
    """
    BOT_1-20 才会走这里：
    - 下 SL & TP（reduceOnly=True）
    - 记录到 DB protective_orders，供 WS fills 自动记账 + OCO
    """
    bot_id = _canon_bot_id(bot_id)
    symbol = str(symbol).upper().strip()
    direction = direction.upper()

    if qty <= 0 or entry_price <= 0:
        return

    # 先取消旧的（避免重复挂单）
    _cancel_existing_brackets(bot_id, symbol, direction)

    sl_trigger, tp_trigger = _compute_fixed_bracket_prices(direction, entry_price)
    if sl_trigger <= 0 or tp_trigger <= 0:
        return

    expiration = int(time.time()) + int(PROTECTIVE_EXPIRE_SEC)
    exit_side = "SELL" if direction == "LONG" else "BUY"

    if PROTECTIVE_ORDER_MODE == "LIMIT":
        sl_type = "STOP_LIMIT"
        tp_type = "TAKE_PROFIT_LIMIT"
        sl_price = _apply_limit_slippage(direction, sl_trigger)
        tp_price = _apply_limit_slippage(direction, tp_trigger)
    else:
        sl_type = "STOP_MARKET"
        tp_type = "TAKE_PROFIT_MARKET"
        sl_price = None
        tp_price = None

    # Snap trigger prices to tick size for clean logs and added safety (API also snaps as last-mile).
    try:
        sl_trigger = snap_price_for_order(symbol, exit_side, sl_type, sl_trigger)
        tp_trigger = snap_price_for_order(symbol, exit_side, tp_type, tp_trigger)
        if sl_price is not None:
            sl_price = snap_price_for_order(symbol, exit_side, sl_type, sl_price)
        if tp_price is not None:
            tp_price = snap_price_for_order(symbol, exit_side, tp_type, tp_price)
    except Exception as e:
        print(f"[BRACKET] WARNING snap price failed: {e}")

    bnum = _bot_num(bot_id)
    ts = int(time.time())
    sl_client = f"{bnum:03d}{ts}01"
    tp_client = f"{bnum:03d}{ts}02"

    sl_res = create_trigger_order(
        symbol=symbol,
        side=exit_side,
        order_type=sl_type,
        size=str(qty),
        trigger_price=str(sl_trigger),
        price=str(sl_price) if sl_price is not None else None,
        reduce_only=True,
        client_id=sl_client,
        expiration_sec=expiration,
    )
    tp_res = create_trigger_order(
        symbol=symbol,
        side=exit_side,
        order_type=tp_type,
        size=str(qty),
        trigger_price=str(tp_trigger),
        price=str(tp_price) if tp_price is not None else None,
        reduce_only=True,
        client_id=tp_client,
        expiration_sec=expiration,
    )

    sl_oid = sl_res.get("order_id")
    tp_oid = tp_res.get("order_id")

    set_protective_orders(
        bot_id=bot_id,
        symbol=symbol,
        direction=direction,
        sl_order_id=str(sl_oid) if sl_oid else None,
        tp_order_id=str(tp_oid) if tp_oid else None,
        sl_client_id=sl_res.get("client_order_id") or sl_client,
        tp_client_id=tp_res.get("client_order_id") or tp_client,
        sl_price=sl_trigger,
        tp_price=tp_trigger,
        is_active=True,
    )

    print(
        f"[BRACKET] set bot={bot_id} {direction} {symbol} qty={qty} entry={entry_price} "
        f"SL({sl_type}) trigger={sl_trigger} oid={sl_oid} | TP({tp_type}) trigger={tp_trigger} oid={tp_oid}"
    )


# ----------------------------
# ✅ WS orders -> 自动记账 + OCO（NO fills subscription）
# ----------------------------
def _orders_loop():
    print("[WS-ORDERS] loop started")
    while True:
        evt = pop_order_event(timeout=0.5)
        if not evt:
            continue

        try:
            if str(evt.get("type") or "") != "order_delta":
                continue

            order_id = str(evt.get("order_id") or "")
            symbol = str(evt.get("symbol") or "").upper().strip()
            status = str(evt.get("status") or "").upper().strip()

            delta_qty = evt.get("delta_qty")
            delta_px = evt.get("delta_price")
            cum_qty = evt.get("cum_filled_qty")

            if not order_id or not symbol or delta_qty is None or delta_px is None or cum_qty is None:
                continue

            dq = Decimal(str(delta_qty))
            dp = Decimal(str(delta_px))
            if dq <= 0 or dp <= 0:
                continue

            owner = find_protective_owner_by_order_id(order_id)
            if not owner:
                continue

            bot_id = _canon_bot_id(owner["bot_id"])
            direction = str(owner["direction"]).upper()
            kind = str(owner.get("kind") or "").upper()  # SL / TP

            # Idempotency: per (order_id, cum_qty)
            sig_id = f"orderfill:{order_id}:{cum_qty}"
            if is_signal_processed(bot_id, sig_id):
                continue
            mark_signal_processed(bot_id, sig_id, kind=f"ws_order_{kind.lower()}_delta")

            entry_side = "BUY" if direction == "LONG" else "SELL"

            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=dq,
                exit_price=dp,
                reason=f"exchange_{kind.lower()}_order_delta",
            )

            opens = get_bot_open_positions(bot_id)
            remaining = opens.get((symbol, direction), {}).get("qty", Decimal("0"))

            po = get_protective_orders(bot_id, symbol, direction)
            sl_oid = po.get("sl_order_id") if po else None
            tp_oid = po.get("tp_order_id") if po else None

            if remaining <= 0:
                other_oid = tp_oid if kind == "SL" else sl_oid
                if other_oid:
                    try:
                        cancel_order(str(other_oid))
                    except Exception:
                        pass

                try:
                    clear_protective_orders(bot_id, symbol, direction)
                except Exception:
                    pass
                try:
                    clear_lock_level_pct(bot_id, symbol, direction)
                except Exception:
                    pass

                key_local = (bot_id, symbol)
                if key_local in BOT_POSITIONS:
                    BOT_POSITIONS[key_local]["qty"] = Decimal("0")
                    BOT_POSITIONS[key_local]["entry_price"] = None

                print(f"[WS-ORDERS] position closed by {kind}. bot={bot_id} {direction} {symbol} now flat. OCO done.")
            else:
                print(
                    f"[WS-ORDERS] {kind} delta bot={bot_id} {direction} {symbol} "
                    f"delta_qty={dq} delta_px={dp} remaining={remaining} status={status}"
                )

        except Exception as e:
            print("[WS-ORDERS] error:", e)


def _ensure_ws_orders_thread():
    global _FILLS_THREAD_STARTED
    with _FILLS_LOCK:
        if _FILLS_THREAD_STARTED:
            return
        t = threading.Thread(target=_orders_loop, daemon=True)
        t.start()
        _FILLS_THREAD_STARTED = True
        print("[WS-ORDERS] thread created")


def _ensure_monitor_thread():
    """
    ✅ 关键：只在 ENABLE_WS=1 的进程里启动 WS + orders 线程
    web(gunicorn) 进程只 init_db，不开 WS
    worker 进程 init_db + 开 WS + 开 orders
    """
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_THREAD_STARTED:
            return

        init_db()

        if ENABLE_WS:
            start_private_ws()
            _ensure_ws_orders_thread()

            # ✅ Backup path: REST poll orders every N seconds (main path still WS)
            if str(os.getenv("ENABLE_REST_POLL", "1")).strip() == "1":
                try:
                    start_order_rest_poller(poll_interval=float(os.getenv("REST_ORDER_POLL_INTERVAL", "5.0")))
                    print("[SYSTEM] REST order poller enabled (backup path)")
                except Exception as e:
                    print("[SYSTEM] REST order poller failed to start:", e)

            print("[SYSTEM] WS enabled in this process (ENABLE_WS=1)")
        else:
            print("[SYSTEM] WS disabled in this process (ENABLE_WS=0)")

        _MONITOR_THREAD_STARTED = True
        print("[SYSTEM] monitor/ws threads ready")


@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200


@app.route("/api/pnl", methods=["GET"])
def api_pnl():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    bots = list_bots_with_activity()
    only_bot = request.args.get("bot_id")
    if only_bot:
        only_bot = _canon_bot_id(only_bot)
        bots = [b for b in bots if _canon_bot_id(b) == only_bot]

    out = []
    for bot_id in bots:
        bot_id = _canon_bot_id(bot_id)
        base = get_bot_summary(bot_id)
        opens = get_bot_open_positions(bot_id)

        unrealized = Decimal("0")
        open_rows = []

        for (symbol, direction), v in opens.items():
            qty = v["qty"]
            wentry = v["weighted_entry"]
            if qty <= 0:
                continue

            try:
                rules = _get_symbol_rules(symbol)
                min_qty = rules["min_qty"]
                exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
                px_str = get_market_price(symbol, exit_side, str(min_qty))
                px = Decimal(str(px_str))
            except Exception:
                continue

            if direction.upper() == "LONG":
                unrealized += (px - wentry) * qty
            else:
                unrealized += (wentry - px) * qty

            open_rows.append({
                "symbol": str(symbol).upper(),
                "direction": direction.upper(),
                "qty": str(qty),
                "weighted_entry": str(wentry),
                "mark_price": str(px),
            })

        base.update({
            "unrealized": str(unrealized),
            "open_positions": open_rows,
        })
        out.append(base)

    def _rt(x):
        try:
            return Decimal(str(x.get("realized_total", "0")))
        except Exception:
            return Decimal("0")

    out.sort(key=_rt, reverse=True)
    return jsonify({"ts": int(time.time()), "bots": out}), 200


@app.route("/dashboard", methods=["GET"])
def dashboard():
    # ✅ Mobile-friendly dashboard (HTML)
    # Security model:
    # - /api/pnl is protected by DASHBOARD_TOKEN (if set).
    # - /dashboard can load without a token; the page will ask for token if needed.
    _ensure_monitor_thread()

    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Apex Bot PnL Dashboard</title>
  <style>
    :root { --bg:#0b1020; --card:#121a33; --muted:#9aa4b2; --text:#eef2ff; --line:#24304f; --bad:#ff6b6b; --good:#6bff95; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; background:var(--bg); color:var(--text); }
    a { color:inherit; }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 16px; }
    .top { display:flex; flex-direction:column; gap:12px; }
    .title { font-size: 20px; font-weight: 700; }
    .sub { color:var(--muted); font-size: 12px; }
    .row { display:flex; gap:12px; flex-wrap:wrap; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:12px; box-shadow: 0 6px 18px rgba(0,0,0,.25); }
    .controls { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
    label { display:block; color:var(--muted); font-size: 12px; margin-bottom:6px; }
    input, select, button { border-radius:10px; border:1px solid var(--line); background:#0f1730; color:var(--text); padding:10px 10px; font-size:14px; }
    input { width: 260px; }
    input.small { width: 140px; }
    button { cursor:pointer; }
    button.primary { background:#1b2a55; }
    button.danger { background:#3a1530; }
    .grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    @media (min-width: 860px) { .grid { grid-template-columns: repeat(5, minmax(0, 1fr)); } }
    .k { color:var(--muted); font-size: 12px; }
    .v { font-size: 18px; font-weight: 700; margin-top: 6px; }
    .v.good { color: var(--good); }
    .v.bad { color: var(--bad); }
    .err { color: var(--bad); font-size: 13px; white-space: pre-wrap; }
    .ok { color: var(--good); font-size: 13px; }
    .tableWrap { overflow:auto; }
    table { width:100%; border-collapse: collapse; min-width: 860px; }
    th, td { text-align:left; padding:10px 10px; border-bottom:1px solid var(--line); font-size: 13px; }
    th { position: sticky; top: 0; background: #0f1730; z-index: 1; }
    tr:hover td { background: rgba(255,255,255,0.03); }
    details { border:1px solid var(--line); border-radius:12px; padding:10px; background:#0f1730; }
    summary { cursor:pointer; font-weight: 700; }
    .pill { display:inline-block; padding:2px 8px; border-radius: 999px; border:1px solid var(--line); color:var(--muted); font-size:12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="title">Apex Bot PnL Dashboard</div>
        <div class="sub">Mobile-friendly view. Data loads from <span class="pill">/api/pnl</span>. If you set <span class="pill">DASHBOARD_TOKEN</span>, paste it below.</div>
      </div>

      <div class="card">
        <div class="controls">
          <div>
            <label>Token (optional; required if DASHBOARD_TOKEN is set)</label>
            <input id="token" placeholder="paste DASHBOARD_TOKEN" />
          </div>
          <div>
            <label>Bot filter (optional)</label>
            <input id="bot" class="small" placeholder="BOT_1" />
          </div>
          <div>
            <label>Auto refresh</label>
            <select id="refresh">
              <option value="0">Off</option>
              <option value="2">2s</option>
              <option value="5" selected>5s</option>
              <option value="10">10s</option>
              <option value="30">30s</option>
            </select>
          </div>
          <div>
            <button class="primary" id="load">Load</button>
            <button id="save">Save Token</button>
            <button class="danger" id="clear">Clear Token</button>
          </div>
          <div style="flex:1"></div>
          <div class="sub" id="status">Idle.</div>
        </div>
      </div>

      <div class="grid" id="cards">
        <div class="card"><div class="k">Realized (24h)</div><div class="v" id="c_day">—</div></div>
        <div class="card"><div class="k">Realized (7d)</div><div class="v" id="c_week">—</div></div>
        <div class="card"><div class="k">Realized (total)</div><div class="v" id="c_total">—</div></div>
        <div class="card"><div class="k">Unrealized (mark)</div><div class="v" id="c_unr">—</div></div>
        <div class="card"><div class="k">Bots / Trades</div><div class="v" id="c_meta">—</div></div>
      </div>

      <div class="card">
        <div class="row" style="align-items:center; justify-content:space-between">
          <div style="font-weight:700">Bots</div>
          <div class="sub" id="updated">Not loaded.</div>
        </div>
        <div class="tableWrap">
          <table>
            <thead>
              <tr>
                <th>bot_id</th>
                <th>realized_day</th>
                <th>realized_week</th>
                <th>realized_total</th>
                <th>unrealized</th>
                <th>trades</th>
                <th>open_positions</th>
              </tr>
            </thead>
            <tbody id="tbody">
              <tr><td colspan="7" class="sub">Click “Load” to fetch data.</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <details>
          <summary>Quick links</summary>
          <div class="sub" style="margin-top:10px; line-height:1.6">
            API (all): <a href="/api/pnl" target="_blank">/api/pnl</a><br />
            API (single bot): <span class="pill">/api/pnl?bot_id=BOT_1</span><br />
            If token is enabled: add <span class="pill">&amp;token=YOUR_TOKEN</span>
          </div>
        </details>
      </div>

      <div id="msg" class="err" style="display:none"></div>
    </div>
  </div>

  <script>
    const el = (id) => document.getElementById(id);
    const tokenEl = el('token');
    const botEl = el('bot');
    const refreshEl = el('refresh');
    const statusEl = el('status');
    const msgEl = el('msg');

    const fmt = (x) => {
      if (x === null || x === undefined) return '0';
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toFixed(4);
    };
    const clsPNL = (x) => {
      const n = Number(x);
      if (Number.isNaN(n)) return '';
      if (n > 0) return 'good';
      if (n < 0) return 'bad';
      return '';
    };

    function setStatus(s, ok=false) {
      statusEl.textContent = s;
      statusEl.className = ok ? 'ok' : 'sub';
    }

    function setErr(s) {
      if (!s) { msgEl.style.display='none'; msgEl.textContent=''; return; }
      msgEl.style.display='block';
      msgEl.textContent = s;
    }

    function getSavedToken() {
      try { return localStorage.getItem('apex_pnl_token') || ''; } catch(e) { return ''; }
    }
    function saveToken(t) {
      try { localStorage.setItem('apex_pnl_token', t || ''); } catch(e) {}
    }
    function clearToken() {
      try { localStorage.removeItem('apex_pnl_token'); } catch(e) {}
    }

    // preload token from URL or localStorage
    const qs = new URLSearchParams(location.search);
    const qsToken = qs.get('token') || '';
    const qsBot = qs.get('bot_id') || qs.get('bot') || '';
    tokenEl.value = qsToken || getSavedToken();
    botEl.value = qsBot;
    if (qsToken) saveToken(qsToken);

    let timer = null;

    async function fetchPnL() {
      setErr('');
      const token = (tokenEl.value || '').trim();
      const bot = (botEl.value || '').trim();
      const url = new URL('/api/pnl', location.origin);
      if (bot) url.searchParams.set('bot_id', bot);
      if (token) url.searchParams.set('token', token);

      setStatus('Loading...');
      const t0 = Date.now();
      let res;
      try {
        res = await fetch(url.toString(), { method: 'GET' });
      } catch (e) {
        setStatus('Network error');
        setErr(String(e));
        return;
      }
      const dt = Date.now() - t0;

      let data;
      try { data = await res.json(); } catch (e) { data = null; }

      if (!res.ok) {
        setStatus('Failed');
        const hint = (res.status === 403)
          ? '403 Forbidden. If you set DASHBOARD_TOKEN, paste it above (or open /dashboard?token=...).'
          : `HTTP ${res.status}`;
        setErr(hint + (data ? ('\n' + JSON.stringify(data, null, 2)) : ''));
        return;
      }

      const bots = (data && data.bots) ? data.bots : [];

      // totals
      let day=0, week=0, total=0, unr=0, trades=0;
      for (const b of bots) {
        day += Number(b.realized_day || 0);
        week += Number(b.realized_week || 0);
        total += Number(b.realized_total || 0);
        unr += Number(b.unrealized || 0);
        trades += Number(b.trades_count || 0);
      }

      el('c_day').textContent = fmt(day); el('c_day').className = 'v ' + clsPNL(day);
      el('c_week').textContent = fmt(week); el('c_week').className = 'v ' + clsPNL(week);
      el('c_total').textContent = fmt(total); el('c_total').className = 'v ' + clsPNL(total);
      el('c_unr').textContent = fmt(unr); el('c_unr').className = 'v ' + clsPNL(unr);
      el('c_meta').textContent = `${bots.length} / ${trades}`;

      const ts = data && data.ts ? Number(data.ts) : null;
      el('updated').textContent = ts ? ('Updated: ' + new Date(ts*1000).toLocaleString()) : 'Updated.';
      setStatus(`Loaded in ${dt}ms`, true);

      const tbody = el('tbody');
      tbody.innerHTML = '';
      if (!bots.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td colspan="7" class="sub">No bots with activity yet (no lots/exits recorded).</td>`;
        tbody.appendChild(tr);
        return;
      }

      for (const b of bots) {
        const opens = Array.isArray(b.open_positions) ? b.open_positions : [];
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><a href="/dashboard?bot_id=${encodeURIComponent(b.bot_id || '')}${token ? ('&token=' + encodeURIComponent(token)) : ''}">${b.bot_id || ''}</a></td>
          <td class="${clsPNL(b.realized_day)}">${fmt(b.realized_day)}</td>
          <td class="${clsPNL(b.realized_week)}">${fmt(b.realized_week)}</td>
          <td class="${clsPNL(b.realized_total)}">${fmt(b.realized_total)}</td>
          <td class="${clsPNL(b.unrealized)}">${fmt(b.unrealized)}</td>
          <td>${b.trades_count ?? 0}</td>
          <td>${opens.length}</td>
        `;
        tbody.appendChild(tr);

        if (opens.length) {
          const tr2 = document.createElement('tr');
          const rows = opens.map(p => {
            const dir = String(p.direction || '');
            return `<tr>
              <td>${p.symbol || ''}</td>
              <td>${dir}</td>
              <td>${p.qty || ''}</td>
              <td>${p.weighted_entry || ''}</td>
              <td>${p.mark_price || ''}</td>
            </tr>`;
          }).join('');
          tr2.innerHTML = `
            <td colspan="7">
              <details>
                <summary>Open positions for ${b.bot_id} (${opens.length})</summary>
                <div class="tableWrap" style="margin-top:10px;">
                  <table style="min-width:620px;">
                    <thead><tr><th>symbol</th><th>direction</th><th>qty</th><th>weighted_entry</th><th>mark_price</th></tr></thead>
                    <tbody>${rows}</tbody>
                  </table>
                </div>
              </details>
            </td>
          `;
          tbody.appendChild(tr2);
        }
      }
    }

    function setAutoRefresh() {
      if (timer) { clearInterval(timer); timer = null; }
      const sec = Number(refreshEl.value || 0);
      if (sec > 0) timer = setInterval(fetchPnL, sec * 1000);
    }

    el('load').addEventListener('click', () => { fetchPnL(); });
    el('save').addEventListener('click', () => { saveToken((tokenEl.value||'').trim()); setStatus('Token saved.', true); });
    el('clear').addEventListener('click', () => { tokenEl.value=''; clearToken(); setStatus('Token cleared.', true); });
    refreshEl.addEventListener('change', setAutoRefresh);

    setAutoRefresh();
    // auto-load once on open
    fetchPnL();
  </script>
</body>
</html>"""

    return Response(html, mimetype="text/html")


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()

    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        print("[WEBHOOK] invalid secret")
        return "forbidden", 403

    symbol = body.get("symbol")
    if not symbol:
        return "missing symbol", 400
    symbol = str(symbol).upper().strip()

    bot_id = _canon_bot_id(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper().strip()
    signal_type_raw = str(body.get("signal_type", "")).lower().strip()
    action_raw = str(body.get("action", "")).lower().strip()
    tv_client_id = body.get("client_id")

    mode: Optional[str] = None
    if signal_type_raw in ("entry", "open"):
        mode = "entry"
    elif signal_type_raw.startswith("exit"):
        mode = "exit"
    else:
        if action_raw in ("open", "entry"):
            mode = "entry"
        elif action_raw in ("close", "exit"):
            mode = "exit"

    if mode is None:
        return "missing or invalid signal_type / action", 400

    sig_id = _get_signal_id(body, mode, bot_id, symbol)
    if is_signal_processed(bot_id, sig_id):
        print(f"[WEBHOOK] dedup: bot={bot_id} symbol={symbol} mode={mode} sig={sig_id}")
        return jsonify({"status": "dedup", "mode": mode, "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}), 200
    mark_signal_processed(bot_id, sig_id, kind=f"webhook_{mode}")

    # -------------------------
    # ENTRY
    # -------------------------
    if mode == "entry":
        expected = _bot_expected_entry_side(bot_id)
        if expected is not None:
            if side_raw and side_raw != expected:
                return jsonify({
                    "status": "rejected_wrong_direction",
                    "bot_id": bot_id,
                    "symbol": symbol,
                    "expected_side": expected,
                    "got_side": side_raw,
                    "signal_id": sig_id,
                }), 200
            side_raw = expected

        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        # ✅ Constraint A: global same-symbol same-direction (across all bots)
        desired_dir = "LONG" if side_raw == "BUY" else "SHORT"
        open_dirs = get_symbol_open_directions(symbol)
        if len(open_dirs) > 1:
            # This should not happen; fail-closed to avoid netting issues.
            return jsonify({
                "status": "reject_direction_conflict",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "desired_direction": desired_dir,
                "open_directions": sorted(list(open_dirs)),
                "signal_id": sig_id,
            }), 200
        if len(open_dirs) == 1 and desired_dir not in open_dirs:
            return jsonify({
                "status": "reject_opposite_direction_locked",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "desired_direction": desired_dir,
                "locked_direction": list(open_dirs)[0],
                "signal_id": sig_id,
            }), 200

        try:
            budget = _extract_budget_usdt(body)
        except Exception as e:
            print("[ENTRY] budget error:", e)
            return str(e), 400

        try:
            snapped_qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            print("[ENTRY] qty compute error:", e)
            return "qty compute error", 500

        size_str = str(snapped_qty)
        print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} budget={budget} -> qty={size_str} sig={sig_id}")

        # Deterministic numeric clientId for attribution: BBB + ts + 01 (ENTRY)
        bnum = _bot_num(bot_id)
        ts_now = int(time.time())
        entry_client_id = f"{bnum:03d}{ts_now}01"


        # Snapshot current position size BEFORE placing the order.
        # This makes position-fallback safer when multiple bots can trade the same symbol.
        pre_pos_size = Decimal("0")
        try:
            pre = get_open_position_for_symbol(symbol)
            if isinstance(pre, dict) and pre.get("size") is not None:
                if side_raw == "BUY" and str(pre.get("side", "")).upper() == "LONG":
                    pre_pos_size = Decimal(str(pre["size"]))
                elif side_raw == "SELL" and str(pre.get("side", "")).upper() == "SHORT":
                    pre_pos_size = Decimal(str(pre["size"]))
        except Exception:
            pre_pos_size = Decimal("0")


        try:
            order = create_market_order(
                symbol=symbol,
                side=side_raw,
                size=size_str,
                reduce_only=False,
                client_id=entry_client_id,
            )
        except Exception as e:
            print("[ENTRY] create_market_order error:", e)
            return "order error", 500

        status, cancel_reason = _order_status_and_reason(order)
        print(f"[ENTRY] order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return jsonify({
                "status": "order_rejected",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "request_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "signal_id": sig_id,
            }), 200

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        entry_price_dec: Optional[Decimal] = None
        final_qty = snapped_qty

        # ✅ Primary: order-based fill summary (from WS order channel)
        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                max_wait_sec=float(os.getenv("FILL_MAX_WAIT_SEC", "20.0")),
                poll_interval=float(os.getenv("FILL_POLL_INTERVAL", "0.25")),
            )
            entry_price_dec = Decimal(str(fill["avg_fill_price"]))
            final_qty = Decimal(str(fill["filled_qty"]))
            print(f"[ENTRY] fill ok bot={bot_id} symbol={symbol} filled_qty={final_qty} avg_fill={entry_price_dec}")
        except Exception as e:
            print(f"[ENTRY] fill fallback bot={bot_id} symbol={symbol} err:", e)

            # Fallback: use position entryPrice snapshot if order detail is delayed.
            try:
                pos_wait = float(os.getenv("POSITION_FALLBACK_WAIT_SEC", "8.0"))
                pos_poll = float(os.getenv("POSITION_FALLBACK_POLL_INTERVAL", "0.5"))
                deadline = time.time() + pos_wait

                while time.time() < deadline and entry_price_dec is None:
                    pos = get_open_position_for_symbol(symbol)
                    if isinstance(pos, dict):
                        sz = pos.get("size")
                        ep = pos.get("entryPrice") or pos.get("entry_price") or pos.get("avgEntryPrice")
                        try:
                            dsz = Decimal(str(sz)) if sz is not None else Decimal("0")
                        except Exception:
                            dsz = Decimal("0")

                        if dsz > 0 and ep not in (None, "", "0", "0.0"):
                            entry_price_dec = Decimal(str(ep))
                            # Use delta vs pre-order position size, capped at requested qty.
                            try:
                                delta_sz = (dsz - pre_pos_size)
                            except Exception:
                                delta_sz = dsz
                            if delta_sz <= 0:
                                final_qty = snapped_qty
                            else:
                                final_qty = min(snapped_qty, delta_sz)

                            print(f"[ENTRY] position fallback bot={bot_id} symbol={symbol} posSize={dsz} prePos={pre_pos_size} delta={delta_sz} final_qty={final_qty} entryPrice={entry_price_dec}")
                            break

                    time.sleep(pos_poll)

            except Exception as e2:
                print(f"[ENTRY] position fallback failed bot={bot_id} symbol={symbol} err:", e2)

        key = (bot_id, symbol)
        BOT_POSITIONS[key] = {"side": side_raw, "qty": final_qty, "entry_price": entry_price_dec}

        if entry_price_dec is not None and final_qty > 0:
            try:
                record_entry(
                    bot_id=bot_id,
                    symbol=symbol,
                    side=side_raw,
                    qty=final_qty,
                    price=entry_price_dec,
                    reason="strategy_entry_fill",
                )
            except Exception as e:
                print("[PNL] record_entry error:", e)

        # ✅ BOT_1-20：挂交易所 TP/SL（reduceOnly=True）
        if _bot_has_exchange_brackets(bot_id) and entry_price_dec is not None and final_qty > 0:
            direction = "LONG" if side_raw == "BUY" else "SHORT"
            try:
                _place_fixed_brackets(bot_id, symbol, direction, final_qty, entry_price_dec)
            except Exception as e:
                print("[BRACKET] place error:", e)

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(final_qty),
            "entry_price": str(entry_price_dec) if entry_price_dec else None,
            "requested_qty": size_str,
            "order_status": status,
            "cancel_reason": cancel_reason,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "signal_id": sig_id,
        }), 200


    # -------------------------
    # EXIT（策略出场 / TV 出场）
    # -------------------------
    if mode == "exit":
        if not _exit_guard_allow(bot_id, symbol):
            print(f"[EXIT] SKIP (cooldown) bot={bot_id} symbol={symbol} sig={sig_id}")
            return jsonify({"status": "cooldown_skip", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}), 200

        opens = get_bot_open_positions(bot_id)
        long_key = (symbol, "LONG")
        short_key = (symbol, "SHORT")

        long_qty = opens.get(long_key, {}).get("qty", Decimal("0"))
        short_qty = opens.get(short_key, {}).get("qty", Decimal("0"))

        key_local = (bot_id, symbol)
        local = BOT_POSITIONS.get(key_local)
        preferred = "LONG"
        if local and str(local.get("side", "")).upper() == "SELL":
            preferred = "SHORT"

        direction_to_close = None
        qty_to_close = Decimal("0")

        if preferred == "LONG" and long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif preferred == "SHORT" and short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty
        elif long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty

        if (not direction_to_close or qty_to_close <= 0):
            if symbol in REMOTE_FALLBACK_SYMBOLS:
                remote = get_open_position_for_symbol(symbol)
                if remote and remote["size"] > 0:
                    direction_to_close = remote["side"]
                    qty_to_close = remote["size"]
                else:
                    return jsonify({"status": "no_position"}), 200
            else:
                return jsonify({"status": "no_position"}), 200

        # ✅ 手动/TV exit：先取消交易所挂的 TP/SL
        try:
            _cancel_existing_brackets(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        entry_side = "BUY" if direction_to_close == "LONG" else "SELL"
        exit_side = "SELL" if direction_to_close == "LONG" else "BUY"

        # Deterministic numeric clientId for attribution: BBB + ts + 02 (EXIT)
        bnum = _bot_num(bot_id)
        ts_now = int(time.time())
        exit_client_id = f"{bnum:03d}{ts_now}02"

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty_to_close),
                reduce_only=True,
                client_id=exit_client_id,
            )
        except Exception as e:
            print("[EXIT] create_market_order error:", e)
            return "order error", 500

        status, cancel_reason = _order_status_and_reason(order)
        print(f"[EXIT] order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return jsonify({
                "status": "exit_rejected",
                "mode": "exit",
                "bot_id": bot_id,
                "symbol": symbol,
                "exit_side": exit_side,
                "requested_qty": str(qty_to_close),
                "order_status": status,
                "cancel_reason": cancel_reason,
                "signal_id": sig_id,
            }), 200

        exit_price = None
        filled_qty = qty_to_close

        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order.get("order_id"),
                client_order_id=order.get("client_order_id"),
                max_wait_sec=float(os.getenv("FILL_MAX_WAIT_SEC", "20.0")),
                poll_interval=float(os.getenv("FILL_POLL_INTERVAL", "0.25")),
            )
            exit_price = Decimal(str(fill["avg_fill_price"]))
            filled_qty = Decimal(str(fill["filled_qty"]))
            print(f"[EXIT] fill ok bot={bot_id} symbol={symbol} filled_qty={filled_qty} avg_fill={exit_price}")
        except Exception as e:
            print(f"[EXIT] fill wait failed bot={bot_id} symbol={symbol} err:", e)
            # Last resort: mark price for bookkeeping ONLY if absolutely necessary
            try:
                rules = _get_symbol_rules(symbol)
                min_qty = rules["min_qty"]
                px_str = get_market_price(symbol, exit_side, str(min_qty))
                exit_price = Decimal(str(px_str))
            except Exception:
                exit_price = Decimal("0.00000001")

        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=filled_qty,
                exit_price=exit_price,
                reason="strategy_exit",
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (strategy):", e)

        try:
            clear_lock_level_pct(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        # Refresh local cache based on DB remaining
        try:
            opens2 = get_bot_open_positions(bot_id)
            rem = opens2.get((symbol, direction_to_close), {}).get("qty", Decimal("0"))
        except Exception:
            rem = Decimal("0")

        if key_local in BOT_POSITIONS:
            BOT_POSITIONS[key_local]["qty"] = rem
            if rem <= 0:
                BOT_POSITIONS[key_local]["entry_price"] = None

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(qty_to_close),
            "order_status": status,
            "cancel_reason": cancel_reason,
            "signal_id": sig_id,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
