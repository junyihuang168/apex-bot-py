import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set, Any

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_market_price,
    get_fill_summary,
    get_open_position_for_symbol,
    _get_symbol_rules,
    _snap_quantity,
    start_private_ws,
    pop_fill_event,
    create_trigger_order,
    cancel_order,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
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

# 退出互斥窗口（秒）：防止重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}

# ✅ 固定止损/止盈（百分比）
FIXED_SL_PCT = Decimal(os.getenv("FIXED_SL_PCT", "0.5"))  # -0.5%
FIXED_TP_PCT = Decimal(os.getenv("FIXED_TP_PCT", "1.0"))  # +1.0%

# ✅ 保护单模式：MARKET（默认稳） or LIMIT（你若坚持“限价止损/止盈”）
PROTECTIVE_ORDER_MODE = str(os.getenv("PROTECTIVE_ORDER_MODE", "MARKET")).upper().strip()
# LIMIT 模式下：给限价一个“更差一点”的价格，提升成交概率
PROTECTIVE_SLIPPAGE_PCT = Decimal(os.getenv("PROTECTIVE_SLIPPAGE_PCT", "0.15"))  # 0.15%

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
# ✅ 新 BOT 分组（按你要求）
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


def _apply_limit_slippage(direction: str, kind: str, trigger: Decimal) -> Decimal:
    """
    LIMIT 模式：给“更差一点”的限价，提升成交概率。
    kind: "SL" or "TP"
    """
    slip = (PROTECTIVE_SLIPPAGE_PCT / Decimal("100"))
    d = direction.upper()
    k = kind.upper()

    if d == "LONG":
        # SELL orders: use <= trigger
        return trigger * (Decimal("1") - slip)
    else:
        # SHORT closing BUY orders: use >= trigger
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

    # expiration：当前时间 + N 秒
    expiration = int(time.time()) + int(PROTECTIVE_EXPIRE_SEC)

    if direction == "LONG":
        exit_side = "SELL"
    else:
        exit_side = "BUY"

    # 订单类型
    if PROTECTIVE_ORDER_MODE == "LIMIT":
        sl_type = "STOP_LIMIT"
        tp_type = "TAKE_PROFIT_LIMIT"
        sl_price = _apply_limit_slippage(direction, "SL", sl_trigger)
        tp_price = _apply_limit_slippage(direction, "TP", tp_trigger)
    else:
        sl_type = "STOP_MARKET"
        tp_type = "TAKE_PROFIT_MARKET"
        # MARKET 模式下 price 给一个占位（worstPrice/trigger），由 apex_client 内部兜底
        sl_price = None
        tp_price = None

    # 给 clientId 做一个“数字-only”的可追踪串（不依赖 tv client_id）
    bnum = _bot_num(bot_id)
    ts = int(time.time())
    sl_client = f"{bnum:03d}{ts}01"
    tp_client = f"{bnum:03d}{ts}02"

    # 下单（reduceOnly=True）
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
# ✅ WS fills -> 自动记账 + OCO
# ----------------------------
def _fills_loop():
    print("[WS-FILLS] loop started")
    while True:
        evt = pop_fill_event(timeout=0.5)
        if not evt:
            continue

        try:
            fill_id = str(evt.get("id") or "")
            order_id = str(evt.get("orderId") or "")
            symbol = str(evt.get("symbol") or "").upper().strip()
            side = str(evt.get("side") or "").upper().strip()  # BUY/SELL
            px = evt.get("price")
            sz = evt.get("size")

            if not fill_id or not order_id or not symbol or px is None or sz is None:
                continue

            # 找到这个 fill 是否属于我们的 TP/SL 保护单
            owner = find_protective_owner_by_order_id(order_id)
            if not owner:
                continue

            bot_id = _canon_bot_id(owner["bot_id"])
            direction = str(owner["direction"]).upper()
            kind = str(owner.get("kind") or "").upper()  # SL / TP

            # 幂等：同一 fill_id 只记一次账
            sig_id = f"fill:{fill_id}"
            if is_signal_processed(bot_id, sig_id):
                continue
            mark_signal_processed(bot_id, sig_id, kind=f"ws_{kind.lower()}_fill")

            dq = Decimal(str(sz))
            dp = Decimal(str(px))
            if dq <= 0 or dp <= 0:
                continue

            entry_side = "BUY" if direction == "LONG" else "SELL"

            # 记账：把这次成交当作“平仓成交”
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=dq,
                exit_price=dp,
                reason=f"exchange_{kind.lower()}_fill",
            )

            # 如果仓位已经清空：OCO（取消另一边）+ 清理 protective_orders
            opens = get_bot_open_positions(bot_id)
            remaining = opens.get((symbol, direction), {}).get("qty", Decimal("0"))

            po = get_protective_orders(bot_id, symbol, direction)
            sl_oid = po.get("sl_order_id") if po else None
            tp_oid = po.get("tp_order_id") if po else None

            if remaining <= 0:
                # cancel the other leg
                other_oid = None
                if kind == "SL":
                    other_oid = tp_oid
                elif kind == "TP":
                    other_oid = sl_oid
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

                # 本地 cache 清理
                key_local = (bot_id, symbol)
                if key_local in BOT_POSITIONS:
                    BOT_POSITIONS[key_local]["qty"] = Decimal("0")
                    BOT_POSITIONS[key_local]["entry_price"] = None

                print(f"[WS-FILLS] position closed by {kind}. bot={bot_id} {direction} {symbol} now flat. OCO done.")
            else:
                # 部分成交：保留 protective_orders（你也可以选择在这里“重挂剩余 qty 的 TP/SL”，先不做复杂化）
                print(
                    f"[WS-FILLS] partial {kind} fill bot={bot_id} {direction} {symbol} "
                    f"fill_qty={dq} fill_px={dp} remaining={remaining}"
                )

        except Exception as e:
            print("[WS-FILLS] error:", e)


def _ensure_ws_fills_thread():
    global _FILLS_THREAD_STARTED
    with _FILLS_LOCK:
        if _FILLS_THREAD_STARTED:
            return
        t = threading.Thread(target=_fills_loop, daemon=True)
        t.start()
        _FILLS_THREAD_STARTED = True
        print("[WS-FILLS] thread created")


def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_THREAD_STARTED:
            return

        init_db()

        # ✅ WS：启动一次就好
        start_private_ws()
        _ensure_ws_fills_thread()

        _MONITOR_THREAD_STARTED = True
        print("[SYSTEM] monitor/ws threads ready")


@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200


# ----------------------------
# PnL API（给手机/电脑看）
# ----------------------------
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

            # mark price 用 worstPrice(min_qty) 近似（够用）
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


# ----------------------------
# 简易 dashboard（可选）
# ----------------------------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not _require_token():
        return Response("Forbidden", status=403)

    _ensure_monitor_thread()

    html = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bot PnL Dashboard</title>
  <style>
    :root { --bg:#0b0f14; --card:#111826; --muted:#9aa4b2; --text:#eef2f7; --good:#22c55e; --bad:#ef4444; }
    body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Noto Sans CJK SC"; background:var(--bg); color:var(--text); }
    header { padding:16px 20px; border-bottom:1px solid #1d2636; display:flex; gap:12px; align-items:center; justify-content:space-between; }
    h1 { font-size:18px; margin:0; }
    .muted { color:var(--muted); font-size:12px; }
    .wrap { padding:16px; max-width:1100px; margin:0 auto; }
    .controls { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px; }
    input, select, button {
      background:#0f1624; border:1px solid #22304a; color:var(--text);
      padding:8px 10px; border-radius:10px; font-size:12px;
    }
    button { cursor:pointer; }
    .grid { display:grid; grid-template-columns: repeat(12, 1fr); gap:12px; }
    .card { background:var(--card); border:1px solid #1c2a44; border-radius:16px; padding:14px; grid-column: span 6; }
    .card.full { grid-column: span 12; }
    @media (max-width: 900px) { .card { grid-column: span 12; } }
    .row { display:flex; align-items:center; justify-content:space-between; gap:10px; }
    .bot { font-size:14px; font-weight:600; }
    .pill { font-size:10px; padding:2px 8px; background:#0b1220; border:1px solid #23324f; border-radius:999px; color:var(--muted); }
    .num { font-variant-numeric: tabular-nums; }
    .good { color:var(--good); }
    .bad { color:var(--bad); }
    table { width:100%; border-collapse: collapse; margin-top:10px; }
    th, td { text-align:left; font-size:11px; padding:8px 6px; border-bottom:1px solid #1b2538; color:var(--text); }
    th { color:var(--muted); font-weight:500; }
    .small { font-size:10px; color:var(--muted); }
    .group { display:flex; gap:8px; flex-wrap:wrap; }
  </style>
</head>
<body>
<header>
  <div>
    <h1>Bot PnL Dashboard</h1>
    <div class="muted">WS fills 自动记账 · BOT 分组方向固定 · TP/SL 挂交易所（BOT_1-20）· reduceOnly 平仓风控</div>
  </div>
  <div class="muted" id="lastUpdate">Loading...</div>
</header>

<div class="wrap">
  <div class="controls">
    <button onclick="refresh()">刷新</button>
    <select id="sortBy" onchange="render()">
      <option value="realized_total">按总已实现</option>
      <option value="realized_day">按24h已实现</option>
      <option value="realized_week">按7d已实现</option>
      <option value="unrealized">按未实现</option>
      <option value="trades_count">按交易次数</option>
    </select>
    <input id="filter" placeholder="过滤 BOT_1..." oninput="render()" />
  </div>

  <div class="grid">
    <div class="card full">
      <div class="row">
        <div class="group">
          <span class="pill">BOT_1-10: LONG + Exchange TP/SL (-0.5% / +1%)</span>
          <span class="pill">BOT_11-20: SHORT + Exchange TP/SL (-0.5% / +1%)</span>
          <span class="pill">BOT_21-30: LONG PnL-only (no TP/SL)</span>
          <span class="pill">BOT_31-40: SHORT PnL-only (no TP/SL)</span>
        </div>
        <div class="small">数据来自 /api/pnl</div>
      </div>
    </div>
  </div>

  <div class="grid" id="cards"></div>
</div>

<script>
let DATA = [];

function numClass(v) {
  const n = Number(v);
  if (isNaN(n) || n === 0) return "num";
  return n > 0 ? "num good" : "num bad";
}

function pickSort(a, b, key) {
  const av = Number(a[key] ?? 0);
  const bv = Number(b[key] ?? 0);
  return bv - av;
}

function render() {
  const key = document.getElementById("sortBy").value;
  const f = (document.getElementById("filter").value || "").trim().toUpperCase();

  let arr = [...DATA];
  if (f) arr = arr.filter(x => String(x.bot_id || "").toUpperCase().includes(f));
  arr.sort((a, b) => pickSort(a, b, key));

  const root = document.getElementById("cards");
  root.innerHTML = "";

  for (const b of arr) {
    const bot = b.bot_id;
    const realized_total = b.realized_total ?? "0";
    const realized_day = b.realized_day ?? "0";
    const realized_week = b.realized_week ?? "0";
    const unrealized = b.unrealized ?? "0";
    const trades = b.trades_count ?? 0;
    const open = b.open_positions || [];

    const div = document.createElement("div");
    div.className = "card";

    div.innerHTML = `
      <div class="row">
        <div class="row" style="gap:8px;">
          <span class="bot">${bot}</span>
          <span class="pill">active</span>
        </div>
        <span class="small">Trades: <span class="num">${trades}</span></span>
      </div>

      <table>
        <thead>
          <tr>
            <th>24h 已实现</th>
            <th>7d 已实现</th>
            <th>总已实现</th>
            <th>未实现</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td class="${numClass(realized_day)}">${realized_day}</td>
            <td class="${numClass(realized_week)}">${realized_week}</td>
            <td class="${numClass(realized_total)}">${realized_total}</td>
            <td class="${numClass(unrealized)}">${unrealized}</td>
          </tr>
        </tbody>
      </table>

      <div class="small" style="margin-top:8px;">Open lots</div>
      <table>
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Dir</th>
            <th>Qty</th>
            <th>W.Entry</th>
            <th>Mark</th>
          </tr>
        </thead>
        <tbody>
          ${
            open.length === 0
              ? `<tr><td colspan="5" class="small">No open lots</td></tr>`
              : open.map(p => `
                <tr>
                  <td>${p.symbol}</td>
                  <td>${p.direction}</td>
                  <td class="num">${p.qty}</td>
                  <td class="num">${p.weighted_entry}</td>
                  <td class="num">${p.mark_price}</td>
                </tr>
              `).join("")
          }
        </tbody>
      </table>
    `;

    root.appendChild(div);
  }
}

async function refresh() {
  try {
    const url = new URL("/api/pnl", window.location.origin);
    const t = new URLSearchParams(window.location.search).get("token");
    if (t) url.searchParams.set("token", t);

    const res = await fetch(url.toString());
    const js = await res.json();
    DATA = js.bots || [];
    document.getElementById("lastUpdate").textContent =
      "Updated: " + new Date((js.ts || Date.now()/1000) * 1000).toLocaleString();
    render();
  } catch (e) {
    document.getElementById("lastUpdate").textContent = "Load error";
    console.error(e);
  }
}

refresh();
setInterval(refresh, 15000);
</script>
</body>
</html>
    """
    return Response(html, mimetype="text/html")


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()

    # 1) parse json
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    # 2) secret check
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

    # 3) mode
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

    # 4) idempotency (webhook)
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
            side_raw = expected  # 强制固定方向

        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

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
        print(
            f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} "
            f"budget={budget} -> qty={size_str} sig={sig_id}"
        )

        try:
            order = create_market_order(
                symbol=symbol,
                side=side_raw,
                size=size_str,
                reduce_only=False,
                client_id=tv_client_id,
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

        # ✅ 真实成交价优先：WS/REST fill summary
        computed = (order or {}).get("computed") or {}
        fallback_price_str = computed.get("price")

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        entry_price_dec: Optional[Decimal] = None
        final_qty = snapped_qty

        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                max_wait_sec=float(os.getenv("FILL_MAX_WAIT_SEC", "2.0")),
                poll_interval=float(os.getenv("FILL_POLL_INTERVAL", "0.25")),
            )
            entry_price_dec = Decimal(str(fill["avg_fill_price"]))
            final_qty = Decimal(str(fill["filled_qty"]))
            print(
                f"[ENTRY] fill ok bot={bot_id} symbol={symbol} "
                f"filled_qty={final_qty} avg_fill={entry_price_dec}"
            )
        except Exception as e:
            print(f"[ENTRY] fill fallback bot={bot_id} symbol={symbol} err:", e)
            try:
                if fallback_price_str is not None:
                    entry_price_dec = Decimal(str(fallback_price_str))
            except Exception:
                entry_price_dec = None

        # 本地 cache 辅助
        key = (bot_id, symbol)
        BOT_POSITIONS[key] = {
            "side": side_raw,
            "qty": final_qty,
            "entry_price": entry_price_dec,
        }

        # ✅ lots 记账
        if entry_price_dec is not None and final_qty > 0:
            try:
                record_entry(
                    bot_id=bot_id,
                    symbol=symbol,
                    side=side_raw,
                    qty=final_qty,
                    price=entry_price_dec,
                    reason="strategy_entry_fill" if order_id else "strategy_entry",
                )
            except Exception as e:
                print("[PNL] record_entry error:", e)

        # ✅ BOT_1-20：挂交易所 TP/SL（reduceOnly=True）
        if _bot_has_exchange_brackets(bot_id) and entry_price_dec is not None and final_qty > 0:
            direction = "LONG" if side_raw == "BUY" else "SHORT"
            try:
                _place_fixed_brackets(
                    bot_id=bot_id,
                    symbol=symbol,
                    direction=direction,
                    qty=final_qty,
                    entry_price=entry_price_dec,
                )
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

        # 优先按本地 side 推断方向
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

        # ✅ 远程兜底严格条件：本地无 lots + symbol 白名单允许
        if (not direction_to_close or qty_to_close <= 0):
            if symbol in REMOTE_FALLBACK_SYMBOLS:
                print(f"[EXIT] local lots empty; REMOTE fallback allowed for symbol={symbol}. bot={bot_id} sig={sig_id}")
                remote = get_open_position_for_symbol(symbol)
                if remote and remote["size"] > 0:
                    direction_to_close = remote["side"]  # LONG/SHORT
                    qty_to_close = remote["size"]
                    print(f"[EXIT] remote fallback found: {direction_to_close} {qty_to_close} symbol={symbol}")
                else:
                    return jsonify({"status": "no_position"}), 200
            else:
                return jsonify({"status": "no_position"}), 200

        # ✅ 先取消交易所挂的 TP/SL（避免手动 exit 后还有残留单）
        try:
            _cancel_existing_brackets(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        entry_side = "BUY" if direction_to_close == "LONG" else "SELL"
        exit_side = "SELL" if direction_to_close == "LONG" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} direction={direction_to_close} "
            f"qty={qty_to_close} -> exit_side={exit_side} sig={sig_id}"
        )

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty_to_close),
                reduce_only=True,   # ✅ reduceOnly 风控
                client_id=tv_client_id,
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

        # exit_price 用 worstPrice(min_qty) 近似（记账够用）
        try:
            rules = _get_symbol_rules(symbol)
            min_qty = rules["min_qty"]
            px_str = get_market_price(symbol, exit_side, str(min_qty))
            exit_price = Decimal(str(px_str))
        except Exception:
            exit_price = Decimal("0")

        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=qty_to_close,
                exit_price=exit_price if exit_price > 0 else Decimal("0.00000001"),
                reason="strategy_exit",
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (strategy):", e)

        try:
            clear_lock_level_pct(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        if key_local in BOT_POSITIONS:
            BOT_POSITIONS[key_local]["qty"] = Decimal("0")
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
