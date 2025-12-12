import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set, Any

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_mark_price,                # ✅ 用 mid/mark 近似价算 pnl/触发锁盈
    get_fill_summary,              # ✅ 真实成交价/数量（fills/avgFill）
    get_open_position_for_symbol,  # ✅ 远程兜底查仓
    map_position_side_to_exit_order_side,
    _get_symbol_rules,
    _snap_quantity,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
    get_lock_level_pct,
    set_lock_level_pct,
    clear_lock_level_pct,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ----------------------------
# 你最新的风控分组（默认）
# BOT_1~5：做多（LONG）带止损/锁盈
# BOT_6~10：做空（SHORT）带止损/锁盈
# 其它 bot：策略进出场，但也会记录真实成交价
# ----------------------------
def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip() for b in env_val.split(",") if b.strip()}

LONG_RISK_BOTS = _parse_bot_list(
    os.getenv("LONG_RISK_BOTS", ",".join([f"BOT_{i}" for i in range(1, 6)]))
)
SHORT_RISK_BOTS = _parse_bot_list(
    os.getenv("SHORT_RISK_BOTS", ",".join([f"BOT_{i}" for i in range(6, 11)]))
)

BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))  # -0.5%
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.0"))

# fills 追踪参数（满足你“前 2–3 秒高频尝试”）
FILL_FAST_WINDOW_SEC = float(os.getenv("FILL_FAST_WINDOW_SEC", "3.0"))
FILL_FAST_POLL_INTERVAL = float(os.getenv("FILL_FAST_POLL_INTERVAL", "0.2"))
FILL_MAX_WAIT_SEC = float(os.getenv("FILL_MAX_WAIT_SEC", "30.0"))
FILL_POLL_INTERVAL = float(os.getenv("FILL_POLL_INTERVAL", "0.6"))

# 本地 cache（辅助；真相源是 pnl_store）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

# pending fills：当 webhook 返回时还拿不到 fills，会后台补全
PENDING_FILL_TASKS: Dict[str, Dict[str, Any]] = {}
_PENDING_LOCK = threading.Lock()

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()


def _require_token() -> bool:
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


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
# 预算 -> qty（用 mark/mid 近似价，避免 worstPrice 低估导致 qty 计算报错/偏差）
# ----------------------------
def _compute_entry_qty(symbol: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    # ✅ 参考价用 mark（worstBuy/worstSell 的中值），更稳定
    ref_price_dec = Decimal(get_mark_price(symbol))
    if ref_price_dec <= 0:
        raise ValueError("ticker price unavailable")

    theoretical_qty = budget / ref_price_dec
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    if snapped_qty <= 0:
        raise ValueError(f"snapped_qty <= 0, symbol={symbol}, budget={budget}")

    # 至少要 >= min_qty
    if snapped_qty < min_qty:
        return Decimal("0")

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
# 你最新的阶梯锁盈规则
# pnl >= 0.15 -> lock 0.10
# pnl >= 0.45 -> lock 0.20
# 之后每 +0.10 pnl，lock +0.10
# ----------------------------
TRIGGER_1 = Decimal(os.getenv("LOCK_TRIGGER_1", "0.15"))
LOCK_1 = Decimal(os.getenv("LOCK_LEVEL_1", "0.10"))

TRIGGER_2 = Decimal(os.getenv("LOCK_TRIGGER_2", "0.45"))
LOCK_2 = Decimal(os.getenv("LOCK_LEVEL_2", "0.20"))

LOCK_STEP_PNL = Decimal(os.getenv("LOCK_STEP_PNL", "0.10"))
LOCK_STEP_LEVEL = Decimal(os.getenv("LOCK_STEP_LEVEL", "0.10"))


def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    if pnl_pct < TRIGGER_1:
        return Decimal("0")
    if pnl_pct < TRIGGER_2:
        return LOCK_1
    n = (pnl_pct - TRIGGER_2) // LOCK_STEP_PNL
    return LOCK_2 + LOCK_STEP_LEVEL * n


def _bot_allows_direction(bot_id: str, direction: str) -> bool:
    d = direction.upper()
    if bot_id in LONG_RISK_BOTS and d == "LONG":
        return True
    if bot_id in SHORT_RISK_BOTS and d == "SHORT":
        return True
    return False


def _get_mark(symbol: str) -> Optional[Decimal]:
    try:
        return Decimal(get_mark_price(symbol))
    except Exception as e:
        print(f"[PRICE] mark price error symbol={symbol}:", e)
        return None


def _calc_pnl_pct(direction: str, entry_price: Decimal, current_price: Decimal) -> Decimal:
    if entry_price <= 0:
        return Decimal("0")
    if direction.upper() == "LONG":
        return (current_price - entry_price) * Decimal("100") / entry_price
    else:
        return (entry_price - current_price) * Decimal("100") / entry_price


def _base_sl_hit(direction: str, entry_price: Decimal, current_price: Decimal) -> bool:
    pct = BASE_SL_PCT / Decimal("100")
    if direction.upper() == "LONG":
        stop_price = entry_price * (Decimal("1") - pct)
        return current_price <= stop_price
    else:
        stop_price = entry_price * (Decimal("1") + pct)
        return current_price >= stop_price


def _lock_hit(direction: str, entry_price: Decimal, current_price: Decimal, lock_level_pct: Decimal) -> bool:
    if lock_level_pct <= 0:
        return False
    pct = lock_level_pct / Decimal("100")
    if direction.upper() == "LONG":
        lock_price = entry_price * (Decimal("1") + pct)
        return current_price <= lock_price
    else:
        lock_price = entry_price * (Decimal("1") - pct)
        return current_price >= lock_price


def _finalize_entry_fill_async(task_key: str):
    """
    后台追踪 fills：拿到真实成交价后再入账、更新 BOT_POSITIONS
    """
    with _PENDING_LOCK:
        task = PENDING_FILL_TASKS.get(task_key)
    if not task:
        return

    bot_id = task["bot_id"]
    symbol = task["symbol"]
    side = task["side"]
    order_id = task.get("order_id")
    client_order_id = task.get("client_order_id")
    requested_qty = Decimal(str(task.get("requested_qty") or "0"))

    try:
        fill = get_fill_summary(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
            max_wait_sec=FILL_MAX_WAIT_SEC,
            poll_interval=FILL_POLL_INTERVAL,
            fast_window_sec=FILL_FAST_WINDOW_SEC,
            fast_poll_interval=FILL_FAST_POLL_INTERVAL,
        )
        entry_price = Decimal(str(fill["avg_fill_price"]))
        filled_qty = Decimal(str(fill["filled_qty"]))

        if filled_qty <= 0 or entry_price <= 0:
            raise RuntimeError("fill returned invalid qty/price")

        # cache
        key = (bot_id, symbol)
        BOT_POSITIONS[key] = {"side": side, "qty": filled_qty, "entry_price": entry_price}

        # record
        record_entry(
            bot_id=bot_id,
            symbol=symbol,
            side=side,
            qty=filled_qty,
            price=entry_price,
            reason="strategy_entry_fill_async",
        )

        print(f"[FILL-ASYN] entry ok bot={bot_id} {symbol} qty={filled_qty} px={entry_price}")

    except Exception as e:
        print(f"[FILL-ASYN] entry fill failed bot={bot_id} {symbol} err:", e)

    finally:
        with _PENDING_LOCK:
            PENDING_FILL_TASKS.pop(task_key, None)


def _finalize_exit_fill(
    symbol: str,
    order_id: Optional[str],
    client_order_id: Optional[str],
) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    返回 (filled_qty, avg_fill_price)
    """
    try:
        fill = get_fill_summary(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
            max_wait_sec=FILL_MAX_WAIT_SEC,
            poll_interval=FILL_POLL_INTERVAL,
            fast_window_sec=FILL_FAST_WINDOW_SEC,
            fast_poll_interval=FILL_FAST_POLL_INTERVAL,
        )
        return Decimal(str(fill["filled_qty"])), Decimal(str(fill["avg_fill_price"]))
    except Exception as e:
        print(f"[EXIT] fill not available symbol={symbol} err:", e)
        return None, None


def _execute_virtual_exit(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    if qty <= 0:
        return

    direction_u = direction.upper()
    entry_side = "BUY" if direction_u == "LONG" else "SELL"
    exit_side = "SELL" if direction_u == "LONG" else "BUY"

    # 下单
    print(f"[RISK] EXIT bot={bot_id} symbol={symbol} direction={direction_u} qty={qty} reason={reason}")
    try:
        order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=None,
        )
        status, cancel_reason = _order_status_and_reason(order)
        print(f"[RISK] exit order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        filled_qty, avg_px = _finalize_exit_fill(symbol, order_id, client_order_id)

        # ✅ 优先用真实 exit fill；拿不到则用 mark 兜底（不是 worst）
        if filled_qty is None or avg_px is None:
            mk = _get_mark(symbol)
            if mk is None:
                return
            filled_qty = qty
            avg_px = mk

        if filled_qty <= 0:
            return

        record_exit_fifo(
            bot_id=bot_id,
            symbol=symbol,
            entry_side=entry_side,
            exit_qty=filled_qty,
            exit_price=avg_px,
            reason=reason,
        )

        # 清理锁盈（如果仍有剩余仓位，下一轮会重建）
        try:
            clear_lock_level_pct(bot_id, symbol, direction_u)
        except Exception:
            pass

    except Exception as e:
        print(f"[RISK] create_market_order error bot={bot_id} symbol={symbol}:", e)


def _risk_loop():
    print("[RISK] ladder/baseSL thread started")
    while True:
        try:
            bots = sorted(list(LONG_RISK_BOTS | SHORT_RISK_BOTS))

            for bot_id in bots:
                opens = get_bot_open_positions(bot_id)

                for (symbol, direction), v in opens.items():
                    direction_u = direction.upper()

                    if not _bot_allows_direction(bot_id, direction_u):
                        continue

                    qty = v["qty"]
                    entry_price = v["weighted_entry"]

                    # 远程兜底（防止本地/DB 不一致）
                    if qty <= 0:
                        remote = get_open_position_for_symbol(symbol)
                        if remote and remote["size"] > 0 and remote["side"] == direction_u:
                            qty = remote["size"]
                            entry_price = remote["entryPrice"]
                            print(f"[RISK] Found remote position for {bot_id} {symbol}: {qty} @ {entry_price}")

                    if qty <= 0 or entry_price <= 0:
                        continue

                    mk = _get_mark(symbol)
                    if mk is None:
                        continue

                    pnl_pct = _calc_pnl_pct(direction_u, entry_price, mk)

                    # 1) 基础止损优先
                    if _base_sl_hit(direction_u, entry_price, mk):
                        _execute_virtual_exit(
                            bot_id=bot_id,
                            symbol=symbol,
                            direction=direction_u,
                            qty=qty,
                            reason="base_sl_exit",
                        )
                        continue

                    # 2) 锁盈档位更新
                    desired_lock = _desired_lock_level_pct(pnl_pct)
                    current_lock = get_lock_level_pct(bot_id, symbol, direction_u)

                    if desired_lock > current_lock:
                        set_lock_level_pct(bot_id, symbol, direction_u, desired_lock)
                        current_lock = desired_lock
                        print(f"[RISK] LOCK UP bot={bot_id} symbol={symbol} direction={direction_u} pnl={pnl_pct:.4f}% lock={current_lock}%")

                    # 3) 锁盈触发
                    if _lock_hit(direction_u, entry_price, mk, current_lock):
                        _execute_virtual_exit(
                            bot_id=bot_id,
                            symbol=symbol,
                            direction=direction_u,
                            qty=qty,
                            reason="ladder_lock_exit",
                        )

        except Exception as e:
            print("[RISK] loop top-level error:", e)

        time.sleep(RISK_POLL_INTERVAL)


def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            init_db()
            t = threading.Thread(target=_risk_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True
            print("[RISK] thread created")


@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200


# ----------------------------
# PnL API
# ----------------------------
@app.route("/api/pnl", methods=["GET"])
def api_pnl():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    bots = list_bots_with_activity()
    only_bot = request.args.get("bot_id")
    if only_bot:
        bots = [b for b in bots if b == only_bot]

    out = []
    for bot_id in bots:
        base = get_bot_summary(bot_id)
        opens = get_bot_open_positions(bot_id)

        unrealized = Decimal("0")
        open_rows = []

        for (symbol, direction), v in opens.items():
            qty = v["qty"]
            wentry = v["weighted_entry"]
            if qty <= 0:
                continue

            mk = _get_mark(symbol)
            if mk is None:
                continue

            if direction.upper() == "LONG":
                unrealized += (mk - wentry) * qty
            else:
                unrealized += (wentry - mk) * qty

            open_rows.append({
                "symbol": symbol,
                "direction": direction.upper(),
                "qty": str(qty),
                "weighted_entry": str(wentry),
                "mark_price": str(mk),
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
# 简易 dashboard
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
    <div class="muted">真实成交价优先（fills） · 止损 0.5% · 阶梯锁盈（0.15→0.10, 0.45→0.20, +0.10→+0.10）</div>
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
          <span class="pill">BOT 1-5: LONG 风控</span>
          <span class="pill">BOT 6-10: SHORT 风控</span>
          <span class="pill">Others: Strategy-only</span>
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

    # parse json
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    # secret check
    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        print("[WEBHOOK] invalid secret")
        return "forbidden", 403

    symbol = body.get("symbol")
    if not symbol:
        return "missing symbol", 400

    bot_id = str(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal_type_raw = str(body.get("signal_type", "")).lower()
    action_raw = str(body.get("action", "")).lower()
    tv_client_id = body.get("client_id")

    # mode
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

    # -------------------------
    # ENTRY
    # -------------------------
    if mode == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        try:
            budget = _extract_budget_usdt(body)
        except Exception as e:
            print("[ENTRY] budget error:", e)
            return str(e), 400

        try:
            snapped_qty = _compute_entry_qty(symbol, budget)
            if snapped_qty <= 0:
                raise ValueError("qty too small (snap<=0)")
        except Exception as e:
            print(f"[ENTRY] qty compute error: {e}")
            return "qty compute error", 500

        size_str = str(snapped_qty)
        print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} budget={budget} -> qty={size_str}")

        # 下单
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
            }), 200

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        entry_price_dec: Optional[Decimal] = None
        final_qty = snapped_qty

        # ✅ 先按你要求：前 2–3 秒高频拿 fills
        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                max_wait_sec=FILL_FAST_WINDOW_SEC,
                poll_interval=FILL_FAST_POLL_INTERVAL,
                fast_window_sec=FILL_FAST_WINDOW_SEC,
                fast_poll_interval=FILL_FAST_POLL_INTERVAL,
            )
            entry_price_dec = Decimal(str(fill["avg_fill_price"]))
            final_qty = Decimal(str(fill["filled_qty"]))
            print(f"[ENTRY] fill ok bot={bot_id} symbol={symbol} filled_qty={final_qty} avg_fill={entry_price_dec}")

            # cache + record（只要拿到了真实成交）
            key = (bot_id, symbol)
            BOT_POSITIONS[key] = {"side": side_raw, "qty": final_qty, "entry_price": entry_price_dec}
            record_entry(
                bot_id=bot_id,
                symbol=symbol,
                side=side_raw,
                qty=final_qty,
                price=entry_price_dec,
                reason="strategy_entry_fill",
            )

        except Exception as e:
            # ✅ 不再用 worstPrice 回填成交价
            print(f"[ENTRY] Fill-First failed bot={bot_id} symbol={symbol} err:", e)

            # 启动后台追踪 fills（拿到后再入账/风控）
            task_key = f"{bot_id}:{symbol}:{order_id or ''}:{client_order_id or ''}:{int(time.time()*1000)}"
            with _PENDING_LOCK:
                PENDING_FILL_TASKS[task_key] = {
                    "bot_id": bot_id,
                    "symbol": symbol,
                    "side": side_raw,
                    "order_id": order_id,
                    "client_order_id": client_order_id,
                    "requested_qty": str(snapped_qty),
                }
            t = threading.Thread(target=_finalize_entry_fill_async, args=(task_key,), daemon=True)
            t.start()

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
            "fill_pending": entry_price_dec is None,
        }), 200

    # -------------------------
    # EXIT（策略出场）
    # -------------------------
    if mode == "exit":
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

        # 远程兜底
        if not direction_to_close or qty_to_close <= 0:
            print(f"[EXIT] local 0 position, trying remote fallback for {symbol}...")
            remote = get_open_position_for_symbol(symbol)
            if remote and remote["size"] > 0:
                direction_to_close = remote["side"]
                qty_to_close = remote["size"]
                print(f"[EXIT] remote fallback found: {direction_to_close} {qty_to_close}")
            else:
                print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close (local+remote)")
                return jsonify({"status": "no_position"}), 200

        entry_side = "BUY" if direction_to_close == "LONG" else "SELL"
        exit_side = "SELL" if direction_to_close == "LONG" else "BUY"

        print(f"[EXIT] bot={bot_id} symbol={symbol} direction={direction_to_close} qty={qty_to_close} -> exit_side={exit_side}")

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty_to_close),
                reduce_only=True,
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
            }), 200

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        filled_qty, avg_px = _finalize_exit_fill(symbol, order_id, client_order_id)

        # ✅ exit 真实 fills 拿不到就用 mark 兜底
        if filled_qty is None or avg_px is None:
            mk = _get_mark(symbol)
            if mk is None:
                return jsonify({"status": "exit_price_unavailable"}), 200
            filled_qty = qty_to_close
            avg_px = mk

        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=filled_qty,
                exit_price=avg_px,
                reason="strategy_exit",
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (strategy):", e)

        try:
            clear_lock_level_pct(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        if key_local in BOT_POSITIONS:
            # 若未全平，不要清空（这里简单处理：仅当全平才清空）
            BOT_POSITIONS[key_local]["qty"] = Decimal("0")
            BOT_POSITIONS[key_local]["entry_price"] = None

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(filled_qty),
            "exit_price": str(avg_px),
            "order_status": status,
            "cancel_reason": cancel_reason,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
