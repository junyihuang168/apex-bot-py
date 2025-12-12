import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_fill_summary,              # ✅ 真实成交价（高频轮询 2–3 秒）
    get_open_position_for_symbol,  # ✅ 远程兜底查仓
    get_ticker_price,              # ✅ 风控触发用：更贴近图表的价格（mark/last/index）
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
    get_open_qty,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ----------------------------
# 并发控制：每个 (bot_id, symbol) 一把锁
# 特别针对 SMC：BOT_1 / BOT_6 容易高频触发
# ----------------------------
_LOCKS: Dict[Tuple[str, str], threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def _get_lock(bot_id: str, symbol: str) -> threading.Lock:
    key = (bot_id, symbol)
    with _LOCKS_GUARD:
        if key not in _LOCKS:
            _LOCKS[key] = threading.Lock()
        return _LOCKS[key]


def _require_token() -> bool:
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip() for b in (env_val or "").split(",") if b.strip()}


# ✅ 你的最新分组（旧的 BOT_1~10 LONG / BOT_11~20 SHORT 已彻底删除）
RISK_LONG_BOTS = _parse_bot_list(os.getenv("RISK_LONG_BOTS", ",".join([f"BOT_{i}" for i in range(1, 6)])))
RISK_SHORT_BOTS = _parse_bot_list(os.getenv("RISK_SHORT_BOTS", ",".join([f"BOT_{i}" for i in range(6, 11)])))

# ✅ 基础止损默认 0.5%
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))

# 风控轮询间隔（建议 0.5~1.0 更贴近你要的触发感；默认 1.0）
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.0"))

# fills 拉取（前 2–3 秒高频）
FILL_MAX_WAIT_SEC = float(os.getenv("FILL_MAX_WAIT_SEC", "3.0"))
FILL_POLL_FAST = float(os.getenv("FILL_POLL_FAST", "0.15"))
FILL_POLL_SLOW = float(os.getenv("FILL_POLL_SLOW", "0.35"))
FILL_FAST_WINDOW = float(os.getenv("FILL_FAST_WINDOW", "2.2"))

# 风控价格源：mark/last/index（更贴近图表）
PRICE_PREFER = os.getenv("RISK_PRICE_PREFER", "mark").lower().strip()  # mark / last / index

# 本地 cache（只作辅助，不作为真相源）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()


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
# （估值用 ticker last，不用 worstPrice）
# ----------------------------
def _compute_entry_qty(symbol: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    decimals = rules["qty_decimals"]

    px = Decimal(get_ticker_price(symbol, prefer="last"))
    if px <= 0:
        raise ValueError("ticker price <= 0")

    theoretical_qty = budget / px
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    if snapped_qty <= 0:
        raise ValueError(f"snapped_qty <= 0, symbol={symbol}, budget={budget}")

    # 统一小数精度
    quantum = Decimal("1").scaleb(-decimals)
    return snapped_qty.quantize(quantum)


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
# 你的阶梯锁盈规则（按你最新要求）
# pnl 是百分比（例如 0.15 代表 0.15%）
# ----------------------------
TRIG_1 = Decimal(os.getenv("LADDER_TRIG_1", "0.15"))
LOCK_1 = Decimal(os.getenv("LADDER_LOCK_1", "0.10"))

TRIG_2 = Decimal(os.getenv("LADDER_TRIG_2", "0.45"))
LOCK_2 = Decimal(os.getenv("LADDER_LOCK_2", "0.20"))

TRIG_3 = Decimal(os.getenv("LADDER_TRIG_3", "0.55"))
LOCK_3 = Decimal(os.getenv("LADDER_LOCK_3", "0.30"))

TRIG_4 = Decimal(os.getenv("LADDER_TRIG_4", "0.65"))
LOCK_4 = Decimal(os.getenv("LADDER_LOCK_4", "0.40"))

STEP_AFTER_4 = Decimal(os.getenv("LADDER_STEP_AFTER_4", "0.10"))  # 之后每 +0.10% → lock +0.10%


def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    if pnl_pct < TRIG_1:
        return Decimal("0")
    if pnl_pct < TRIG_2:
        return LOCK_1
    if pnl_pct < TRIG_3:
        return LOCK_2
    if pnl_pct < TRIG_4:
        return LOCK_3

    n = (pnl_pct - TRIG_4) // STEP_AFTER_4
    return LOCK_4 + STEP_AFTER_4 * n


def _bot_allows_direction(bot_id: str, direction: str) -> bool:
    d = direction.upper()
    if bot_id in RISK_LONG_BOTS and d == "LONG":
        return True
    if bot_id in RISK_SHORT_BOTS and d == "SHORT":
        return True
    return False


def _get_current_price(symbol: str) -> Optional[Decimal]:
    """
    风控触发价：用 ticker (mark/last/index) —— 更贴近你看的图
    """
    try:
        px_str = get_ticker_price(symbol, prefer=PRICE_PREFER)
        px = Decimal(str(px_str))
        if px > 0:
            return px
        return None
    except Exception as e:
        print(f"[RISK] get_ticker_price error symbol={symbol} prefer={PRICE_PREFER}:", e)
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


def _fill_first_price(symbol: str, order_id: Optional[str], client_order_id: Optional[str]) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    返回 (filled_qty, avg_fill_price)
    """
    fill = get_fill_summary(
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        max_wait_sec=FILL_MAX_WAIT_SEC,
        poll_interval_fast=FILL_POLL_FAST,
        poll_interval_slow=FILL_POLL_SLOW,
        fast_window_sec=FILL_FAST_WINDOW,
    )
    q = Decimal(str(fill["filled_qty"]))
    p = Decimal(str(fill["avg_fill_price"]))
    if q > 0 and p > 0:
        return q, p
    return None, None


def _execute_virtual_exit(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    if qty <= 0:
        return

    lock = _get_lock(bot_id, symbol)
    if not lock.acquire(blocking=False):
        print(f"[RISK] skip exit due to lock busy bot={bot_id} symbol={symbol}")
        return

    try:
        direction_u = direction.upper()
        entry_side = "BUY" if direction_u == "LONG" else "SELL"
        exit_side = "SELL" if direction_u == "LONG" else "BUY"

        print(f"[RISK] EXIT bot={bot_id} symbol={symbol} direction={direction_u} qty={qty} reason={reason}")

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

        # ✅ 出场也用真实成交价（Fill-first）
        filled_qty = qty
        exit_price = None
        try:
            fq, fp = _fill_first_price(symbol, order_id, client_order_id)
            if fq and fp:
                filled_qty = fq
                exit_price = fp
                print(f"[RISK] exit fill ok bot={bot_id} symbol={symbol} filled_qty={filled_qty} avg_fill={exit_price}")
        except Exception as e:
            print(f"[RISK] exit fill not available bot={bot_id} symbol={symbol} err:", e)

        # 若 fills 仍拿不到：用 ticker 兜底（不使用 worstPrice 作为默认）
        if exit_price is None:
            px = _get_current_price(symbol)
            if px is None:
                print(f"[RISK] skip record_exit due to no price bot={bot_id} symbol={symbol}")
                return
            exit_price = px

        # ✅ FIFO 记账，仅扣该 bot 自己的 lots
        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=filled_qty,
                exit_price=exit_price,
                reason=reason,
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (risk):", e)

        # 清理锁盈状态：仅当该方向已无剩余仓位
        try:
            remain = get_open_qty(bot_id, symbol, direction_u)
            if remain <= 0:
                clear_lock_level_pct(bot_id, symbol, direction_u)
        except Exception:
            pass

        # 本地 cache 更新（非真相源）
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)
        if pos:
            pos_qty = Decimal(str(pos.get("qty") or "0"))
            new_qty = pos_qty - filled_qty
            if new_qty < 0:
                new_qty = Decimal("0")
            pos["qty"] = new_qty
            if new_qty == 0:
                pos["entry_price"] = None

    except Exception as e:
        print(f"[RISK] create_market_order error bot={bot_id} symbol={symbol}:", e)
    finally:
        lock.release()


def _risk_loop():
    print("[RISK] thread started (fill-first + ticker price for triggers)")
    while True:
        try:
            bots = sorted(list(RISK_LONG_BOTS | RISK_SHORT_BOTS))

            for bot_id in bots:
                opens = get_bot_open_positions(bot_id)

                for (symbol, direction), v in opens.items():
                    direction_u = direction.upper()

                    if not _bot_allows_direction(bot_id, direction_u):
                        continue

                    qty = v["qty"]
                    entry_price = v["weighted_entry"]

                    # ✅ 远程兜底：如果本地记录异常，去交易所拿 entryPrice
                    if qty <= 0 or entry_price <= 0:
                        remote = get_open_position_for_symbol(symbol)
                        if remote and remote["size"] > 0 and remote["side"] == direction_u and remote["entryPrice"]:
                            qty = remote["size"]
                            entry_price = remote["entryPrice"]
                            print(f"[RISK] Remote fallback: bot={bot_id} {symbol} {qty} @ {entry_price}")

                    if qty <= 0 or entry_price <= 0:
                        continue

                    current_price = _get_current_price(symbol)
                    if current_price is None:
                        continue

                    pnl_pct = _calc_pnl_pct(direction_u, entry_price, current_price)

                    # 1) 基础止损优先（默认 0.5%）
                    if _base_sl_hit(direction_u, entry_price, current_price):
                        _execute_virtual_exit(bot_id, symbol, direction_u, qty, "base_sl_exit")
                        continue

                    # 2) 阶梯锁盈档位更新
                    desired_lock = _desired_lock_level_pct(pnl_pct)
                    current_lock = get_lock_level_pct(bot_id, symbol, direction_u)

                    if desired_lock > current_lock:
                        set_lock_level_pct(bot_id, symbol, direction_u, desired_lock)
                        current_lock = desired_lock
                        print(f"[RISK] LOCK UP bot={bot_id} symbol={symbol} dir={direction_u} pnl={pnl_pct:.4f}% lock={current_lock}%")

                    # 3) 锁盈触发
                    if _lock_hit(direction_u, entry_price, current_price, current_lock):
                        _execute_virtual_exit(bot_id, symbol, direction_u, qty, "ladder_lock_exit")

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
            if qty <= 0 or wentry <= 0:
                continue

            px = _get_current_price(symbol)
            if px is None:
                continue

            if direction.upper() == "LONG":
                unrealized += (px - wentry) * qty
            else:
                unrealized += (wentry - px) * qty

            open_rows.append({
                "symbol": symbol,
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
    <div class="muted">独立 lots 记账 · 真实成交价优先 · 触发价使用 ticker(mark/last) · SL 0.5% · 阶梯锁盈</div>
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

    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)
    if not isinstance(body, dict):
        return "bad payload", 400

    if WEBHOOK_SECRET:
        if body.get("secret") != WEBHOOK_SECRET:
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

    # (bot,symbol) 互斥，避免 SMC 乱序/连环警报
    lock = _get_lock(bot_id, symbol)
    with lock:
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
            except Exception as e:
                print("[ENTRY] qty compute error:", e)
                return f"qty compute error: {e}", 500

            size_str = str(snapped_qty)
            print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} budget={budget} -> qty={size_str}")

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

            # ✅ 真实成交价系统默认（Fill-first）
            entry_price_dec: Optional[Decimal] = None
            final_qty = snapped_qty

            try:
                fq, fp = _fill_first_price(symbol, order_id, client_order_id)
                if fq and fp:
                    final_qty = fq
                    entry_price_dec = fp
                    print(f"[ENTRY] fill ok bot={bot_id} symbol={symbol} filled_qty={final_qty} avg_fill={entry_price_dec}")
            except Exception as e:
                print(f"[ENTRY] fill unavailable bot={bot_id} symbol={symbol} err:", e)

            # 如果 fills 暂时拿不到：用远程仓位 entryPrice 再兜底一次（不使用 worstPrice 作为默认）
            if entry_price_dec is None:
                try:
                    remote = get_open_position_for_symbol(symbol)
                    # side_raw BUY=>LONG / SELL=>SHORT
                    want_dir = "LONG" if side_raw == "BUY" else "SHORT"
                    if remote and remote["side"] == want_dir and remote["size"] > 0 and remote["entryPrice"]:
                        final_qty = remote["size"]
                        entry_price_dec = remote["entryPrice"]
                        print(f"[ENTRY] remote entryPrice fallback ok bot={bot_id} symbol={symbol} qty={final_qty} entry={entry_price_dec}")
                except Exception as e:
                    print("[ENTRY] remote entryPrice fallback err:", e)

            # 最后兜底：用 ticker last（不用于“真实成交”，但至少不走 worstPrice）
            if entry_price_dec is None:
                try:
                    entry_price_dec = Decimal(get_ticker_price(symbol, prefer="last"))
                    print(f"[ENTRY] ticker last fallback used bot={bot_id} symbol={symbol} entry={entry_price_dec}")
                except Exception:
                    entry_price_dec = None

            # 本地 cache
            key = (bot_id, symbol)
            BOT_POSITIONS[key] = {
                "side": side_raw,
                "qty": final_qty,
                "entry_price": entry_price_dec,
            }

            # ✅ 独立 lots 记账（只在我们拿到“可用价格”时记）
            if entry_price_dec is not None and final_qty > 0:
                try:
                    record_entry(
                        bot_id=bot_id,
                        symbol=symbol,
                        side=side_raw,
                        qty=final_qty,
                        price=entry_price_dec,
                        reason="entry_fill_first" if order_id else "entry",
                    )
                except Exception as e:
                    print("[PNL] record_entry error:", e)

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
            }), 200

        # -------------------------
        # EXIT（策略出场）
        # 以 lots 为准，只平本 bot
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
                    direction_to_close = remote["side"]  # LONG/SHORT
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

            # ✅ 出场也 Fill-first
            filled_qty = qty_to_close
            exit_price = None
            try:
                fq, fp = _fill_first_price(symbol, order_id, client_order_id)
                if fq and fp:
                    filled_qty = fq
                    exit_price = fp
                    print(f"[EXIT] fill ok bot={bot_id} symbol={symbol} filled_qty={filled_qty} avg_fill={exit_price}")
            except Exception as e:
                print("[EXIT] fill unavailable:", e)

            if exit_price is None:
                px = _get_current_price(symbol)
                if px is None:
                    return jsonify({"status": "ok", "note": "exit_done_but_no_price_for_accounting"}), 200
                exit_price = px

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
                remain = get_open_qty(bot_id, symbol, direction_to_close)
                if remain <= 0:
                    clear_lock_level_pct(bot_id, symbol, direction_to_close)
            except Exception:
                pass

            if key_local in BOT_POSITIONS:
                # 只做轻量更新：留给 DB 真相源
                BOT_POSITIONS[key_local]["qty"] = Decimal("0")
                BOT_POSITIONS[key_local]["entry_price"] = None

            return jsonify({
                "status": "ok",
                "mode": "exit",
                "bot_id": bot_id,
                "symbol": symbol,
                "exit_side": exit_side,
                "closed_qty": str(filled_qty),
                "exit_price": str(exit_price),
                "order_status": status,
                "cancel_reason": cancel_reason,
            }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
