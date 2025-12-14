import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set, Any

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_market_price,
    get_fill_summary,              # ✅ 真实成交回写（保持你原逻辑）
    get_open_position_for_symbol,  # ✅ 远程查仓兜底（按你规则严格限制触发）
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
    # ✅ 新增：幂等去重
    is_signal_processed,
    mark_signal_processed,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# Dashboard token（可选）
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# 退出互斥窗口（秒）：防止 webhook exit 与 risk exit 同时触发重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
# 例：REMOTE_FALLBACK_SYMBOLS="ZEC-USDT,BTC-USDT"
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}

# 风控轮询间隔
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "2.0"))

# ✅ 基础止损默认 0.5%
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))

def _require_token() -> bool:
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip().upper() for b in env_val.split(",") if b.strip()}


def _canon_bot_id(bot_id: str) -> str:
    """
    Canonicalize bot id:
    - Accept: "BOT_1", "bot1", "bot_1", "BOT 1"
    - Output: "BOT_1"
    """
    s = str(bot_id or "").strip().upper().replace(" ", "").replace("-", "_")
    if not s:
        return "BOT_0"

    if s.startswith("BOT_"):
        core = s[4:]
    elif s.startswith("BOT"):
        core = s[3:]
    else:
        # If user passes pure number "1"
        core = s

    core = core.replace("_", "")
    if core.isdigit():
        return f"BOT_{int(core)}"
    # fallback
    return s


# BOT 1-10：做多风控
LONG_LADDER_BOTS = _parse_bot_list(
    os.getenv("LONG_LADDER_BOTS", ",".join([f"BOT_{i}" for i in range(1, 11)]))
)

# BOT 11-20：做空风控
SHORT_LADDER_BOTS = _parse_bot_list(
    os.getenv("SHORT_LADDER_BOTS", ",".join([f"BOT_{i}" for i in range(11, 21)]))
)

# 本地 cache（仅辅助）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ✅ 退出互斥：bot+symbol 粒度 cooldown
_EXIT_LOCK = threading.Lock()
_LAST_EXIT_TS: Dict[Tuple[str, str], float] = {}


def _exit_guard_allow(bot_id: str, symbol: str) -> bool:
    """
    Return True if allowed to execute an exit now; False if within cooldown window.
    """
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
    """
    Prefer user-provided ids:
    - signal_id / alert_id / id / tv_id
    Otherwise generate a stable-ish id by timestamp + core fields.
    """
    for k in ("signal_id", "alert_id", "id", "tv_id", "client_id"):
        v = body.get(k)
        if v:
            return f"{mode}:{bot_id}:{symbol}:{str(v)}"

    # fallback to time bucket (seconds) to dedup typical retries
    ts = body.get("ts") or body.get("time") or int(time.time())
    try:
        ts_int = int(float(str(ts)))
    except Exception:
        ts_int = int(time.time())

    sig = str(body.get("signal_type") or body.get("action") or "").lower().strip()
    side = str(body.get("side") or "").upper().strip()

    return f"{mode}:{bot_id}:{symbol}:{sig}:{side}:{ts_int}"


# ----------------------------
# 你的阶梯锁盈规则 (Local Ladder Logic - Original)
# ----------------------------
TRIGGER_1 = Decimal("0.18")
LOCK_1 = Decimal("0.10")
TRIGGER_2 = Decimal("0.38")
LOCK_2 = Decimal("0.20")
STEP = Decimal("0.20")


def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    if pnl_pct < TRIGGER_1:
        return Decimal("0")
    if pnl_pct < TRIGGER_2:
        return LOCK_1
    n = (pnl_pct - TRIGGER_2) // STEP
    return LOCK_2 + STEP * n


def _bot_allows_direction(bot_id: str, direction: str) -> bool:
    bot_id = _canon_bot_id(bot_id)
    d = direction.upper()
    if bot_id in LONG_LADDER_BOTS and d == "LONG":
        return True
    if bot_id in SHORT_LADDER_BOTS and d == "SHORT":
        return True
    return False


def _get_current_price(symbol: str, direction: str) -> Optional[Decimal]:
    try:
        rules = _get_symbol_rules(symbol)
        min_qty = rules["min_qty"]
        exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
        px_str = get_market_price(symbol, exit_side, str(min_qty))
        return Decimal(str(px_str))
    except Exception as e:
        print(f"[RISK] get_market_price error symbol={symbol} direction={direction}: {e}")
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


def _execute_virtual_exit(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    """
    Risk-thread exit. Uses lots as truth source (same as your original),
    adds:
    - exit cooldown guard (avoid duplicates)
    - processed_signals idempotency record
    """
    bot_id = _canon_bot_id(bot_id)
    symbol_u = str(symbol or "").upper().strip()
    direction_u = direction.upper()

    if qty <= 0:
        return

    # cooldown guard
    if not _exit_guard_allow(bot_id, symbol_u):
        print(f"[RISK] EXIT SKIP (cooldown) bot={bot_id} symbol={symbol_u} reason={reason}")
        return

    # idempotency key
    sig_id = f"risk_exit:{bot_id}:{symbol_u}:{direction_u}:{reason}:{int(time.time())}"
    if is_signal_processed(bot_id, sig_id):
        print(f"[RISK] EXIT SKIP (dedup) bot={bot_id} symbol={symbol_u} reason={reason} sig={sig_id}")
        return
    mark_signal_processed(bot_id, sig_id, kind="risk_exit")

    entry_side = "BUY" if direction_u == "LONG" else "SELL"
    exit_side = "SELL" if direction_u == "LONG" else "BUY"

    exit_price = _get_current_price(symbol_u, direction_u)
    if exit_price is None:
        print(f"[RISK] skip exit due to no price bot={bot_id} symbol={symbol_u}")
        return

    print(
        f"[RISK] EXIT bot={bot_id} symbol={symbol_u} direction={direction_u} "
        f"qty={qty} reason={reason} ref_exit={exit_price}"
    )

    try:
        order = create_market_order(
            symbol=symbol_u,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=None,
        )
        status, cancel_reason = _order_status_and_reason(order)
        print(f"[RISK] exit order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return

        # FIFO 记账，仅扣该 bot 自己的 lots
        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol_u,
                entry_side=entry_side,
                exit_qty=qty,
                exit_price=exit_price,
                reason=reason,
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (risk):", e)

        # 清理锁盈状态
        try:
            clear_lock_level_pct(bot_id, symbol_u, direction_u)
        except Exception:
            pass

        # 本地 cache 清理（非真相源）
        key = (bot_id, symbol_u)
        pos = BOT_POSITIONS.get(key)
        if pos:
            pos_qty = Decimal(str(pos.get("qty") or "0"))
            new_qty = pos_qty - qty
            if new_qty < 0:
                new_qty = Decimal("0")
            pos["qty"] = new_qty
            if new_qty == 0:
                pos["entry_price"] = None

    except Exception as e:
        print(f"[RISK] create_market_order error bot={bot_id} symbol={symbol_u}: {e}")


def _risk_loop():
    print("[RISK] ladder/baseSL thread started (using The Polling Loop)")
    while True:
        try:
            bots = sorted(list(LONG_LADDER_BOTS | SHORT_LADDER_BOTS))

            for bot_id in bots:
                bot_id = _canon_bot_id(bot_id)
                opens = get_bot_open_positions(bot_id)

                for (symbol, direction), v in opens.items():
                    symbol_u = str(symbol or "").upper().strip()
                    direction_u = direction.upper()

                    if not _bot_allows_direction(bot_id, direction_u):
                        continue

                    qty = v["qty"]
                    entry_price = v["weighted_entry"]

                    if qty <= 0 or entry_price <= 0:
                        continue

                    current_price = _get_current_price(symbol_u, direction_u)
                    if current_price is None:
                        continue

                    pnl_pct = _calc_pnl_pct(direction_u, entry_price, current_price)

                    # 1) 基础止损优先（默认 0.5%）
                    if _base_sl_hit(direction_u, entry_price, current_price):
                        _execute_virtual_exit(
                            bot_id=bot_id,
                            symbol=symbol_u,
                            direction=direction_u,
                            qty=qty,
                            reason="base_sl_exit",
                        )
                        continue

                    # 2) 计算目标锁盈档位 (Local Ladder Logic)
                    desired_lock = _desired_lock_level_pct(pnl_pct)
                    current_lock = get_lock_level_pct(bot_id, symbol_u, direction_u)

                    if desired_lock > current_lock:
                        set_lock_level_pct(bot_id, symbol_u, direction_u, desired_lock)
                        current_lock = desired_lock
                        print(
                            f"[RISK] LOCK UP bot={bot_id} symbol={symbol_u} direction={direction_u} "
                            f"pnl={pnl_pct:.4f}% lock_level={current_lock}%"
                        )

                    # 3) 锁盈触发
                    if _lock_hit(direction_u, entry_price, current_price, current_lock):
                        _execute_virtual_exit(
                            bot_id=bot_id,
                            symbol=symbol_u,
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

            px = _get_current_price(symbol, direction)
            if px is None:
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

    # 默认按总已实现排序
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
    <div class="muted">独立 lots 记账 · 真实成交价优先 · 基础止损默认 0.5% · 阶梯锁盈 · Exit cooldown 防重复</div>
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
          <span class="pill">BOT 1-10: LONG 阶梯锁盈 + 基础SL(0.5%)</span>
          <span class="pill">BOT 11-20: SHORT 阶梯锁盈 + 基础SL(0.5%)</span>
          <span class="pill">REMOTE_FALLBACK_SYMBOLS: 仅本地无 lots 才允许远程兜底</span>
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
    if WEBHOOK_SECRET:
        if body.get("secret") != WEBHOOK_SECRET:
            print("[WEBHOOK] invalid secret")
            return "forbidden", 403

    symbol = body.get("symbol")
    if not symbol:
        return "missing symbol", 400
    symbol = str(symbol).upper().strip()

    bot_id = _canon_bot_id(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal_type_raw = str(body.get("signal_type", "")).lower()
    action_raw = str(body.get("action", "")).lower()
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

        # ✅ 真实成交价优先 (保持你原逻辑)
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

        # ✅ 独立 lots 记账
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
    # EXIT（策略出场）
    # lots 为准，裁剪只平本 bot
    # 远程兜底：仅本地无 lots 且 symbol 在 REMOTE_FALLBACK_SYMBOLS
    # -------------------------
    if mode == "exit":
        # exit cooldown guard
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
                    print(f"[EXIT] no position to close (local empty + remote none). bot={bot_id} symbol={symbol}")
                    return jsonify({"status": "no_position"}), 200
            else:
                print(f"[EXIT] local lots empty; remote fallback DISABLED (not whitelisted). bot={bot_id} symbol={symbol} sig={sig_id}")
                return jsonify({"status": "no_position"}), 200

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
                "signal_id": sig_id,
            }), 200

        exit_price = _get_current_price(symbol, direction_to_close) or Decimal("0")

        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=qty_to_close,
                exit_price=exit_price,
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
