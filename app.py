import os
import time
import threading
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, Optional, Any, Set

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_market_price,
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

# =========================
# Dashboard auth
# =========================
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

def _require_token():
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


# =========================
# Bot group definitions
# =========================
def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip() for b in env_val.split(",") if b.strip()}

# 默认：BOT_1..BOT_10 走多风控
LONG_LADDER_BOTS = _parse_bot_list(
    os.getenv("LONG_LADDER_BOTS",
              ",".join([f"BOT_{i}" for i in range(1, 11)]))
)

# 默认：BOT_11..BOT_20 走空风控
SHORT_LADDER_BOTS = _parse_bot_list(
    os.getenv("SHORT_LADDER_BOTS",
              ",".join([f"BOT_{i}" for i in range(11, 21)]))
)

# 基础止损百分比（你要求 0.35%）
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.35"))

# 轮询间隔
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "2.0"))

# =========================
# Local cache (optional)
# =========================
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

# 线程控制
_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()


# ------------------------------------------------
# 从 payload 里取 USDT 预算
# ------------------------------------------------
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


# ------------------------------------------------
# 根据 USDT 预算算出 snapped qty
# ------------------------------------------------
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


# ------------------------------------------------
# Ladder logic (your latest spec)
# ------------------------------------------------
TRIGGER_1 = Decimal("0.18")
LOCK_1 = Decimal("0.10")
TRIGGER_2 = Decimal("0.38")
LOCK_2 = Decimal("0.20")
STEP = Decimal("0.20")

def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    """
    你的最新规则：
    0.18 -> 0.10
    0.38 -> 0.20
    0.58 -> 0.40
    之后每 +0.20 触发，锁盈线 +0.20
    """
    if pnl_pct < TRIGGER_1:
        return Decimal("0")

    if pnl_pct < TRIGGER_2:
        return LOCK_1

    # pnl >= 0.38
    # n = floor((pnl - 0.38)/0.20)
    # lock = 0.20 + 0.20*n
    n = (pnl_pct - TRIGGER_2) // STEP
    return LOCK_2 + STEP * n


def _bot_allows_direction(bot_id: str, direction: str) -> bool:
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
        # 用平仓方向的 worst price 作为 mark 参考
        exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
        px_str = get_market_price(symbol, exit_side, str(min_qty))
        return Decimal(str(px_str))
    except Exception as e:
        print(f"[RISK] get_market_price error symbol={symbol} direction={direction}:", e)
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
    真正发 reduceOnly 市价单，并用 FIFO 从该 bot 的 lots 里记账扣减。
    """
    if qty <= 0:
        return

    entry_side = "BUY" if direction.upper() == "LONG" else "SELL"
    exit_side = "SELL" if direction.upper() == "LONG" else "BUY"

    # 用 worst price 作为 exit 参考价写 PnL
    exit_price = _get_current_price(symbol, direction)
    if exit_price is None:
        print(f"[RISK] skip exit due to no price bot={bot_id} symbol={symbol}")
        return

    print(
        f"[RISK] EXIT bot={bot_id} symbol={symbol} direction={direction} "
        f"qty={qty} reason={reason}"
    )

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

        # ✅ PnL FIFO 记账
        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=qty,
                exit_price=exit_price,
                reason=reason,
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (risk):", e)

        # ✅ 清理 ladder 状态
        try:
            clear_lock_level_pct(bot_id, symbol, direction.upper())
        except Exception:
            pass

        # ✅ 同步清一下本地 cache（如果存在）
        key = (bot_id, symbol)
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
        print(f"[RISK] create_market_order error bot={bot_id} symbol={symbol}:", e)


# ------------------------------------------------
# ✅ Risk monitor thread
# ------------------------------------------------
def _risk_loop():
    print("[RISK] ladder/baseSL thread started")
    while True:
        try:
            # 只扫描你定义的风控组
            bots = sorted(list(LONG_LADDER_BOTS | SHORT_LADDER_BOTS))

            for bot_id in bots:
                opens = get_bot_open_positions(bot_id)

                for (symbol, direction), v in opens.items():
                    direction = direction.upper()

                    if not _bot_allows_direction(bot_id, direction):
                        continue

                    qty = v["qty"]
                    entry_price = v["weighted_entry"]

                    if qty <= 0 or entry_price <= 0:
                        continue

                    current_price = _get_current_price(symbol, direction)
                    if current_price is None:
                        continue

                    pnl_pct = _calc_pnl_pct(direction, entry_price, current_price)

                    # 1) ✅ 基础止损优先
                    if _base_sl_hit(direction, entry_price, current_price):
                        _execute_virtual_exit(
                            bot_id=bot_id,
                            symbol=symbol,
                            direction=direction,
                            qty=qty,
                            reason="base_sl_exit",
                        )
                        continue

                    # 2) ✅ 计算“希望达到的锁盈档位”
                    desired_lock = _desired_lock_level_pct(pnl_pct)

                    # 当前已生效锁盈档位（持久化）
                    current_lock = get_lock_level_pct(bot_id, symbol, direction)

                    # 只上不下
                    if desired_lock > current_lock:
                        set_lock_level_pct(bot_id, symbol, direction, desired_lock)
                        current_lock = desired_lock
                        print(
                            f"[RISK] LOCK UP bot={bot_id} symbol={symbol} direction={direction} "
                            f"pnl={pnl_pct:.4f}% lock_level={current_lock}%"
                        )

                    # 3) ✅ 锁盈触发
                    if _lock_hit(direction, entry_price, current_price, current_lock):
                        _execute_virtual_exit(
                            bot_id=bot_id,
                            symbol=symbol,
                            direction=direction,
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


# =========================
# ✅ PnL API
# =========================
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

            px = _get_current_price(symbol, direction)
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

    return jsonify({
        "ts": int(time.time()),
        "bots": out
    }), 200


# =========================
# ✅ Dashboard 页面（保持你之前的版本即可）
# =========================
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
    <div class="muted">bot 独立 lots 记账 · 含基础止损/阶梯锁盈线程</div>
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
          <span class="pill">BOT 1-10: LONG 阶梯锁盈 + 基础SL</span>
          <span class="pill">BOT 11-20: SHORT 阶梯锁盈 + 基础SL</span>
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


# =========================
# ✅ Webhook
# =========================
@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()

    # 1) Parse JSON & secret
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

    # 2) Determine mode
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

    # ========================================================
    # ENTRY
    # ========================================================
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

        computed = (order or {}).get("computed") or {}
        price_str = computed.get("price")

        entry_price_dec: Optional[Decimal] = None
        try:
            if price_str is not None:
                entry_price_dec = Decimal(str(price_str))
        except Exception:
            entry_price_dec = None

        # 本地 cache
        key = (bot_id, symbol)
        BOT_POSITIONS[key] = {
            "side": side_raw,
            "qty": snapped_qty,
            "entry_price": entry_price_dec,
        }

        # ✅ PnL ENTRY 记账
        if entry_price_dec is not None:
            try:
                record_entry(
                    bot_id=bot_id,
                    symbol=symbol,
                    side=side_raw,
                    qty=snapped_qty,
                    price=entry_price_dec,
                    reason="strategy_entry",
                )
            except Exception as e:
                print("[PNL] record_entry error:", e)

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": size_str,
            "entry_price": str(entry_price_dec) if entry_price_dec else None,
            "order_status": status,
            "cancel_reason": cancel_reason,
        }), 200

    # ========================================================
    # EXIT（策略出场）
    # ========================================================
    if mode == "exit":
        # ✅ 这里不再只依赖本地 cache
        # 以 PnL lots 为准来决定“这个 bot 真正该平多少”
        entry_side_hint = side_raw if side_raw in ("BUY", "SELL") else "BUY"

        # 从 lots 汇总出该 bot 在这个 symbol 的两边数量
        opens = get_bot_open_positions(bot_id)

        # 判断你这次 exit 想平哪边（用本地 side + 习惯：exit 通常是平当前方向）
        # 我们优先尝试平 LONG，然后再 SHORT（更保守）
        long_key = (symbol, "LONG")
        short_key = (symbol, "SHORT")

        long_qty = opens.get(long_key, {}).get("qty", Decimal("0")) if long_key in opens else Decimal("0")
        short_qty = opens.get(short_key, {}).get("qty", Decimal("0")) if short_key in opens else Decimal("0")

        # 如果本地 cache 有 side，就按那边优先
        key_local = (bot_id, symbol)
        local = BOT_POSITIONS.get(key_local)
        if local and str(local.get("side", "")).upper() == "SELL":
            # 本地认为是空方向
            preferred = "SHORT"
        else:
            preferred = "LONG"

        direction_to_close = None
        qty_to_close = Decimal("0")

        if preferred == "LONG" and long_qty > 0:
            direction_to_close = "LONG"
            qty_to_close = long_qty
        elif preferred == "SHORT" and short_qty > 0:
            direction_to_close = "SHORT"
            qty_to_close = short_qty
        elif long_qty > 0:
            direction_to_close = "LONG"
            qty_to_close = long_qty
        elif short_qty > 0:
            direction_to_close = "SHORT"
            qty_to_close = short_qty

        if not direction_to_close or qty_to_close <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no lots position to close")
            return jsonify({"status": "no_position"}), 200

        # 用当前方向决定 entry_side
        entry_side = "BUY" if direction_to_close == "LONG" else "SELL"
        exit_side = "SELL" if direction_to_close == "LONG" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} "
            f"direction={direction_to_close} qty={qty_to_close} -> exit_side={exit_side}"
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
            }), 200

        # 用 worst 价作为参考 exit 价
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

        # 清本地 cache
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
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
