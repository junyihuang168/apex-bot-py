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
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
)

from risk_manager import (
    start_risk_manager,
    update_runtime_bot_groups,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# 交易总开关（你 env 里有 ENABLE_LIVE_TRADING=true）
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "true").lower() in ("1", "true", "yes", "y", "on")


def _require_token() -> bool:
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip() for b in str(env_val or "").split(",") if b.strip()}


# ✅ 你最新规则：BOT_1~5 只管 LONG；BOT_6~10 只管 SHORT
LONG_RISK_BOTS = _parse_bot_list(os.getenv("LONG_RISK_BOTS", ",".join([f"BOT_{i}" for i in range(1, 6)])))
SHORT_RISK_BOTS = _parse_bot_list(os.getenv("SHORT_RISK_BOTS", ",".join([f"BOT_{i}" for i in range(6, 11)])))

# 把 bot 分组同步给 risk_manager
update_runtime_bot_groups(LONG_RISK_BOTS, SHORT_RISK_BOTS)

# fills 等待参数（按你要求：前 2–3 秒高频）
FILL_MAX_WAIT_SEC = float(os.getenv("FILL_MAX_WAIT_SEC", "3.0"))
FILL_POLL_INTERVAL = float(os.getenv("FILL_POLL_INTERVAL", "0.10"))


_MONITOR_STARTED = False
_MONITOR_LOCK = threading.Lock()


def _ensure_started():
    global _MONITOR_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_STARTED:
            return
        init_db()
        start_risk_manager()
        _MONITOR_STARTED = True
        print("[BOOT] risk manager started")


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


def _safe_decimal(x) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        d = Decimal(str(x))
        return d if d > 0 else None
    except Exception:
        return None


def _compute_entry_qty(symbol: str, side: str, budget_usdt: Decimal) -> Decimal:
    """
    预算 -> qty（用最小 qty 的 worst price 做参考）
    """
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    ref_px = Decimal(str(get_market_price(symbol, side, str(min_qty))))
    if ref_px <= 0:
        raise ValueError("ticker price unavailable")

    theoretical = budget_usdt / ref_px

    # 用 stepSize/minQty snap（直接复用 apex_client 的规则）
    from apex_client import _snap_quantity
    snapped = _snap_quantity(symbol, theoretical)
    if snapped <= 0:
        raise ValueError("snapped_qty <= 0")
    return snapped


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


@app.route("/", methods=["GET"])
def index():
    _ensure_started()
    return "OK", 200


# ----------------------------
# PnL API
# ----------------------------
@app.route("/api/pnl", methods=["GET"])
def api_pnl():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_started()

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

            # 用最小 qty 的 worst price 当 mark（偏保守）
            try:
                rules = _get_symbol_rules(symbol)
                min_qty = rules["min_qty"]
                exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
                px = Decimal(str(get_market_price(symbol, exit_side, str(min_qty))))
            except Exception:
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


# ----------------------------
# Dashboard
# ----------------------------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not _require_token():
        return Response("Forbidden", status=403)

    _ensure_started()

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
    <div class="muted">FIFO lots · risk bots: BOT_1~5 LONG / BOT_6~10 SHORT</div>
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
    _ensure_started()

    # 1) parse json
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    if not isinstance(body, dict):
        return "bad payload", 400

    # 2) secret check
    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        print("[WEBHOOK] invalid secret")
        return "forbidden", 403

    if not ENABLE_LIVE_TRADING:
        return jsonify({"status": "disabled"}), 200

    symbol = body.get("symbol")
    if not symbol:
        return "missing symbol", 400

    bot_id = str(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal_type_raw = str(body.get("signal_type", "")).lower()
    action_raw = str(body.get("action", "")).lower()
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

    # -------------------------
    # ENTRY
    # -------------------------
    if mode == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        try:
            budget = _extract_budget_usdt(body)
            qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            print("[ENTRY] qty compute error:", e)
            return jsonify({"status": "qty_compute_error", "err": str(e)}), 200

        print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} budget={budget} qty={qty}")

        try:
            order = create_market_order(
                symbol=symbol,
                side=side_raw,
                size=str(qty),
                reduce_only=False,
                client_id=tv_client_id,
            )
        except Exception as e:
            print("[ENTRY] create_market_order error:", e)
            return jsonify({"status": "order_error", "err": str(e)}), 200

        status, cancel_reason = _order_status_and_reason(order)
        print(f"[ENTRY] order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return jsonify({
                "status": "order_rejected",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "request_qty": str(qty),
                "order_status": status,
                "cancel_reason": cancel_reason,
            }), 200

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        entry_price = None
        filled_qty = None

        # ✅ fill-first（2–3 秒高频）
        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                max_wait_sec=FILL_MAX_WAIT_SEC,
                poll_interval=FILL_POLL_INTERVAL,
            )
            entry_price = _safe_decimal(fill.get("avg_fill_price"))
            filled_qty = _safe_decimal(fill.get("filled_qty"))
        except Exception as e:
            print(f"[ENTRY] fill-first failed bot={bot_id} symbol={symbol} err:", e)

        # 如果 fill 拿到就立刻记账
        if entry_price and filled_qty:
            try:
                record_entry(
                    bot_id=bot_id,
                    symbol=symbol,
                    side=side_raw,
                    qty=filled_qty,
                    price=entry_price,
                    reason="entry_fill",
                )
                print(f"[PNL] entry recorded bot={bot_id} {symbol} qty={filled_qty} px={entry_price}")
            except Exception as e:
                print("[PNL] record_entry error:", e)
        else:
            # 兜底：尝试远程查仓 entryPrice（有些账户返回 0，这里只在 >0 时采用）
            try:
                remote = get_open_position_for_symbol(symbol)
                if remote and remote.get("entryPrice"):
                    ep = _safe_decimal(remote["entryPrice"])
                    sz = _safe_decimal(remote.get("size"))
                    if ep and sz:
                        entry_price = ep
                        filled_qty = sz
                        record_entry(
                            bot_id=bot_id,
                            symbol=symbol,
                            side=side_raw,
                            qty=filled_qty,
                            price=entry_price,
                            reason="entry_remote_fallback",
                        )
                        print(f"[PNL] entry recorded by remote bot={bot_id} {symbol} qty={filled_qty} px={entry_price}")
            except Exception as e:
                print("[ENTRY] remote fallback failed:", e)

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(filled_qty) if filled_qty else str(qty),
            "entry_price": str(entry_price) if entry_price else None,
            "order_status": status,
            "cancel_reason": cancel_reason,
            "order_id": order_id,
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

        direction_to_close = None
        qty_to_close = Decimal("0")

        if long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty

        # 如果本地没仓位，尝试远程查询兜底
        if not direction_to_close or qty_to_close <= 0:
            print(f"[EXIT] local 0 position, trying remote fallback for {symbol}...")
            remote = get_open_position_for_symbol(symbol)
            if remote and remote["size"] > 0:
                direction_to_close = remote["side"]
                qty_to_close = remote["size"]
                print(f"[EXIT] remote fallback found: {direction_to_close} {qty_to_close}")
            else:
                return jsonify({"status": "no_position"}), 200

        entry_side = "BUY" if direction_to_close == "LONG" else "SELL"
        exit_side = "SELL" if direction_to_close == "LONG" else "BUY"

        print(f"[EXIT] bot={bot_id} symbol={symbol} dir={direction_to_close} qty={qty_to_close} -> {exit_side}")

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
            return jsonify({"status": "order_error", "err": str(e)}), 200

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

        # ✅ 退出也优先用真实 avg fill
        exit_price = None
        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order.get("order_id"),
                client_order_id=order.get("client_order_id"),
                max_wait_sec=FILL_MAX_WAIT_SEC,
                poll_interval=FILL_POLL_INTERVAL,
            )
            exit_price = _safe_decimal(fill.get("avg_fill_price"))
        except Exception as e:
            print(f"[EXIT] fill-first failed bot={bot_id} symbol={symbol} err:", e)

        if not exit_price:
            try:
                rules = _get_symbol_rules(symbol)
                min_qty = rules["min_qty"]
                px = get_market_price(symbol, exit_side, str(min_qty))
                exit_price = _safe_decimal(px)
            except Exception:
                exit_price = None

        if exit_price:
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
                print("[PNL] record_exit_fifo error:", e)

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(qty_to_close),
            "exit_price": str(exit_price) if exit_price else None,
            "order_status": status,
            "cancel_reason": cancel_reason,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_started()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
