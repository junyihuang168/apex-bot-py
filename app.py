import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Any

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
# Position state (lightweight)
# 只作为你策略/风控运行时的缓存
# =========================
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

# 监控线程（你后续移动止盈引擎可以继续用）
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
# 监控线程占位（可继续扩展）
# ------------------------------------------------
def _monitor_positions_loop():
    print("[MONITOR] thread started (PnL/Dashboard build)")
    while True:
        time.sleep(2.0)


def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            init_db()  # ✅ 初始化 PnL DB
            t = threading.Thread(target=_monitor_positions_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True
            print("[MONITOR] thread created")


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

    # 允许你手动指定 bot 过滤
    only_bot = request.args.get("bot_id")
    if only_bot:
        bots = [b for b in bots if b == only_bot]

    out = []
    for bot_id in bots:
        base = get_bot_summary(bot_id)
        opens = get_bot_open_positions(bot_id)

        # 计算未实现（按当前 worst 参考价）
        unrealized = Decimal("0")
        open_rows = []

        for (symbol, direction), v in opens.items():
            qty = v["qty"]
            wentry = v["weighted_entry"]

            if qty <= 0:
                continue

            # 用 exit 方向 worst price 估当前价
            rules = _get_symbol_rules(symbol)
            min_qty = rules["min_qty"]

            if direction == "LONG":
                px = Decimal(get_market_price(symbol, "SELL", str(min_qty)))
                unrealized += (px - wentry) * qty
            else:
                px = Decimal(get_market_price(symbol, "BUY", str(min_qty)))
                unrealized += (wentry - px) * qty

            open_rows.append({
                "symbol": symbol,
                "direction": direction,
                "qty": str(qty),
                "weighted_entry": str(wentry),
                "mark_price": str(px),
            })

        base.update({
            "unrealized": str(unrealized),
            "open_positions": open_rows,
        })
        out.append(base)

    # 简单排序：按 realized_total
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
# ✅ Dashboard 页面（手机/电脑自适应）
# =========================
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not _require_token():
        return Response("Forbidden", status=403)

    _ensure_monitor_thread()

    # 轻量 HTML（不依赖外部 CDN，避免被拦）
    # 你可以后续美化成 React
    html = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bot PnL Dashboard</title>
  <style>
    :root { --bg:#0b0f14; --card:#111826; --muted:#9aa4b2; --text:#eef2f7; --accent:#4f8cff; --good:#22c55e; --bad:#ef4444; }
    body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Noto Sans CJK SC"; background:var(--bg); color:var(--text); }
    header { padding:16px 20px; border-bottom:1px solid #1d2636; display:flex; gap:12px; align-items:center; justify-content:space-between; }
    h1 { font-size:18px; margin:0; letter-spacing:.3px; }
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
    <div class="muted">按 bot 独立 lots 记账 · 手机/电脑通用</div>
  </div>
  <div class="muted" id="lastUpdate">Loading...</div>
</header>

<div class="wrap">
  <div class="controls">
    <button onclick="refresh()">刷新 Refresh</button>
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
          <span class="pill">BOT 1-10: 多头移动止盈组</span>
          <span class="pill">BOT 11-20: 空头移动止盈组</span>
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
          <span class="pill">${bot.startsWith("BOT_") ? "active" : "custom"}</span>
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

      <div class="small" style="margin-top:8px;">Open positions</div>
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
    // 继承 token
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
                "raw_order": order,
            }), 200

        # 从回单 computed 里抓 entry 参考价
        computed = (order or {}).get("computed") or {}
        price_str = computed.get("price")

        entry_price_dec: Optional[Decimal] = None
        try:
            if price_str is not None:
                entry_price_dec = Decimal(str(price_str))
        except Exception:
            entry_price_dec = None

        # 更新本地 position cache
        key = (bot_id, symbol)
        BOT_POSITIONS[key] = {
            "side": side_raw,
            "qty": snapped_qty,
            "entry_price": entry_price_dec,
        }

        # ✅ PnL 记账：ENTRY
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
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if not pos or (pos.get("qty") or Decimal("0")) <= 0:
            # 即使本地没有，也允许你之后用“远端兜底版本”再增强
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no local position")
            return jsonify({"status": "no_position"}), 200

        entry_side = str(pos.get("side") or "BUY").upper()
        qty = Decimal(str(pos.get("qty") or "0"))
        entry_price = pos.get("entry_price")

        if qty <= 0:
            return jsonify({"status": "no_position"}), 200

        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(f"[EXIT] bot={bot_id} symbol={symbol} entry_side={entry_side} qty={qty} -> exit_side={exit_side}")

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty),
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
                "requested_qty": str(qty),
                "order_status": status,
                "cancel_reason": cancel_reason,
            }), 200

        # 从回单 computed 里抓 exit 参考价
        computed = (order or {}).get("computed") or {}
        px_str = computed.get("price")

        exit_price_dec: Optional[Decimal] = None
        try:
            if px_str is not None:
                exit_price_dec = Decimal(str(px_str))
        except Exception:
            exit_price_dec = None

        # ✅ PnL 记账：EXIT (FIFO)
        if exit_price_dec is not None:
            try:
                record_exit_fifo(
                    bot_id=bot_id,
                    symbol=symbol,
                    entry_side=entry_side,
                    exit_qty=qty,
                    exit_price=exit_price_dec,
                    reason="strategy_exit",
                )
            except Exception as e:
                print("[PNL] record_exit_fifo error:", e)

        # 清空本地 cache
        BOT_POSITIONS[key] = {
            "side": entry_side,
            "qty": Decimal("0"),
            "entry_price": None,
        }

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(qty),
            "order_status": status,
            "cancel_reason": cancel_reason,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
