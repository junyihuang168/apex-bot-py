import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set

from flask import Flask, request, jsonify, Response

# 引入工具库
from apex_client import (
    create_market_order,
    get_market_price,
    get_fill_summary,
    get_symbol_rules,
    snap_quantity,
)

# 引入数据库层
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

def _require_token() -> bool:
    if not DASHBOARD_TOKEN: return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN

def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip() for b in env_val.split(",") if b.strip()}

# 配置 BOT 列表
LONG_LADDER_BOTS = _parse_bot_list(os.getenv("LONG_LADDER_BOTS", ",".join([f"BOT_{i}" for i in range(1, 11)])))
SHORT_LADDER_BOTS = _parse_bot_list(os.getenv("SHORT_LADDER_BOTS", ",".join([f"BOT_{i}" for i in range(11, 21)])))

# ✅ 基础止损默认 0.5%
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))

# 风控轮询间隔 (秒)
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.5"))

# =========================================================================
# ✅ 内存镜像状态 (In-Memory State)
# 核心升级：为了避免每秒高频读数据库，我们在内存维护一份实时状态。
# Key: (bot_id, symbol, direction)
# Value: {"qty": Decimal, "entry_price": Decimal, "lock_level_pct": Decimal}
# =========================================================================
LOCAL_POSITIONS: Dict[Tuple[str, str, str], dict] = {}
_STATE_LOCK = threading.Lock()
_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ----------------------------
# 辅助函数
# ----------------------------
def _extract_budget_usdt(body: dict) -> Decimal:
    size_field = body.get("position_size_usdt") or body.get("size_usdt") or body.get("size")
    if size_field is None: raise ValueError("missing position_size_usdt / size_usdt / size")
    budget = Decimal(str(size_field))
    if budget <= 0: raise ValueError("size_usdt must be > 0")
    return budget

def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    rules = get_symbol_rules(symbol)
    min_qty = rules["min_qty"]
    ref_price_dec = Decimal(get_market_price(symbol, side, str(min_qty)))
    theoretical_qty = budget / ref_price_dec
    snapped_qty = snap_quantity(symbol, theoretical_qty)
    if snapped_qty <= 0: raise ValueError(f"snapped_qty <= 0, symbol={symbol}, budget={budget}")
    return snapped_qty

def _order_status_and_reason(order: dict):
    data = (order or {}).get("data", {}) or {}
    status = str(data.get("status", "")).upper()
    cancel_reason = str(data.get("cancelReason") or data.get("rejectReason") or data.get("errorMessage") or "")
    return status, cancel_reason

# ----------------------------
# 阶梯锁盈算法
# ----------------------------
TRIGGER_1 = Decimal("0.18")
LOCK_1 = Decimal("0.10")
TRIGGER_2 = Decimal("0.38")
LOCK_2 = Decimal("0.20")
STEP = Decimal("0.20")

def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    if pnl_pct < TRIGGER_1: return Decimal("0")
    if pnl_pct < TRIGGER_2: return LOCK_1
    n = (pnl_pct - TRIGGER_2) // STEP
    return LOCK_2 + STEP * n

def _bot_allows_direction(bot_id: str, direction: str) -> bool:
    d = direction.upper()
    if bot_id in LONG_LADDER_BOTS and d == "LONG": return True
    if bot_id in SHORT_LADDER_BOTS and d == "SHORT": return True
    return False

# =========================================================================
# ✅ 核心：虚拟监控循环 (基于内存 + 真实价格)
# =========================================================================
def _monitor_positions_loop():
    print("[RISK] Virtual Monitor Loop (Memory-Based) Started")
    
    while True:
        try:
            # 1. 快速获取内存快照
            with _STATE_LOCK:
                snapshot = list(LOCAL_POSITIONS.items())

            for (bot_id, symbol, direction), pos in snapshot:
                qty = pos.get("qty", Decimal("0"))
                entry_price = pos.get("entry_price", Decimal("0"))
                current_lock = pos.get("lock_level_pct", Decimal("0"))

                # 过滤无效仓位
                if qty <= 0 or entry_price <= 0:
                    continue
                
                if not _bot_allows_direction(bot_id, direction):
                    continue

                # 2. 获取真实可成交价格 (Real Executable Price)
                exit_side = "SELL" if direction == "LONG" else "BUY"
                rules = get_symbol_rules(symbol)
                
                try:
                    px_str = get_market_price(symbol, exit_side, str(rules["min_qty"]))
                    current_price = Decimal(px_str)
                except Exception:
                    # 网络波动时跳过本次检查，不报错刷屏
                    continue

                # 3. 计算 PnL %
                if direction == "LONG":
                    pnl_pct = (current_price - entry_price) * Decimal("100") / entry_price
                else:
                    pnl_pct = (entry_price - current_price) * Decimal("100") / entry_price

                # 4. 阶梯更新逻辑 (Local Ladder Logic)
                desired_lock = _desired_lock_level_pct(pnl_pct)
                if desired_lock > current_lock:
                    print(f"[RISK] 🚀 UPGRADE {bot_id} {symbol}: PnL {pnl_pct:.2f}% -> Lock {desired_lock}%")
                    
                    # 更新内存 + 数据库
                    with _STATE_LOCK:
                        if LOCAL_POSITIONS.get((bot_id, symbol, direction), {}).get("qty") == qty:
                             LOCAL_POSITIONS[(bot_id, symbol, direction)]["lock_level_pct"] = desired_lock
                    
                    set_lock_level_pct(bot_id, symbol, direction, desired_lock)
                    current_lock = desired_lock

                # 5. 检查退出条件
                should_exit = False
                exit_reason = ""

                # A. 基础硬止损 (Base SL)
                sl_threshold = BASE_SL_PCT / Decimal("100")
                if direction == "LONG":
                    if current_price <= entry_price * (Decimal("1") - sl_threshold):
                        should_exit = True
                        exit_reason = "base_sl"
                else:
                    if current_price >= entry_price * (Decimal("1") + sl_threshold):
                        should_exit = True
                        exit_reason = "base_sl"

                # B. 阶梯触发 (Ladder Lock)
                if not should_exit and current_lock > 0:
                    lock_threshold = current_lock / Decimal("100")
                    if direction == "LONG":
                        if current_price <= entry_price * (Decimal("1") + lock_threshold):
                            should_exit = True
                            exit_reason = f"ladder_lock_{current_lock}%"
                    else:
                        if current_price >= entry_price * (Decimal("1") - lock_threshold):
                            should_exit = True
                            exit_reason = f"ladder_lock_{current_lock}%"

                # 6. 执行退出
                if should_exit:
                    print(f"[RISK] 💥 TRIGGER EXIT {bot_id} {symbol} ({exit_reason}) PnL: {pnl_pct:.2f}%")
                    _execute_exit_logic(bot_id, symbol, direction, qty, exit_reason)

        except Exception as e:
            print("[RISK] Loop Error:", e)
        
        time.sleep(RISK_POLL_INTERVAL)

def _execute_exit_logic(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    """
    统一退出逻辑：下单 -> 记账 -> 清理内存
    """
    exit_side = "SELL" if direction == "LONG" else "BUY"
    entry_side = "BUY" if direction == "LONG" else "SELL"

    # 1. 下单
    try:
        order = create_market_order(symbol=symbol, side=exit_side, size=str(qty), reduce_only=True)
        status, cancel_reason = _order_status_and_reason(order)
        if status in ("CANCELED", "REJECTED"):
            print(f"[RISK] Exit Rejected: {cancel_reason}")
            return
    except Exception as e:
        print(f"[RISK] Order Creation Error: {e}")
        return

    # 2. 记账 (DB) - 尝试获取一次最新价格用于记录
    try:
        px_str = get_market_price(symbol, exit_side, "0.01")
        exit_price = Decimal(px_str)
        record_exit_fifo(bot_id, symbol, entry_side, qty, exit_price, reason)
    except Exception as e:
        print("[RISK] Record Exit Error:", e)

    # 3. 清理数据库锁盈状态
    clear_lock_level_pct(bot_id, symbol, direction)

    # 4. ✅ 清理内存状态
    with _STATE_LOCK:
        key = (bot_id, symbol, direction)
        if key in LOCAL_POSITIONS:
            curr_qty = LOCAL_POSITIONS[key]["qty"]
            new_qty = curr_qty - qty
            if new_qty <= Decimal("0.0000001"):
                del LOCAL_POSITIONS[key]
            else:
                LOCAL_POSITIONS[key]["qty"] = new_qty

def _init_local_cache():
    """启动时从数据库加载状态到内存"""
    print("[RISK] Hydrating local cache from DB...")
    with _STATE_LOCK:
        bots = list_bots_with_activity()
        for bot_id in bots:
            opens = get_bot_open_positions(bot_id)
            for (symbol, direction), v in opens.items():
                qty = v["qty"]
                wentry = v["weighted_entry"]
                if qty > 0:
                    lock = get_lock_level_pct(bot_id, symbol, direction)
                    LOCAL_POSITIONS[(bot_id, symbol, direction)] = {
                        "qty": qty,
                        "entry_price": wentry,
                        "lock_level_pct": lock
                    }
    print(f"[RISK] Cache hydrated. {len(LOCAL_POSITIONS)} active positions.")

def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            init_db()
            _init_local_cache() # 加载数据
            t = threading.Thread(target=_monitor_positions_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True
            print("[RISK] Thread created")

@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "Apex Bot Running", 200

# ----------------------------
# PnL API
# ----------------------------
@app.route("/api/pnl", methods=["GET"])
def api_pnl():
    if not _require_token(): return jsonify({"error": "forbidden"}), 403
    _ensure_monitor_thread()

    bots = list_bots_with_activity()
    only_bot = request.args.get("bot_id")
    if only_bot: bots = [b for b in bots if b == only_bot]

    out = []
    for bot_id in bots:
        base = get_bot_summary(bot_id)
        opens = get_bot_open_positions(bot_id)
        
        unrealized = Decimal("0")
        open_rows = []
        for (symbol, direction), v in opens.items():
            qty = v["qty"]
            wentry = v["weighted_entry"]
            if qty <= 0: continue
            
            # 仅用于报表展示，暂定为0，避免API卡顿
            px = Decimal("0")
            open_rows.append({
                "symbol": symbol,
                "direction": direction,
                "qty": str(qty),
                "weighted_entry": str(wentry),
                "mark_price": str(px),
            })
        
        base.update({"unrealized": str(unrealized), "open_positions": open_rows})
        out.append(base)
    
    # 按总盈亏排序
    def _rt(x):
        try: return Decimal(str(x.get("realized_total", "0")))
        except: return Decimal("0")
    out.sort(key=_rt, reverse=True)
    
    return jsonify({"ts": int(time.time()), "bots": out}), 200

# ----------------------------
# Dashboard (已恢复)
# ----------------------------
@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not _require_token(): return Response("Forbidden", status=403)
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
    <div class="muted">独立 lots 记账 · 真实成交价优先 · 基础止损默认 0.5% · 阶梯锁盈</div>
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

# ----------------------------
# Webhook (接收信号)
# ----------------------------
@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()
    try:
        body = request.get_json(force=True, silent=False)
    except Exception: return "invalid json", 400

    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET: return "forbidden", 403

    symbol = body.get("symbol")
    bot_id = str(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal = str(body.get("signal_type", "")).lower()
    action = str(body.get("action", "")).lower()
    tv_client_id = body.get("client_id")

    mode = None
    if signal in ("entry", "open") or action in ("open", "entry"): mode = "entry"
    elif signal.startswith("exit") or action in ("close", "exit"): mode = "exit"

    if not mode: return "unknown mode", 400

    # -------------------------
    # ENTRY
    # -------------------------
    if mode == "entry":
        try:
            budget = _extract_budget_usdt(body)
            qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            return str(e), 400

        # 1. 下单
        try:
            order = create_market_order(symbol=symbol, side=side_raw, size=str(qty), client_id=tv_client_id)
        except Exception:
            return "order error", 500
        
        status, reason = _order_status_and_reason(order)
        if status in ("CANCELED", "REJECTED"):
             return jsonify({"status": "rejected", "reason": reason}), 200

        # 2. 获取成交详情 (优先)
        computed = (order or {}).get("computed") or {}
        entry_price = None
        final_qty = qty
        
        try:
            fill = get_fill_summary(symbol=symbol, order_id=order.get("order_id"))
            entry_price = Decimal(str(fill["avg_fill_price"]))
            final_qty = Decimal(str(fill["filled_qty"]))
        except Exception:
            # Fallback
            if computed.get("price"): entry_price = Decimal(str(computed.get("price")))

        # 3. ✅ 更新内存 (让监控线程立马感知)
        if entry_price and final_qty > 0:
            direction = "LONG" if side_raw == "BUY" else "SHORT"
            with _STATE_LOCK:
                key = (bot_id, symbol, direction)
                old_pos = LOCAL_POSITIONS.get(key, {})
                old_qty = old_pos.get("qty", Decimal("0"))
                old_entry = old_pos.get("entry_price", Decimal("0"))
                
                new_total = old_qty + final_qty
                if new_total > 0:
                    new_avg = (old_qty * old_entry + final_qty * entry_price) / new_total
                else:
                    new_avg = entry_price
                
                LOCAL_POSITIONS[key] = {
                    "qty": new_total,
                    "entry_price": new_avg,
                    "lock_level_pct": old_pos.get("lock_level_pct", Decimal("0"))
                }

            # 4. 写入数据库
            record_entry(bot_id, symbol, side_raw, final_qty, entry_price, "strategy_entry")

        return jsonify({"status": "ok", "qty": str(final_qty)}), 200

    # -------------------------
    # EXIT (TV 信号平仓)
    # -------------------------
    elif mode == "exit":
        target_direction = None
        target_qty = Decimal("0")
        
        # 优先查内存
        with _STATE_LOCK:
            key_long = (bot_id, symbol, "LONG")
            key_short = (bot_id, symbol, "SHORT")
            if key_long in LOCAL_POSITIONS:
                target_direction = "LONG"
                target_qty = LOCAL_POSITIONS[key_long]["qty"]
            elif key_short in LOCAL_POSITIONS:
                target_direction = "SHORT"
                target_qty = LOCAL_POSITIONS[key_short]["qty"]

        if not target_direction:
             return jsonify({"status": "no_position_in_memory"}), 200

        # 执行退出
        _execute_exit_logic(bot_id, symbol, target_direction, target_qty, "strategy_exit_signal")

        return jsonify({"status": "closed", "qty": str(target_qty)}), 200

    return "unknown", 400

if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
