import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set
from flask import Flask, request, jsonify, Response

# 引入优化后的模块
from apex_client import create_market_order, get_market_price, get_fill_summary, get_symbol_rules, snap_quantity
from pnl_store import (
    init_db, record_entry, record_exit_fifo, list_bots_with_activity, 
    get_bot_summary, get_bot_open_positions, get_lock_level_pct, 
    set_lock_level_pct, clear_lock_level_pct
)

app = Flask(__name__)

# ================= 配置区 =================
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# 基础止损 (0.5%)
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))
# 监控轮询间隔 (秒)
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.5"))

# 解析 Bot 列表
def _parse_bots(env_key, default_range):
    raw = os.getenv(env_key, ",".join([f"BOT_{i}" for i in default_range]))
    return {b.strip() for b in raw.split(",") if b.strip()}

LONG_LADDER_BOTS = _parse_bots("LONG_LADDER_BOTS", range(1, 11))
SHORT_LADDER_BOTS = _parse_bots("SHORT_LADDER_BOTS", range(11, 21))

# ================= 内存镜像 (Memory Mirror) =================
# 优化：监控线程只读内存，避免 DB IO 瓶颈
# Key: (bot_id, symbol, direction)
LOCAL_POSITIONS: Dict[Tuple[str, str, str], dict] = {}
_STATE_LOCK = threading.Lock()
_MONITOR_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ================= 阶梯锁盈逻辑 (Local Ladder Logic) =================
TRIGGER_1, LOCK_1 = Decimal("0.18"), Decimal("0.10")
TRIGGER_2, LOCK_2 = Decimal("0.38"), Decimal("0.20")
STEP = Decimal("0.20")

def _calc_desired_lock(pnl_pct: Decimal) -> Decimal:
    if pnl_pct < TRIGGER_1: return Decimal("0")
    if pnl_pct < TRIGGER_2: return LOCK_1
    return LOCK_2 + STEP * ((pnl_pct - TRIGGER_2) // STEP)

# ================= 虚拟监控线程 (The Polling Loop) =================
def _monitor_loop():
    print("[RISK] Virtual Monitor Loop Started (Memory-Based)")
    while True:
        try:
            # 1. 快速复制内存快照
            with _STATE_LOCK:
                snapshot = list(LOCAL_POSITIONS.items())

            for (key, pos) in snapshot:
                bot_id, symbol, direction = key
                qty, entry_price = pos["qty"], pos["entry_price"]
                current_lock = pos["lock_level_pct"]

                if qty <= 0 or entry_price <= 0: continue
                
                # 检查该Bot是否启用风控
                if direction == "LONG" and bot_id not in LONG_LADDER_BOTS: continue
                if direction == "SHORT" and bot_id not in SHORT_LADDER_BOTS: continue

                # 2. 获取真实可成交价格 (Real Executable Price)
                exit_side = "SELL" if direction == "LONG" else "BUY"
                rules = get_symbol_rules(symbol)
                try:
                    # 使用 min_qty 询价，获取最差成交价
                    px_str = get_market_price(symbol, exit_side, str(rules["min_qty"]))
                    curr_price = Decimal(px_str)
                except Exception:
                    continue # 网络波动跳过

                # 3. 计算 PnL
                if direction == "LONG":
                    pnl_pct = (curr_price - entry_price) * 100 / entry_price
                else:
                    pnl_pct = (entry_price - curr_price) * 100 / entry_price

                # 4. 更新锁盈线
                new_lock = _calc_desired_lock(pnl_pct)
                if new_lock > current_lock:
                    print(f"[RISK] 🚀 UPGRADE {bot_id} {symbol}: {pnl_pct:.2f}% -> Lock {new_lock}%")
                    with _STATE_LOCK:
                        # 再次确认仓位未变
                        if LOCAL_POSITIONS.get(key, {}).get("qty") == qty:
                            LOCAL_POSITIONS[key]["lock_level_pct"] = new_lock
                    set_lock_level_pct(bot_id, symbol, direction, new_lock)
                    current_lock = new_lock

                # 5. 检查退出 (止损 或 锁盈触发)
                reason = None
                
                # A. 基础止损
                sl_ratio = BASE_SL_PCT / 100
                if direction == "LONG" and curr_price <= entry_price * (1 - sl_ratio):
                    reason = "base_sl"
                elif direction == "SHORT" and curr_price >= entry_price * (1 + sl_ratio):
                    reason = "base_sl"
                
                # B. 阶梯锁盈
                if not reason and current_lock > 0:
                    lock_ratio = current_lock / 100
                    if direction == "LONG" and curr_price <= entry_price * (1 + lock_ratio):
                        reason = f"ladder_lock_{current_lock}%"
                    elif direction == "SHORT" and curr_price >= entry_price * (1 - lock_ratio):
                        reason = f"ladder_lock_{current_lock}%"

                # 6. 执行退出
                if reason:
                    print(f"[RISK] 💥 TRIGGER EXIT {bot_id} {symbol} ({reason}) PnL:{pnl_pct:.2f}%")
                    _execute_exit(bot_id, symbol, direction, qty, reason)

        except Exception as e:
            print(f"[RISK] Loop Error: {e}")
        
        time.sleep(RISK_POLL_INTERVAL)

def _execute_exit(bot_id, symbol, direction, qty, reason):
    """统一退出逻辑：下单 -> DB记账 -> 清理内存"""
    exit_side = "SELL" if direction == "LONG" else "BUY"
    entry_side = "BUY" if direction == "LONG" else "SELL"

    # 1. 下单
    try:
        res = create_market_order(symbol, exit_side, size=str(qty), reduce_only=True)
        if res["raw_order"].get("retCode") != 0 and res["raw_order"].get("ret_code") != 0:
             # 简单检查，具体视API返回结构
             pass
    except Exception as e:
        print(f"[RISK] Order Fail: {e}")
        return

    # 2. 记账 (DB)
    try:
        # 获取一次价格用于记账
        px = Decimal(get_market_price(symbol, exit_side, "0.01"))
        record_exit_fifo(bot_id, symbol, entry_side, qty, px, reason)
    except Exception:
        pass

    # 3. 清理状态
    clear_lock_level_pct(bot_id, symbol, direction)
    with _STATE_LOCK:
        key = (bot_id, symbol, direction)
        if key in LOCAL_POSITIONS:
            # 简单处理：全平
            del LOCAL_POSITIONS[key]

# ================= Web / Dashboard =================
@app.route("/dashboard")
def dashboard():
    _ensure_thread()
    # 完整的 Dashboard HTML
    return """
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Apex Bot Dashboard</title>
<style>
:root{--bg:#0b0f14;--card:#111826;--text:#eef2f7;--good:#22c55e;--bad:#ef4444;}
body{margin:0;font-family:sans-serif;background:var(--bg);color:var(--text);padding:20px;}
.card{background:var(--card);padding:15px;margin-bottom:10px;border-radius:10px;border:1px solid #1c2a44;}
.row{display:flex;justify-content:space-between;align-items:center;}
.num{font-family:monospace;font-weight:bold;}
.good{color:var(--good);} .bad{color:var(--bad);}
h1{font-size:18px;margin:0 0 20px 0;}
table{width:100%;border-collapse:collapse;margin-top:10px;font-size:12px;}
th,td{text-align:left;padding:5px;border-bottom:1px solid #333;}
th{color:#888;}
</style>
</head>
<body>
<h1>Apex Bot Dashboard (Memory Monitor)</h1>
<div id="app">Loading...</div>
<script>
async function load(){
  try{
    let res = await fetch('/api/pnl');
    let data = await res.json();
    let html = '';
    data.bots.forEach(b => {
      html += `<div class="card">
        <div class="row">
          <strong>${b.bot_id}</strong>
          <span>Day: <span class="${b.realized_day>0?'good':'bad'}">${b.realized_day}</span></span>
        </div>
        <table><thead><tr><th>Symbol</th><th>Dir</th><th>Qty</th><th>Entry</th></tr></thead><tbody>`;
      b.open_positions.forEach(p => {
        html += `<tr><td>${p.symbol}</td><td>${p.direction}</td><td>${p.qty}</td><td>${p.weighted_entry}</td></tr>`;
      });
      html += `</tbody></table></div>`;
    });
    document.getElementById('app').innerHTML = html;
  }catch(e){console.error(e);}
}
setInterval(load, 2000);
load();
</script>
</body>
</html>
"""

@app.route("/api/pnl")
def api_pnl():
    _ensure_thread()
    bots = list_bots_with_activity()
    out = []
    for bid in bots:
        summ = get_bot_summary(bid)
        opens = get_bot_open_positions(bid)
        rows = []
        for (sym, direct), v in opens.items():
            rows.append({
                "symbol": sym, "direction": direct, 
                "qty": str(v["qty"]), "weighted_entry": str(v["weighted_entry"])
            })
        summ["open_positions"] = rows
        out.append(summ)
    return jsonify({"bots": out})

@app.route("/webhook", methods=["POST"])
def webhook():
    _ensure_thread()
    try:
        data = request.get_json(force=True)
    except: return "invalid json", 400
    
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET: return "forbidden", 403

    bot_id = str(data.get("bot_id", "BOT_1"))
    symbol = data.get("symbol")
    side = str(data.get("side", "")).upper()
    action = str(data.get("action", "")).lower()
    
    # 简单的 signal 解析
    mode = "entry" if action in ["entry", "open"] else "exit"

    if mode == "entry":
        budget = Decimal(str(data.get("size_usdt") or data.get("position_size_usdt") or 0))
        if budget <= 0: return "invalid size", 400
        
        try:
            qty = create_market_order(symbol, side, size_usdt=budget)["computed"]["size"]
            qty = Decimal(qty)
        except Exception as e: return f"order error: {e}", 500

        # 获取成交详情 (回填)
        try:
            fill = get_fill_summary(symbol, "") # 需要优化：传入 order_id
            price = fill["avg_fill_price"]
        except:
            price = Decimal(get_market_price(symbol, side, "0.01"))

        # ✅ 关键：同时更新内存 + DB
        direction = "LONG" if side == "BUY" else "SHORT"
        with _STATE_LOCK:
            key = (bot_id, symbol, direction)
            old = LOCAL_POSITIONS.get(key, {})
            new_q = old.get("qty", Decimal(0)) + qty
            # 简单加权平均逻辑 (可优化)
            if new_q > 0:
                new_p = (old.get("qty", Decimal(0)) * old.get("entry_price", Decimal(0)) + qty * price) / new_q
            else: new_p = price
            
            LOCAL_POSITIONS[key] = {
                "qty": new_q, "entry_price": new_p, 
                "lock_level_pct": old.get("lock_level_pct", Decimal(0))
            }
        
        record_entry(bot_id, symbol, side, qty, price, "tv_entry")
        return jsonify({"status": "ok", "qty": str(qty)})

    elif mode == "exit":
        # 优先读内存平仓
        key_long = (bot_id, symbol, "LONG")
        key_short = (bot_id, symbol, "SHORT")
        
        target = None
        with _STATE_LOCK:
            if key_long in LOCAL_POSITIONS: target = (key_long, LOCAL_POSITIONS[key_long])
            elif key_short in LOCAL_POSITIONS: target = (key_short, LOCAL_POSITIONS[key_short])
        
        if target:
            (key, pos) = target
            _execute_exit(bot_id, symbol, key[2], pos["qty"], "tv_exit")
            return jsonify({"status": "closed"})
        else:
            return "no position found", 200

    return "ok", 200

def _init_cache():
    """启动时从DB加载到内存"""
    print("[RISK] Hydrating Cache...")
    with _STATE_LOCK:
        bots = list_bots_with_activity()
        for bid in bots:
            opens = get_bot_open_positions(bid)
            for (sym, direct), v in opens.items():
                lock = get_lock_level_pct(bid, sym, direct)
                LOCAL_POSITIONS[(bid, sym, direct)] = {
                    "qty": v["qty"], "entry_price": v["weighted_entry"], 
                    "lock_level_pct": lock
                }
    print(f"[RISK] Cache Ready. {len(LOCAL_POSITIONS)} positions.")

def _ensure_thread():
    global _MONITOR_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_STARTED:
            init_db()
            _init_cache()
            threading.Thread(target=_monitor_loop, daemon=True).start()
            _MONITOR_STARTED = True

if __name__ == "__main__":
    _ensure_thread()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
