import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional

from flask import Flask, request, jsonify

# 导入融合后的 helper
from apex_client import (
    create_market_order, get_market_price, get_fill_summary,
    _get_symbol_rules, _snap_quantity, get_open_position_for_symbol,
    map_position_side_to_exit_order_side
)
from pnl_store import (
    init_db, record_entry, record_exit_fifo,
    load_json_state, save_json_state
)

app = Flask(__name__)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# --- 配置区：BOT 分组 & 阶梯参数 ---
_LONG_BOTS = os.getenv("LONG_HARD_SL_BOTS", "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5,BOT_6,BOT_7,BOT_8,BOT_9,BOT_10")
_SHORT_BOTS = os.getenv("SHORT_HARD_SL_BOTS", "BOT_11,BOT_12,BOT_13,BOT_14,BOT_15,BOT_16,BOT_17,BOT_18,BOT_19,BOT_20")
LONG_HARD_SL_BOTS = {b.strip() for b in _LONG_BOTS.split(",") if b.strip()}
SHORT_HARD_SL_BOTS = {b.strip() for b in _SHORT_BOTS.split(",") if b.strip()}
ALL_HARD_SL_BOTS = LONG_HARD_SL_BOTS | SHORT_HARD_SL_BOTS

LADDER_START_TRIGGER_PCT = Decimal(os.getenv("HARD_SL_LADDER_START_TRIGGER_PCT", "0.18"))
LADDER_START_LOCK_PCT = Decimal(os.getenv("HARD_SL_LADDER_START_LOCK_PCT", "0.00"))
LADDER_STEP_PCT = Decimal(os.getenv("HARD_SL_LADDER_STEP_PCT", "0.20"))
HARD_SL_POLL_INTERVAL = float(os.getenv("HARD_SL_POLL_INTERVAL", "1.5"))
HARD_SL_SCOPE = os.getenv("HARD_SL_SCOPE", "bot_symbol").lower()

# 全局内存状态
BOT_POSITIONS = {} 
_STATE_LOCK = threading.Lock()
_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

def _update_state_safe(key, updater):
    with _STATE_LOCK:
        pos = BOT_POSITIONS.get(key) or {}
        updater(pos)
        BOT_POSITIONS[key] = pos
        save_json_state(BOT_POSITIONS)

def _bot_allows_hard_sl(bot_id: str, side: str) -> bool:
    side_u = side.upper()
    if bot_id in LONG_HARD_SL_BOTS: return side_u == "BUY"
    if bot_id in SHORT_HARD_SL_BOTS: return side_u == "SELL"
    return False

def _calc_ladder_n(pnl_pct: Decimal) -> int:
    if pnl_pct < LADDER_START_TRIGGER_PCT: return -1
    try:
        n = int((pnl_pct - LADDER_START_TRIGGER_PCT) // LADDER_STEP_PCT)
        return max(0, n)
    except: return 0

def _lock_pct_for_n(n: int) -> Decimal:
    if n < 0: return Decimal("0")
    return LADDER_START_LOCK_PCT + LADDER_STEP_PCT * Decimal(n)

# --- 核心：Hard SL 监控循环 (移植自第二个仓库) ---
def _monitor_positions_loop():
    print("[HARD_SL] monitor thread started")
    while True:
        try:
            with _STATE_LOCK:
                items = list(BOT_POSITIONS.items())
            
            for (bot_id, symbol), pos in items:
                if bot_id not in ALL_HARD_SL_BOTS: continue
                
                side = str(pos.get("side") or "").upper()
                qty = pos.get("qty") or Decimal("0")
                entry_price = pos.get("entry_price")
                ladder_n = int(pos.get("ladder_n", -1))
                lock_price = pos.get("lock_price")
                
                if not _bot_allows_hard_sl(bot_id, side): continue
                
                # 远程兜底检查 (Remote Check)
                remote = get_open_position_for_symbol(symbol)
                remote_qty = remote["size"] if remote else Decimal("0")
                
                # 如果本地没状态但远程有，进行 Patch (状态恢复)
                if entry_price is None and remote and remote_qty > 0:
                    def _patch(p):
                        remote_side = str(remote["side"]).upper()
                        # 只有方向匹配才认领
                        if (bot_id in LONG_HARD_SL_BOTS and remote_side=="LONG") or \
                           (bot_id in SHORT_HARD_SL_BOTS and remote_side=="SHORT"):
                            p["side"] = "BUY" if remote_side=="LONG" else "SELL"
                            p["entry_price"] = remote["entryPrice"]
                            p["qty"] = remote_qty
                            p["ladder_n"] = -1
                    _update_state_safe((bot_id, symbol), _patch)
                    # 重新读取
                    pos = BOT_POSITIONS.get((bot_id, symbol), {})
                    side = pos.get("side", "").upper()
                    entry_price = pos.get("entry_price")
                    qty = pos.get("qty", Decimal("0"))

                if qty <= 0 or entry_price is None: continue

                # 获取当前价格 (用于计算 PnL)
                try:
                    exit_side = "SELL" if side == "BUY" else "BUY"
                    current_price = Decimal(get_market_price(symbol, exit_side, "0.01")) # 用最小数量询价
                except: continue

                # 计算 PnL %
                pnl_pct = (current_price - entry_price)/entry_price * 100 if side == "BUY" else (entry_price - current_price)/entry_price * 100
                
                # 1. 阶梯升级逻辑 (Ladder Up)
                target_n = _calc_ladder_n(pnl_pct)
                if target_n > ladder_n:
                    lock_pct = _lock_pct_for_n(target_n)
                    if side == "BUY": new_lock = entry_price * (1 + lock_pct/100)
                    else: new_lock = entry_price * (1 - lock_pct/100)
                    
                    def _upgrade(p):
                        p["ladder_n"] = target_n
                        p["lock_price"] = new_lock
                    _update_state_safe((bot_id, symbol), _upgrade)
                    print(f"[HARD_SL] UP bot={bot_id} n={target_n} lock={new_lock}")
                    lock_price = new_lock # 更新本地变量用于下面判断

                # 2. 触发离场逻辑 (Trigger Exit)
                if lock_price is not None:
                    hit = (current_price <= lock_price) if side == "BUY" else (current_price >= lock_price)
                    if hit:
                        print(f"[HARD_SL] TRIGGER EXIT {bot_id} {symbol} pnl={pnl_pct}%")
                        close_qty = min(qty, remote_qty) if remote else qty # 安全起见不超过远程
                        
                        try:
                            create_market_order(symbol, exit_side, size=str(close_qty), reduce_only=True)
                            # 记录到 SQLite PnL
                            record_exit_fifo(bot_id, symbol, side, close_qty, current_price, "ladder_lock")
                            # 清理状态
                            def _clear(p):
                                p["qty"] = Decimal("0")
                                p["entry_price"] = None
                                p["ladder_n"] = -1
                                p["lock_price"] = None
                            _update_state_safe((bot_id, symbol), _clear)
                        except Exception as e:
                            print("[HARD_SL] Exit failed:", e)

        except Exception as e:
            print("[HARD_SL] loop error:", e)
        time.sleep(HARD_SL_POLL_INTERVAL)

def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    global BOT_POSITIONS
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            init_db()
            BOT_POSITIONS = load_json_state() # 加载历史状态
            t = threading.Thread(target=_monitor_positions_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True

# --- Webhook 接口 (逻辑合并) ---
@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()
    try:
        body = request.get_json(force=True)
    except: return "invalid json", 400

    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET: return "forbidden", 403
    
    symbol = body.get("symbol")
    bot_id = str(body.get("bot_id", "BOT_1"))
    side = str(body.get("side", "")).upper()
    action = str(body.get("action", "")).lower() # entry / exit

    if action == "entry":
        budget = Decimal(str(body.get("size_usdt", "0")))
        if budget <= 0: return "bad size", 400
        
        # 1. 计算数量 & 下单
        try:
            # 这里简化了 _compute_entry_qty 的调用，实际会调用 apex_client 里的逻辑
            # 为了代码简洁，直接调用 create_market_order，它内部会处理 budget -> qty
            res = create_market_order(symbol, side, size_usdt=budget)
            
            # 2. 尝试获取真实成交价 (Fill Summary)
            order_id = res["order_id"]
            cid = res["client_order_id"]
            computed_price = Decimal(res["computed"]["price"])
            
            fill_price = computed_price # 默认回退
            fill_qty = Decimal(res["computed"]["size"])
            
            try:
                # 尝试获取真实成交详情
                fill = get_fill_summary(symbol, order_id, cid)
                fill_price = fill["avg_fill_price"]
                fill_qty = fill["filled_qty"]
            except: pass

            # 3. 记录状态 (SQLite + JSON)
            record_entry(bot_id, symbol, side, fill_qty, fill_price)
            
            def _set_state(p):
                p["side"] = side
                p["qty"] = fill_qty
                p["entry_price"] = fill_price
                p["ladder_n"] = -1
                p["lock_price"] = None
            _update_state_safe((bot_id, symbol), _set_state)
            
            return jsonify({"status": "ok", "qty": str(fill_qty), "price": str(fill_price)})
        except Exception as e:
            print("[ENTRY] Error:", e)
            return "error", 500

    elif action == "exit":
        # 查找当前持仓
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key, {})
        qty = pos.get("qty", Decimal("0"))
        local_side = pos.get("side", "BUY")
        
        # 远程检查
        remote = get_open_position_for_symbol(symbol)
        remote_qty = remote["size"] if remote else Decimal("0")
        
        close_qty = max(qty, remote_qty) # 尽可能平干净
        if close_qty <= 0: return jsonify({"status": "no_position"})
        
        exit_side = "SELL" if local_side == "BUY" else "BUY"
        
        try:
            create_market_order(symbol, exit_side, size=str(close_qty), reduce_only=True)
            # 清理状态
            def _clear(p):
                p["qty"] = Decimal("0")
                p["entry_price"] = None
                p["ladder_n"] = -1
            _update_state_safe((bot_id, symbol), _clear)
            
            # 记录 SQLite (价格用当前市价估算)
            current_price = Decimal(get_market_price(symbol, exit_side, "0.01"))
            record_exit_fifo(bot_id, symbol, local_side, close_qty, current_price, "strategy_exit")
            
            return jsonify({"status": "ok", "closed": str(close_qty)})
        except Exception as e:
            return str(e), 500

    return "ok", 200

if __name__ == "__main__":
    _ensure_monitor_thread()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
