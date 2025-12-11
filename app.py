import os
import time
import json
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_market_price,
    get_fill_summary,              # ✅ 真实价格
    get_open_position_for_symbol,  # ✅ 远程仓位查询
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
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ------------------------------------------------------------
# 1. 配置区域：Bot 定义与阶梯参数
# ------------------------------------------------------------
def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip() for b in env_val.split(",") if b.strip()}

# 兼容两种 Bot 定义方式，取并集确保覆盖
_LONG_BOTS_ENV = os.getenv("LONG_HARD_SL_BOTS", os.getenv("LONG_LADDER_BOTS", "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5,BOT_6,BOT_7,BOT_8,BOT_9,BOT_10"))
_SHORT_BOTS_ENV = os.getenv("SHORT_HARD_SL_BOTS", os.getenv("SHORT_LADDER_BOTS", "BOT_11,BOT_12,BOT_13,BOT_14,BOT_15,BOT_16,BOT_17,BOT_18,BOT_19,BOT_20"))

LONG_HARD_SL_BOTS = _parse_bot_list(_LONG_BOTS_ENV)
SHORT_HARD_SL_BOTS = _parse_bot_list(_SHORT_BOTS_ENV)
ALL_HARD_SL_BOTS = LONG_HARD_SL_BOTS | SHORT_HARD_SL_BOTS

# ✅ 阶梯式锁盈参数 (Local Ladder Logic)
LADDER_START_TRIGGER_PCT = Decimal(os.getenv("HARD_SL_LADDER_START_TRIGGER_PCT", "0.18"))
LADDER_START_LOCK_PCT = Decimal(os.getenv("HARD_SL_LADDER_START_LOCK_PCT", "0.00"))
LADDER_STEP_PCT = Decimal(os.getenv("HARD_SL_LADDER_STEP_PCT", "0.20"))

HARD_SL_POLL_INTERVAL = float(os.getenv("HARD_SL_POLL_INTERVAL", "1.5"))
STATE_FILE = os.getenv("HARD_SL_STATE_FILE", "bot_state.json")
HARD_SL_SCOPE = os.getenv("HARD_SL_SCOPE", "bot_symbol").lower().strip() # "bot_symbol" or "symbol"

# ------------------------------------------------------------
# 2. 状态管理 (JSON Persistence for Ladder Logic)
# ------------------------------------------------------------
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}
_STATE_LOCK = threading.Lock()
_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

def _load_state():
    global BOT_POSITIONS
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        restored = {}
        for k, v in (raw or {}).items():
            if "|" not in k or not isinstance(v, dict): continue
            bot_id, symbol = k.split("|", 1)
            restored[(bot_id, symbol)] = {
                "side": v.get("side"),
                "qty": Decimal(str(v.get("qty", "0"))),
                "entry_price": Decimal(str(v["entry_price"])) if v.get("entry_price") else None,
                "ladder_n": int(v.get("ladder_n", -1)),
                "lock_price": Decimal(str(v["lock_price"])) if v.get("lock_price") else None,
                "last_update": int(v.get("last_update", 0)),
            }
        BOT_POSITIONS = restored
        print(f"[STATE] loaded {len(BOT_POSITIONS)} records from {STATE_FILE}")
    except Exception as e:
        print("[STATE] load error:", e)

def _save_state():
    try:
        raw = {}
        for (bot_id, symbol), pos in BOT_POSITIONS.items():
            raw[f"{bot_id}|{symbol}"] = {
                "side": pos.get("side"),
                "qty": str(pos.get("qty", Decimal("0"))),
                "entry_price": str(pos["entry_price"]) if pos.get("entry_price") else None,
                "ladder_n": int(pos.get("ladder_n", -1)),
                "lock_price": str(pos["lock_price"]) if pos.get("lock_price") else None,
                "last_update": int(pos.get("last_update", 0)),
            }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[STATE] save error:", e)

def _state_update(key: Tuple[str, str], updater):
    with _STATE_LOCK:
        pos = BOT_POSITIONS.get(key) or {}
        updater(pos)
        BOT_POSITIONS[key] = pos
        _save_state()

def _clear_other_bots_same_symbol(symbol: str, current_bot_id: str):
    sym_u = str(symbol).upper()
    to_clear = []
    with _STATE_LOCK:
        for (bid, sym) in list(BOT_POSITIONS.keys()):
            if str(sym).upper() != sym_u: continue
            if bid == current_bot_id: continue
            if bid in ALL_HARD_SL_BOTS:
                to_clear.append((bid, sym))
        for key in to_clear:
            p = BOT_POSITIONS.get(key) or {}
            p["qty"] = Decimal("0")
            p["entry_price"] = None
            p["ladder_n"] = -1
            p["lock_price"] = None
            p["last_update"] = int(time.time())
            BOT_POSITIONS[key] = p
        if to_clear:
            _save_state()
            print(f"[MIRROR] cleared stale states for symbol={sym_u}: {to_clear}")

# ------------------------------------------------------------
# 3. 辅助函数
# ------------------------------------------------------------
def _require_token() -> bool:
    if not DASHBOARD_TOKEN: return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN

def _extract_budget_usdt(body: dict) -> Decimal:
    size_field = (body.get("position_size_usdt") or body.get("size_usdt") or body.get("size"))
    if size_field is None: raise ValueError("missing position_size_usdt / size_usdt / size")
    budget = Decimal(str(size_field))
    if budget <= 0: raise ValueError("size_usdt must be > 0")
    return budget

def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]
    ref_price_dec = Decimal(get_market_price(symbol, side, str(min_qty)))
    theoretical_qty = budget / ref_price_dec
    snapped_qty = _snap_quantity(symbol, theoretical_qty)
    if snapped_qty <= 0: raise ValueError("snapped_qty <= 0")
    return snapped_qty

def _order_status_and_reason(order: dict):
    data = (order or {}).get("data", {}) or {}
    status = str(data.get("status", "")).upper()
    cancel_reason = str(data.get("cancelReason") or data.get("rejectReason") or data.get("errorMessage") or "")
    code = (order or {}).get("code")
    msg = (order or {}).get("msg")
    if code not in (None, 200, "200") and not cancel_reason:
        cancel_reason = str(msg or "")
        if not status: status = "ERROR"
    return status, cancel_reason

def _bot_allows_hard_sl(bot_id: str, side: str) -> bool:
    side_u = side.upper()
    if bot_id in LONG_HARD_SL_BOTS: return side_u == "BUY"
    if bot_id in SHORT_HARD_SL_BOTS: return side_u == "SELL"
    return False

def _calc_ladder_n(pnl_pct: Decimal) -> int:
    if pnl_pct < LADDER_START_TRIGGER_PCT: return -1
    try:
        n = int((pnl_pct - LADDER_START_TRIGGER_PCT) // LADDER_STEP_PCT)
    except Exception:
        n = 0
    return max(0, n)

def _lock_pct_for_n(n: int) -> Decimal:
    if n < 0: return Decimal("0")
    return LADDER_START_LOCK_PCT + LADDER_STEP_PCT * Decimal(n)

# ------------------------------------------------------------
# 4. 虚拟监控线程 (The Polling Loop) - 来自仓库逻辑
# ------------------------------------------------------------
def _monitor_positions_loop():
    print("[HARD_SL] ladder virtual monitor thread started")
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
                lock_price_state = pos.get("lock_price")

                if not _bot_allows_hard_sl(bot_id, side): continue

                # Remote fallback check
                remote = get_open_position_for_symbol(symbol)
                remote_qty = remote["size"] if remote else Decimal("0")
                remote_side = str(remote["side"]).upper() if remote else None
                remote_entry = remote.get("entryPrice") if remote else None

                # Patch missing local state from remote
                if entry_price is None and remote_entry and remote_qty > 0:
                    def _patch(p):
                        if not p.get("side"): p["side"] = "BUY" if remote_side == "LONG" else "SELL"
                        if _bot_allows_hard_sl(bot_id, p["side"]):
                            p["entry_price"] = remote_entry
                            if (p.get("qty") or Decimal("0")) <= 0: p["qty"] = remote_qty
                            if "ladder_n" not in p: p["ladder_n"] = -1
                            p["last_update"] = int(time.time())
                    _state_update((bot_id, symbol), _patch)
                    
                    pos = BOT_POSITIONS.get((bot_id, symbol), {})
                    side = str(pos.get("side") or "").upper()
                    qty = pos.get("qty") or Decimal("0")
                    entry_price = pos.get("entry_price")
                    ladder_n = int(pos.get("ladder_n", -1))
                    lock_price_state = pos.get("lock_price")

                if side not in ("BUY", "SELL") or qty <= 0 or entry_price is None:
                    continue

                # Get Current Price
                try:
                    rules = _get_symbol_rules(symbol)
                    min_qty = rules["min_qty"]
                    exit_side_for_quote = "SELL" if side == "BUY" else "BUY"
                    px_str = get_market_price(symbol, exit_side_for_quote, str(min_qty))
                    current_price = Decimal(px_str)
                except Exception as e:
                    print(f"[HARD_SL] get_market_price error bot={bot_id} symbol={symbol}:", e)
                    continue

                # Compute PnL %
                if side == "BUY":
                    pnl_pct = (current_price - entry_price) * Decimal("100") / entry_price
                else:
                    pnl_pct = (entry_price - current_price) * Decimal("100") / entry_price

                # Determine target ladder level
                target_n = _calc_ladder_n(pnl_pct)

                # Upgrade Level
                if target_n > ladder_n:
                    lock_pct = _lock_pct_for_n(target_n)
                    if side == "BUY":
                        new_lock_price = entry_price * (Decimal("1") + lock_pct / Decimal("100"))
                    else:
                        new_lock_price = entry_price * (Decimal("1") - lock_pct / Decimal("100"))

                    def _upgrade(p):
                        p["ladder_n"] = target_n
                        p["lock_price"] = new_lock_price
                        p["last_update"] = int(time.time())
                    _state_update((bot_id, symbol), _upgrade)

                    print(f"[HARD_SL] LADDER UP bot={bot_id} symbol={symbol} side={side} pnl={pnl_pct:.4f}% -> n={target_n} lock_price={new_lock_price}")
                    lock_price_state = new_lock_price
                    ladder_n = target_n

                if ladder_n < 0 or lock_price_state is None:
                    continue

                # Check lock hit
                lock_hit = (current_price <= lock_price_state) if side == "BUY" else (current_price >= lock_price_state)

                if lock_hit:
                    # Decide close qty
                    close_qty = qty
                    if remote_qty > 0:
                        close_qty = remote_qty if HARD_SL_SCOPE == "symbol" else min(qty, remote_qty)

                    if close_qty > 0:
                        exit_side = "SELL" if side == "BUY" else "BUY"
                        print(f"[HARD_SL] TRIGGER EXIT bot={bot_id} symbol={symbol} qty={close_qty} lock_price={lock_price_state} curr={current_price}")
                        
                        try:
                            order = create_market_order(symbol=symbol, side=exit_side, size=str(close_qty), reduce_only=True)
                            status, cancel_reason = _order_status_and_reason(order)
                            if status not in ("CANCELED", "REJECTED", "ERROR", "EXPIRED"):
                                # Clear state
                                def _clear(p):
                                    p["qty"] = Decimal("0")
                                    p["entry_price"] = None
                                    p["ladder_n"] = -1
                                    p["lock_price"] = None
                                    p["last_update"] = int(time.time())
                                _state_update((bot_id, symbol), _clear)
                                
                                # Record Exit PnL
                                try:
                                    record_exit_fifo(bot_id, symbol, side, close_qty, current_price, reason="ladder_lock_exit")
                                except Exception as e:
                                    print("[PNL] record_exit_fifo error:", e)
                                    
                        except Exception as e:
                            print(f"[HARD_SL] execution error:", e)

        except Exception as e:
            print("[HARD_SL] monitor top-level error:", e)
        
        time.sleep(HARD_SL_POLL_INTERVAL)

def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            init_db()
            _load_state()
            t = threading.Thread(target=_monitor_positions_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True
            print("[HARD_SL] ladder virtual monitor thread created")

# ------------------------------------------------------------
# 5. Web Routes & Dashboard
# ------------------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200

@app.route("/api/pnl", methods=["GET"])
def api_pnl():
    if not _require_token(): return jsonify({"error": "forbidden"}), 403
    _ensure_monitor_thread()
    bots = list_bots_with_activity()
    if request.args.get("bot_id"):
        bots = [b for b in bots if b == request.args.get("bot_id")]
    
    out = []
    for bot_id in bots:
        base = get_bot_summary(bot_id)
        # Use DB opens for PnL report
        opens = get_bot_open_positions(bot_id)
        unrealized = Decimal("0")
        open_rows = []
        for (symbol, direction), v in opens.items():
            qty = v["qty"]
            wentry = v["weighted_entry"]
            if qty <= 0: continue
            px_str = get_market_price(symbol, "SELL" if direction == "LONG" else "BUY", "0.01") # approx check
            px = Decimal(px_str)
            if direction == "LONG":
                unrealized += (px - wentry) * qty
            else:
                unrealized += (wentry - px) * qty
            open_rows.append({"symbol": symbol, "direction": direction, "qty": str(qty), "weighted_entry": str(wentry), "mark_price": str(px)})
        base.update({"unrealized": str(unrealized), "open_positions": open_rows})
        out.append(base)
    
    return jsonify({"ts": int(time.time()), "bots": out}), 200

@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not _require_token(): return Response("Forbidden", status=403)
    _ensure_monitor_thread()
    # Preserving Original Dashboard HTML structure
    html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8" />
<title>Bot PnL Dashboard</title>
<style>:root{--bg:#0b0f14;--card:#111826;--muted:#9aa4b2;--text:#eef2f7;--good:#22c55e;--bad:#ef4444;}
body{margin:0;font-family:system-ui;background:var(--bg);color:var(--text);}
.card{background:var(--card);border:1px solid #1c2a44;border-radius:16px;padding:14px;margin:10px;}
table{width:100%;border-collapse:collapse;} th,td{padding:8px;border-bottom:1px solid #1b2538;}
.good{color:var(--good);} .bad{color:var(--bad);}
</style></head><body><div style="padding:20px;"><h1>Dashboard</h1><div id="cards">Loading...</div></div>
<script>
async function refresh(){
  const res = await fetch("/api/pnl"+window.location.search);
  const data = await res.json();
  let h = "";
  for(let b of data.bots){
     h += `<div class="card"><h3>${b.bot_id}</h3>
     <p>Total Realized: <span class="${Number(b.realized_total)>0?'good':'bad'}">${b.realized_total}</span></p>
     <p>Unrealized: ${b.unrealized}</p>
     <table><tr><th>Sym</th><th>Dir</th><th>Qty</th><th>Entry</th></tr>
     ${b.open_positions.map(p=>`<tr><td>${p.symbol}</td><td>${p.direction}</td><td>${p.qty}</td><td>${p.weighted_entry}</td></tr>`).join('')}
     </table></div>`;
  }
  document.getElementById("cards").innerHTML = h;
}
refresh(); setInterval(refresh, 15000);
</script></body></html>"""
    return Response(html, mimetype="text/html")

@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        return "invalid json", 400
    
    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        return "forbidden", 403

    symbol = body.get("symbol")
    if not symbol: return "missing symbol", 400

    bot_id = str(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal_type_raw = str(body.get("signal_type", "")).lower()
    action_raw = str(body.get("action", "")).lower()
    tv_client_id = body.get("client_id")

    mode = None
    if signal_type_raw in ("entry", "open") or action_raw in ("open", "entry"): mode = "entry"
    elif signal_type_raw.startswith("exit") or action_raw in ("close", "exit"): mode = "exit"

    if mode == "entry":
        if side_raw not in ("BUY", "SELL"): return "invalid side", 400
        try:
            budget = _extract_budget_usdt(body)
            snapped_qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            return str(e), 400

        # Execute
        try:
            order = create_market_order(symbol=symbol, side=side_raw, size=str(snapped_qty), client_id=tv_client_id)
        except Exception as e:
            print("[ENTRY] order error:", e)
            return "order error", 500
        
        status, cancel_reason = _order_status_and_reason(order)
        if status in ("REJECTED", "ERROR"):
            return jsonify({"status": "failed", "reason": cancel_reason}), 200

        # ✅ 关键：获取真实价格 (Real Executable Price) 并写库
        order_id = order.get("order_id")
        entry_price_dec = None
        final_qty = snapped_qty

        try:
            fill = get_fill_summary(symbol=symbol, order_id=order_id)
            entry_price_dec = Decimal(str(fill["avg_fill_price"]))
            final_qty = Decimal(str(fill["filled_qty"]))
        except Exception as e:
            print("[ENTRY] fill fetch failed, using computed:", e)
            # Fallback
            comp = (order or {}).get("computed") or {}
            if comp.get("price"): entry_price_dec = Decimal(str(comp["price"]))

        # DB Record
        if entry_price_dec:
            record_entry(bot_id, symbol, side_raw, final_qty, entry_price_dec, reason="strategy_entry")

        # Update Ladder State
        def _set_entry(p):
            p["side"] = side_raw
            p["qty"] = final_qty
            p["entry_price"] = entry_price_dec
            p["ladder_n"] = -1
            p["lock_price"] = None
            p["last_update"] = int(time.time())
        _state_update((bot_id, symbol), _set_entry)
        _clear_other_bots_same_symbol(symbol, bot_id)

        return jsonify({"status": "ok", "mode": "entry", "qty": str(final_qty), "price": str(entry_price_dec)}), 200

    if mode == "exit":
        # Check local state first, then DB
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key) or {}
        local_qty = pos.get("qty", Decimal("0"))
        local_side = str(pos.get("side") or "").upper()
        
        # Determine exit side
        if local_qty > 0:
            exit_side = "SELL" if local_side == "BUY" else "BUY"
            close_qty = local_qty
        else:
            # Fallback to remote check if local state empty
            remote = get_open_position_for_symbol(symbol)
            if remote and remote["size"] > 0:
                close_qty = remote["size"]
                exit_side = map_position_side_to_exit_order_side(remote["side"])
            else:
                return jsonify({"status": "no_position"}), 200

        try:
            order = create_market_order(symbol=symbol, side=exit_side, size=str(close_qty), reduce_only=True, client_id=tv_client_id)
        except Exception as e:
            return "order error", 500

        status, cancel_reason = _order_status_and_reason(order)
        if status not in ("CANCELED", "REJECTED", "ERROR"):
            # Clear Ladder State
            def _clear(p):
                p["qty"] = Decimal("0")
                p["entry_price"] = None
                p["ladder_n"] = -1
                p["lock_price"] = None
                p["last_update"] = int(time.time())
            _state_update(key, _clear)

            # Record Exit PnL (Use worst price as proxy if lazy, or fetch fills again)
            try:
                px = get_market_price(symbol, "SELL" if exit_side=="BUY" else "BUY", "0.01")
                record_exit_fifo(bot_id, symbol, "BUY" if exit_side=="SELL" else "SELL", close_qty, Decimal(px), reason="strategy_exit")
            except Exception:
                pass

        return jsonify({"status": "ok", "mode": "exit", "qty": str(close_qty)}), 200

    return "unsupported mode", 400

if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
