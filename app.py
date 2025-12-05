import os
import time
import json
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional

from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    get_market_price,
    _get_symbol_rules,
    _snap_quantity,
    get_open_position_for_symbol,
    map_position_to_order_side,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# -----------------------------
# 只对 BOT_1-10 生效
# -----------------------------
_HARD_SL_BOTS_ENV = os.getenv(
    "HARD_SL_BOTS",
    "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5,BOT_6,BOT_7,BOT_8,BOT_9,BOT_10",
)
HARD_SL_BOTS = {b.strip() for b in _HARD_SL_BOTS_ENV.split(",") if b.strip()}

# 0.25% -> 武装锁盈
HARD_SL_TRIGGER_PCT = Decimal(os.getenv("HARD_SL_TRIGGER_PCT", "0.25"))
# 0.20% -> 锁盈线
HARD_SL_LEVEL_PCT = Decimal(os.getenv("HARD_SL_LEVEL_PCT", "0.20"))

# 轮询
HARD_SL_POLL_INTERVAL = float(os.getenv("HARD_SL_POLL_INTERVAL", "1.5"))

# 状态文件
STATE_FILE = os.getenv("HARD_SL_STATE_FILE", "bot_state.json")

# 风控作用范围：
# - "bot_symbol": 只按本地 (bot_id, symbol) 追踪
# - "symbol":     以 symbol 真实仓位为准（更稳，但同标的多 bot 会合并影响）
HARD_SL_SCOPE = os.getenv("HARD_SL_SCOPE", "bot_symbol").lower().strip()
if HARD_SL_SCOPE not in ("bot_symbol", "symbol"):
    HARD_SL_SCOPE = "bot_symbol"

# key: (bot_id, symbol) -> {
#   "side": "BUY"/"SELL" (本地视角)
#   "qty": Decimal
#   "entry_price": Decimal | None
#   "armed": bool
#   "lock_price": Decimal | None
#   "last_update": int
# }
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_STATE_LOCK = threading.Lock()
_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()


# ------------------------------------------------
# 状态持久化
# ------------------------------------------------
def _load_state():
    global BOT_POSITIONS
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        restored = {}
        for k, v in (raw or {}).items():
            # k 形如 "BOT_1|ZEC-USDT"
            if "|" not in k or not isinstance(v, dict):
                continue
            bot_id, symbol = k.split("|", 1)
            restored[(bot_id, symbol)] = {
                "side": v.get("side"),
                "qty": Decimal(str(v.get("qty", "0"))),
                "entry_price": Decimal(str(v["entry_price"])) if v.get("entry_price") else None,
                "armed": bool(v.get("armed", False)),
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
                "armed": bool(pos.get("armed", False)),
                "lock_price": str(pos["lock_price"]) if pos.get("lock_price") else None,
                "last_update": int(pos.get("last_update", 0)),
            }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[STATE] save error:", e)


def _state_set(key: Tuple[str, str], new_pos: dict):
    with _STATE_LOCK:
        BOT_POSITIONS[key] = new_pos
        _save_state()


def _state_update(key: Tuple[str, str], updater):
    with _STATE_LOCK:
        pos = BOT_POSITIONS.get(key) or {}
        updater(pos)
        BOT_POSITIONS[key] = pos
        _save_state()


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
# 根据 USDT 预算算 qty
# ------------------------------------------------
def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    ref_price_dec = Decimal(get_market_price(symbol, side, str(min_qty)))
    theoretical_qty = budget / ref_price_dec
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    if snapped_qty <= 0:
        raise ValueError("snapped_qty <= 0")
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
    code = (order or {}).get("code")
    msg = (order or {}).get("msg")
    if code not in (None, 200, "200") and not cancel_reason:
        cancel_reason = str(msg or "")
        if not status:
            status = "ERROR"
    return status, cancel_reason


# ------------------------------------------------
# ✅ 虚拟锁盈监控线程
# ------------------------------------------------
def _monitor_positions_loop():
    print("[HARD_SL] virtual monitor thread started")

    while True:
        try:
            with _STATE_LOCK:
                items = list(BOT_POSITIONS.items())

            for (bot_id, symbol), pos in items:
                # 只对指定 bots
                if bot_id not in HARD_SL_BOTS:
                    continue

                side = pos.get("side")
                qty = pos.get("qty") or Decimal("0")
                entry_price = pos.get("entry_price")
                armed = bool(pos.get("armed", False))

                if not side or qty <= 0 or entry_price is None:
                    continue

                # 当前价格（用平仓方向 worst price）
                try:
                    rules = _get_symbol_rules(symbol)
                    min_qty = rules["min_qty"]
                    exit_side = "SELL" if side == "BUY" else "BUY"
                    px_str = get_market_price(symbol, exit_side, str(min_qty))
                    current_price = Decimal(px_str)
                except Exception as e:
                    print(f"[HARD_SL] get_market_price error bot={bot_id} symbol={symbol}:", e)
                    continue

                # 计算 pnl 和锁盈价
                if side == "BUY":
                    pnl_pct = (current_price - entry_price) * Decimal("100") / entry_price
                    lock_price = entry_price * (Decimal("1") + HARD_SL_LEVEL_PCT / Decimal("100"))
                    lock_hit = current_price <= lock_price
                else:
                    pnl_pct = (entry_price - current_price) * Decimal("100") / entry_price
                    lock_price = entry_price * (Decimal("1") - HARD_SL_LEVEL_PCT / Decimal("100"))
                    lock_hit = current_price >= lock_price

                # 1) 未武装 -> 达 0.25% 武装
                if not armed and pnl_pct >= HARD_SL_TRIGGER_PCT:
                    def _arm(p):
                        p["armed"] = True
                        p["lock_price"] = lock_price
                        p["last_update"] = int(time.time())

                    _state_update((bot_id, symbol), _arm)
                    print(
                        f"[HARD_SL] ARMED bot={bot_id} symbol={symbol} "
                        f"entry={entry_price} lock={lock_price} pnl={pnl_pct:.4f}%"
                    )
                    continue

                # 2) 已武装 -> 触发锁盈平仓
                if armed and lock_hit:
                    # 若你希望更稳，可以用 symbol-level 真实仓位兜底
                    remote = get_open_position_for_symbol(symbol)
                    remote_qty = remote["size"] if remote else None

                    close_qty = qty
                    if HARD_SL_SCOPE == "symbol" and remote_qty and remote_qty > 0:
                        # symbol 级风控：以真实仓位为准
                        close_qty = remote_qty
                    elif remote_qty and remote_qty > 0:
                        # bot_symbol 模式：本地为主，但不超过真实仓位
                        close_qty = min(qty, remote_qty)

                    if close_qty <= 0:
                        continue

                    exit_side = "SELL" if side == "BUY" else "BUY"
                    print(
                        f"[HARD_SL] TRIGGER EXIT bot={bot_id} symbol={symbol} "
                        f"exit_side={exit_side} qty={close_qty} "
                        f"lock={lock_price} current={current_price} pnl={pnl_pct:.4f}%"
                    )

                    try:
                        order = create_market_order(
                            symbol=symbol,
                            side=exit_side,
                            size=str(close_qty),
                            reduce_only=True,
                            client_id=None,
                        )
                        status, cancel_reason = _order_status_and_reason(order)
                        print(f"[HARD_SL] exit order status={status} cancelReason={cancel_reason!r}")

                        if status not in ("CANCELED", "REJECTED", "ERROR", "EXPIRED"):
                            # 清本地
                            def _clear(p):
                                p["qty"] = Decimal("0")
                                p["entry_price"] = None
                                p["armed"] = False
                                p["lock_price"] = None
                                p["last_update"] = int(time.time())

                            _state_update((bot_id, symbol), _clear)

                    except Exception as e:
                        print(f"[HARD_SL] create_market_order error bot={bot_id} symbol={symbol}:", e)

        except Exception as e:
            print("[HARD_SL] monitor top-level error:", e)

        time.sleep(HARD_SL_POLL_INTERVAL)


def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            _load_state()
            t = threading.Thread(target=_monitor_positions_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True
            print("[HARD_SL] virtual monitor thread created")


@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200


# ------------------------------------------------
# Webhook
# ------------------------------------------------
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

    # 判定 entry / exit
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

    # ---------------------------
    # ENTRY
    # ---------------------------
    if mode == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        try:
            budget = _extract_budget_usdt(body)
            snapped_qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            print("[ENTRY] prepare error:", e)
            return "entry prepare error", 500

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

        # ✅ 终局版策略：
        # 不再用“过严 IOC 判断”阻断本地记录
        # 只要不是明确 REJECTED/ERROR，就先记录本地仓位用于风控
        if status in ("REJECTED", "ERROR"):
            return jsonify({
                "status": "order_failed",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "request_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "raw_order": order,
            }), 200

        computed = (order or {}).get("computed") or {}
        price_str = computed.get("price")

        try:
            entry_price_dec = Decimal(str(price_str)) if price_str is not None else None
        except Exception:
            entry_price_dec = None

        key = (bot_id, symbol)
        now = int(time.time())

        def _set_entry(p):
            p["side"] = side_raw
            p["qty"] = Decimal(size_str)
            p["entry_price"] = entry_price_dec
            p["armed"] = False
            p["lock_price"] = None
            p["last_update"] = now

        _state_update(key, _set_entry)

        print(f"[ENTRY] BOT_POSITIONS[{key}] = {BOT_POSITIONS.get(key)}")

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
            "raw_order": order,
        }), 200

    # ---------------------------
    # EXIT
    # ---------------------------
    if mode == "exit":
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        # ✅ 终局版修复点：
        # 本地没仓 -> 读取 Apex 真实仓位 -> 有就照样平
        local_qty = (pos or {}).get("qty", Decimal("0")) if isinstance(pos, dict) else Decimal("0")
        local_side = (pos or {}).get("side") if isinstance(pos, dict) else None

        remote = get_open_position_for_symbol(symbol)
        if remote:
            remote_qty = remote["size"]
            remote_side = remote["side"]  # LONG/SHORT
        else:
            remote_qty = Decimal("0")
            remote_side = None

        if local_qty <= 0 and remote_qty <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position local+remote")
            return jsonify({"status": "no_position"}), 200

        # 决定平仓方向/数量
        if remote_qty > 0 and remote_side:
            exit_side = map_position_to_order_side(remote_side)
            close_qty = remote_qty if HARD_SL_SCOPE == "symbol" else max(remote_qty, local_qty)
        else:
            # 只能靠本地
            entry_side = local_side or "BUY"
            exit_side = "SELL" if entry_side == "BUY" else "BUY"
            close_qty = local_qty

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} "
            f"local_qty={local_qty} remote_qty={remote_qty} "
            f"-> exit_side={exit_side} close_qty={close_qty}"
        )

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(close_qty),
                reduce_only=True,
                client_id=tv_client_id,
            )
        except Exception as e:
            print("[EXIT] create_market_order error:", e)
            return "order error", 500

        status, cancel_reason = _order_status_and_reason(order)
        print(f"[EXIT] order status={status} cancelReason={cancel_reason!r}")

        if status not in ("CANCELED", "REJECTED", "ERROR", "EXPIRED"):
            # 清本地
            now = int(time.time())

            def _clear(p):
                p["qty"] = Decimal("0")
                p["entry_price"] = None
                p["armed"] = False
                p["lock_price"] = None
                p["last_update"] = now

            _state_update(key, _clear)

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(close_qty),
            "order_status": status,
            "cancel_reason": cancel_reason,
            "raw_order": order,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
