import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple

from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    get_market_price,
    _get_symbol_rules,
    _snap_quantity,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ------------------------------------------------------------------
# 硬性锁盈止损配置（只对部分 bot 生效）
# ------------------------------------------------------------------
# 哪些 bot 使用 0.25% -> 0.2% 的硬性锁盈止损
_HARD_SL_BOTS_ENV = os.getenv("HARD_SL_BOTS", "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5")
HARD_SL_BOTS = {
    b.strip()
    for b in _HARD_SL_BOTS_ENV.split(",")
    if b.strip()
}

# 触发挂“锁盈线”的浮盈百分比（0.25%）
HARD_SL_TRIGGER_PCT = Decimal(os.getenv("HARD_SL_TRIGGER_PCT", "0.25"))

# 锁盈线水平（0.2%）：达到 0.25% 后，一旦跌回这个水平就强平
HARD_SL_LEVEL_PCT = Decimal(os.getenv("HARD_SL_LEVEL_PCT", "0.20"))

# 后台轮询 Apex 价格的时间间隔（秒）
HARD_SL_POLL_INTERVAL = float(os.getenv("HARD_SL_POLL_INTERVAL", "2.0"))

# key: (bot_id, symbol) -> {
#   "side": "BUY"/"SELL",
#   "qty": Decimal,
#   "entry_price": Decimal | None,
#   "hard_sl_armed": bool,
#   "hard_sl_level": Decimal | None,
# }
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

# 后台线程启动标记
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
# 后台线程：监控浮盈并执行 0.25% -> 0.2% 锁盈止损
# ------------------------------------------------
def _monitor_positions_loop():
    """后台轮询 Apex 价格，对开启硬 SL 的 bot 执行 0.25% -> 0.2% 锁盈逻辑。"""
    global BOT_POSITIONS
    print("[HARD_SL] monitor thread started")

    while True:
        try:
            # 遍历当前所有 bot 的仓位
            for (bot_id, symbol), pos in list(BOT_POSITIONS.items()):
                # 只对配置好的 bot 生效
                if bot_id not in HARD_SL_BOTS:
                    continue

                side = pos.get("side")
                qty = pos.get("qty") or Decimal("0")
                entry_price = pos.get("entry_price")
                hard_sl_armed = bool(pos.get("hard_sl_armed", False))

                if not side or qty <= 0 or entry_price is None:
                    # 没有效仓位时，重置硬 SL 标记
                    pos["hard_sl_armed"] = False
                    pos["hard_sl_level"] = None
                    continue

                # 当前价格：用 Apex 的 get_worst_price_v3 做参考价
                try:
                    rules = _get_symbol_rules(symbol)
                    min_qty = rules["min_qty"]
                    # 用平仓方向来问 worst price（多单用 SELL，空单用 BUY）
                    exit_side = "SELL" if side == "BUY" else "BUY"
                    px_str = get_market_price(symbol, exit_side, str(min_qty))
                    current_price = Decimal(px_str)
                except Exception as e:
                    print(f"[HARD_SL] get_market_price error bot={bot_id} symbol={symbol}:", e)
                    continue

                # 计算浮盈百分比
                if side == "BUY":
                    pnl_pct = (current_price - entry_price) * Decimal("100") / entry_price
                    lock_price = entry_price * (Decimal("1") + HARD_SL_LEVEL_PCT / Decimal("100"))
                    # 一旦价格回落到锁盈价或更低就触发
                    lock_hit = current_price <= lock_price
                else:
                    # 如果以后你做空，这里也顺便兼容
                    pnl_pct = (entry_price - current_price) * Decimal("100") / entry_price
                    lock_price = entry_price * (Decimal("1") - HARD_SL_LEVEL_PCT / Decimal("100"))
                    lock_hit = current_price >= lock_price

                # 还没“武装”硬 SL，先看是否达到 0.25%
                if not hard_sl_armed and pnl_pct >= HARD_SL_TRIGGER_PCT:
                    pos["hard_sl_armed"] = True
                    pos["hard_sl_level"] = lock_price
                    print(
                        f"[HARD_SL] ARMED bot={bot_id} symbol={symbol} "
                        f"entry={entry_price} lock_price={lock_price} pnl={pnl_pct:.4f}%"
                    )
                    continue

                # 已经“武装”了硬 SL，检查是否跌回锁盈价
                if hard_sl_armed and lock_hit:
                    exit_side = "SELL" if side == "BUY" else "BUY"
                    size_str = str(qty)
                    print(
                        f"[HARD_SL] TRIGGER EXIT bot={bot_id} symbol={symbol} "
                        f"side={side} qty={size_str} pnl={pnl_pct:.4f}% "
                        f"lock_price={lock_price} current={current_price}"
                    )
                    try:
                        order = create_market_order(
                            symbol=symbol,
                            side=exit_side,
                            size=size_str,
                            reduce_only=True,
                            client_id=None,
                        )
                        status, cancel_reason = _order_status_and_reason(order)
                        print(
                            f"[HARD_SL] exit order status={status} "
                            f"cancelReason={cancel_reason!r}"
                        )

                        if status not in ("CANCELED", "REJECTED"):
                            # 认为平仓成功，清除本地仓位与硬 SL 状态
                            pos["qty"] = Decimal("0")
                            pos["entry_price"] = None
                            pos["hard_sl_armed"] = False
                            pos["hard_sl_level"] = None
                    except Exception as e:
                        print(f"[HARD_SL] create_market_order error bot={bot_id} symbol={symbol}:", e)

        except Exception as e:
            print("[HARD_SL] monitor loop top-level error:", e)

        time.sleep(HARD_SL_POLL_INTERVAL)


def _ensure_monitor_thread():
    """确保后台监控线程只启动一次。"""
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if not _MONITOR_THREAD_STARTED:
            t = threading.Thread(target=_monitor_positions_loop, daemon=True)
            t.start()
            _MONITOR_THREAD_STARTED = True
            print("[HARD_SL] monitor thread created")


@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()

    # ---------------------------
    # 1. 解析 JSON & 校验 secret
    # ---------------------------
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

    # ---------------------------
    # 2. 判定 entry / exit
    # ---------------------------
    mode: str | None = None

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
    # 3. ENTRY
    # ---------------------------
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

        if snapped_qty <= 0:
            return "snapped qty <= 0", 500

        size_str = str(snapped_qty)
        print(
            f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} "
            f"budget={budget} -> request_qty={size_str}"
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
        print(
            f"[ENTRY] order status={status} cancelReason={cancel_reason!r}"
        )

        # 如果一上来就被 CANCELED/REJECTED，说明根本没被接受
        if status in ("CANCELED", "REJECTED"):
            print(
                f"[ENTRY] bot={bot_id} symbol={symbol}: "
                f"order rejected, not recording position"
            )
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

        # 其它情况（PENDING / OPEN / FILLED）都认为订单已经被系统接受
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        # 从返回里拿真实成交参考价格（Apex worst price）
        computed = (order or {}).get("computed") or {}
        price_str = computed.get("price")
        try:
            entry_price_dec = Decimal(str(price_str)) if price_str is not None else None
        except Exception:
            entry_price_dec = None

        snapped_dec = snapped_qty

        if pos and pos.get("side") == side_raw and pos.get("qty", Decimal("0")) > 0 and entry_price_dec is not None and pos.get("entry_price") is not None:
            old_qty = pos["qty"]
            old_price = pos["entry_price"]
            new_qty = old_qty + snapped_dec
            # 加权平均新的 entry price
            new_entry_price = (old_price * old_qty + entry_price_dec * snapped_dec) / new_qty
            BOT_POSITIONS[key] = {
                "side": side_raw,
                "qty": new_qty,
                "entry_price": new_entry_price,
                # 加仓后，硬 SL 状态可以保留也可以重置，这里选择保留原状态：
                "hard_sl_armed": pos.get("hard_sl_armed", False),
                "hard_sl_level": pos.get("hard_sl_level"),
            }
        else:
            # 第一次开仓，或者之前是反方向，直接覆盖并重置硬 SL 状态
            BOT_POSITIONS[key] = {
                "side": side_raw,
                "qty": snapped_dec,
                "entry_price": entry_price_dec,
                "hard_sl_armed": False,
                "hard_sl_level": None,
            }

        print(f"[ENTRY] BOT_POSITIONS[{key}] = {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(BOT_POSITIONS[key]["qty"]),
            "entry_price": str(BOT_POSITIONS[key]["entry_price"]) if BOT_POSITIONS[key]["entry_price"] is not None else None,
            "requested_last": str(snapped_qty),
            "order_status": status,
            "cancel_reason": cancel_reason,
            "raw_order": order,
        }), 200

    # ---------------------------
    # 4. EXIT（策略出场）
    # ---------------------------
    if mode == "exit":
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if not pos or pos.get("qty", Decimal("0")) <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close")
            return jsonify({"status": "no_position"}), 200

        entry_side = pos["side"]
        qty = pos["qty"]
        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} "
            f"entry_side={entry_side} qty={qty} -> exit_side={exit_side}"
        )

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
        print(
            f"[EXIT] order status={status} cancelReason={cancel_reason!r}"
        )

        if status in ("CANCELED", "REJECTED"):
            # 平仓单被拒绝，不改本地仓位，下次还能再试
            print(
                f"[EXIT] bot={bot_id} symbol={symbol}: "
                f"exit order rejected, keep qty={qty}"
            )
            return jsonify({
                "status": "exit_rejected",
                "mode": "exit",
                "bot_id": bot_id,
                "symbol": symbol,
                "exit_side": exit_side,
                "requested_qty": str(qty),
                "order_status": status,
                "cancel_reason": cancel_reason,
                "raw_order": order,
            }), 200

        # 否则认为平仓成功，直接把这只 bot 的这份仓清零 + 清除硬 SL 状态
        BOT_POSITIONS[key] = {
            "side": entry_side,
            "qty": Decimal("0"),
            "entry_price": None,
            "hard_sl_armed": False,
            "hard_sl_level": None,
        }
        print(f"[EXIT] BOT_POSITIONS[{key}] -> {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(qty),
            "remaining_qty": "0",
            "order_status": status,
            "cancel_reason": cancel_reason,
            "raw_order": order,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
