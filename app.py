import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple

from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    create_stop_market_order,
    cancel_order,
    get_market_price,
    _get_symbol_rules,
    _snap_quantity,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# -----------------------------
# HARD SL bots only BOT_1-10
# -----------------------------
_HARD_SL_BOTS_ENV = os.getenv(
    "HARD_SL_BOTS",
    "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5,BOT_6,BOT_7,BOT_8,BOT_9,BOT_10",
)
HARD_SL_BOTS = {b.strip() for b in _HARD_SL_BOTS_ENV.split(",") if b.strip()}

HARD_SL_TRIGGER_PCT = Decimal(os.getenv("HARD_SL_TRIGGER_PCT", "0.25"))
HARD_SL_LEVEL_PCT = Decimal(os.getenv("HARD_SL_LEVEL_PCT", "0.20"))
HARD_SL_POLL_INTERVAL = float(os.getenv("HARD_SL_POLL_INTERVAL", "2.0"))

BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()


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
    if not isinstance(order, dict):
        return "", ""

    data = order.get("data") or {}
    status = str(data.get("status", "")).upper()
    cancel_reason = str(
        data.get("cancelReason")
        or data.get("rejectReason")
        or data.get("errorMessage")
        or ""
    )

    code = order.get("code")
    if code not in (None, 200, "200"):
        if not status:
            status = "ERROR"
        if not cancel_reason:
            cancel_reason = str(order.get("msg") or "")

    return status, cancel_reason


def _extract_filled_qty(data: dict):
    """
    尽可能兼容不同 SDK 可能出现的成交字段。
    返回 Decimal 或 None（表示字段缺失/无法判断）。
    """
    keys = [
        "cumMatchFillSize",
        "cumSuccessFillSize",
        "cumFilledSize",
        "cumFillSize",
        "filledSize",
        "executedSize",
    ]
    for k in keys:
        v = data.get(k)
        if v not in (None, "", "0", 0):
            try:
                return Decimal(str(v))
            except Exception:
                continue

    # 如果字段存在但为 0
    for k in keys:
        if k in data:
            try:
                return Decimal(str(data.get(k) or "0"))
            except Exception:
                return Decimal("0")

    return None  # 字段完全缺失


def _monitor_positions_loop():
    global BOT_POSITIONS
    print("[HARD_SL] monitor thread started")

    while True:
        try:
            for (bot_id, symbol), pos in list(BOT_POSITIONS.items()):
                if bot_id not in HARD_SL_BOTS:
                    continue

                side = pos.get("side")
                qty = pos.get("qty") or Decimal("0")
                entry_price = pos.get("entry_price")
                hard_sl_armed = bool(pos.get("hard_sl_armed", False))

                if not side or qty <= 0 or entry_price is None:
                    continue

                if hard_sl_armed:
                    continue

                try:
                    rules = _get_symbol_rules(symbol)
                    min_qty = rules["min_qty"]
                    exit_side = "SELL" if side == "BUY" else "BUY"
                    px_str = get_market_price(symbol, exit_side, str(min_qty))
                    current_price = Decimal(px_str)
                except Exception as e:
                    print(f"[HARD_SL] get_market_price error bot={bot_id} symbol={symbol}:", e)
                    continue

                if side == "BUY":
                    pnl_pct = (current_price - entry_price) * Decimal("100") / entry_price
                    lock_price = entry_price * (Decimal("1") + HARD_SL_LEVEL_PCT / Decimal("100"))
                else:
                    pnl_pct = (entry_price - current_price) * Decimal("100") / entry_price
                    lock_price = entry_price * (Decimal("1") - HARD_SL_LEVEL_PCT / Decimal("100"))

                if pnl_pct >= HARD_SL_TRIGGER_PCT:
                    print(
                        f"[HARD_SL] ARMED -> place STOP_MARKET "
                        f"bot={bot_id} symbol={symbol} entry={entry_price} "
                        f"trigger={lock_price} pnl={pnl_pct:.4f}%"
                    )
                    try:
                        stop_order = create_stop_market_order(
                            symbol=symbol,
                            side=exit_side,
                            size=str(qty),
                            trigger_price=str(lock_price),
                            reduce_only=True,
                            client_id=None,
                        )

                        status, cancel_reason = _order_status_and_reason(stop_order)
                        order_id = stop_order.get("order_id")
                        client_order_id = stop_order.get("client_order_id")

                        print(
                            f"[HARD_SL] STOP_MARKET created status={status} "
                            f"order_id={order_id} cancelReason={cancel_reason!r}"
                        )

                        if status not in ("ERROR", "CANCELED", "REJECTED", "EXPIRED"):
                            pos["hard_sl_armed"] = True
                            pos["hard_sl_level"] = lock_price
                            pos["hard_sl_stop_id"] = order_id
                            pos["hard_sl_client_order_id"] = client_order_id

                    except Exception as e:
                        print(f"[HARD_SL] create_stop_market_order error bot={bot_id} symbol={symbol}:", e)

        except Exception as e:
            print("[HARD_SL] monitor loop top-level error:", e)

        time.sleep(HARD_SL_POLL_INTERVAL)


def _ensure_monitor_thread():
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
    # ENTRY
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

        size_str = str(snapped_qty)
        print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} budget={budget} -> request_qty={size_str}")

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

        data = (order or {}).get("data") or {}
        status, cancel_reason = _order_status_and_reason(order)
        tif = str(data.get("timeInForce", "")).upper()
        filled_qty = _extract_filled_qty(data)

        print(
            f"[ENTRY] order status={status} cancelReason={cancel_reason!r} "
            f"timeInForce={tif} filled={filled_qty}"
        )

        # 明显“没成交”的 IOC 场景仍然不记录
        if status == "PENDING" and tif == "IMMEDIATE_OR_CANCEL" and cancel_reason and (filled_qty is None or filled_qty <= 0):
            print("[ENTRY] IOC pending + cancelReason -> treat as not filled")
            return jsonify({
                "status": "order_not_filled",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "requested_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "raw_order": order,
            }), 200

        # 顶层 ERROR 直接不记录
        if status in ("ERROR", "REJECTED", "CANCELED", "EXPIRED"):
            return jsonify({
                "status": "order_failed",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "requested_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "raw_order": order,
            }), 200

        # ✅ 关键折中：
        # - 如果 filled_qty 有且 >0 -> 记录
        # - 如果 filled_qty 缺失 -> 只要不是明显 IOC 失败场景，也允许记录
        should_record = (filled_qty is not None and filled_qty > 0) or (filled_qty is None)

        if not should_record:
            print("[ENTRY] filled=0 -> not recording position")
            return jsonify({
                "status": "order_not_filled",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "requested_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "filled": str(filled_qty),
                "raw_order": order,
            }), 200

        key = (bot_id, symbol)

        computed = (order or {}).get("computed") or {}
        price_str = computed.get("price")
        try:
            entry_price_dec = Decimal(str(price_str)) if price_str is not None else None
        except Exception:
            entry_price_dec = None

        BOT_POSITIONS[key] = {
            "side": side_raw,
            "qty": Decimal(size_str),
            "entry_price": entry_price_dec,
            "hard_sl_armed": False,
            "hard_sl_level": None,
            "hard_sl_stop_id": None,
            "hard_sl_client_order_id": None,
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

        if not pos or pos.get("qty", Decimal("0")) <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close (local)")
            return jsonify({"status": "no_position_local"}), 200

        entry_side = pos["side"]
        qty = pos["qty"]
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

        if status in ("ERROR", "REJECTED", "CANCELED", "EXPIRED"):
            print("[EXIT] exit failed -> keep local position")
            return jsonify({
                "status": "exit_failed",
                "mode": "exit",
                "bot_id": bot_id,
                "symbol": symbol,
                "exit_side": exit_side,
                "requested_qty": str(qty),
                "order_status": status,
                "cancel_reason": cancel_reason,
                "raw_order": order,
            }), 200

        # 尝试撤硬 SL
        hard_sl_order_id = pos.get("hard_sl_stop_id")
        hard_sl_client_order_id = pos.get("hard_sl_client_order_id")
        if hard_sl_order_id or hard_sl_client_order_id:
            try:
                print(
                    f"[EXIT] cancel hard SL bot={bot_id} symbol={symbol} "
                    f"order_id={hard_sl_order_id} client_order_id={hard_sl_client_order_id}"
                )
                cancel_order(
                    symbol=symbol,
                    order_id=hard_sl_order_id,
                    client_order_id=hard_sl_client_order_id,
                )
            except Exception as e:
                print("[EXIT] cancel hard SL error:", e)

        BOT_POSITIONS[key] = {
            "side": entry_side,
            "qty": Decimal("0"),
            "entry_price": None,
            "hard_sl_armed": False,
            "hard_sl_level": None,
            "hard_sl_stop_id": None,
            "hard_sl_client_order_id": None,
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
