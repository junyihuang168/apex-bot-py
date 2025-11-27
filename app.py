import os
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

# key: (bot_id, symbol) -> {"side": "BUY"/"SELL", "qty": Decimal}
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}


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


@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
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

        # 其它情况（PENDING / OPEN / FILLED）都认为订单已经被系统接受，
        # 市价单基本都会按 request_qty 成交，直接记仓。
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if pos and pos["side"] == side_raw:
            new_qty = pos["qty"] + snapped_qty
        else:
            # 第一次开仓，或者之前是反方向，直接覆盖
            new_qty = snapped_qty

        BOT_POSITIONS[key] = {"side": side_raw, "qty": new_qty}
        print(f"[ENTRY] BOT_POSITIONS[{key}] = {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(BOT_POSITIONS[key]["qty"]),
            "requested_last": str(snapped_qty),
            "order_status": status,
            "cancel_reason": cancel_reason,
            "raw_order": order,
        }), 200

    # ---------------------------
    # 4. EXIT
    # ---------------------------
    if mode == "exit":
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if not pos or pos["qty"] <= 0:
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

        # 否则认为平仓成功，直接把这只 bot 的这份仓清零
        BOT_POSITIONS[key] = {"side": entry_side, "qty": Decimal("0")}
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
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
