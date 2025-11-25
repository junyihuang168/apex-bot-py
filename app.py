# app.py

import os
from decimal import Decimal, ROUND_DOWN

from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    get_market_price,
    create_market_order_with_tpsl,
)

app = Flask(__name__)

# TradingView 里要带的 secret，用来校验
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ------------------------------------------------
# （仅用于日志预览的撮合规则）
# 真正生效的规则在 apex_client.DEFAULT_SYMBOL_RULES 里
# ------------------------------------------------
MIN_QTY = Decimal("0.01")
STEP_SIZE = Decimal("0.01")
QTY_DECIMALS = 2

# 现在只跑一个 bot：
# key: symbol -> {"side": "BUY"/"SELL", "qty": Decimal}
POSITIONS: dict[str, dict] = {}


def _snap_qty(theoretical_qty: Decimal) -> Decimal:
    """
    仅用于日志预览，真实撮合已在 apex_client 里实现。
    """
    if theoretical_qty <= 0:
        raise ValueError("theoretical_qty must be > 0")

    steps = theoretical_qty // STEP_SIZE
    snapped = steps * STEP_SIZE

    quantum = Decimal("1").scaleb(-QTY_DECIMALS)
    snapped = snapped.quantize(quantum, rounding=ROUND_DOWN)

    if snapped < MIN_QTY:
        raise ValueError(
            f"snapped qty {snapped} < minQty {MIN_QTY} (budget too small)"
        )

    return snapped


@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    # ---------------------------
    # 1. 解析 & 校验基础字段
    # ---------------------------
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    # 校验 secret（可选）
    if WEBHOOK_SECRET:
        if body.get("secret") != WEBHOOK_SECRET:
            print("[WEBHOOK] invalid secret")
            return "forbidden", 403

    symbol = body.get("symbol")
    side_raw = str(body.get("side", "")).upper()
    signal_type = str(body.get("signal_type", "")).lower()  # "entry" / "exit"
    action = str(body.get("action", "")).lower()
    size_field = body.get("size")  # 入场时的 USDT 金额
    bot_id = str(body.get("bot_id", "BOT_1"))  # 现在只跑一个 bot，主要用于日志
    tv_client_id = body.get("client_id")  # 只用于日志，真正给 Apex 的 ID 在 apex_client 里处理

    if not symbol:
        return "missing symbol", 400

    # 兼容：如果没写 signal_type，用 action 来判断 open/close
    if signal_type not in ("entry", "exit"):
        if action in ("open", "entry"):
            signal_type = "entry"
        elif action in ("close", "exit"):
            signal_type = "exit"
        else:
            return "missing signal_type (entry/exit)", 400

    # -----------------------
    # 2. ENTRY：买入 / 开仓
    # -----------------------
    if signal_type == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        if size_field is None:
            return "missing size (USDT)", 400

        # 把 size 当作 USDT 预算
        budget = Decimal(str(size_field))
        if budget <= 0:
            return "size (USDT) must be > 0", 400

        # 仅日志预览：用最小数量估一下理论数量 & snapped
        try:
            ref_price_str = get_market_price(symbol, side_raw, str(MIN_QTY))
            ref_price_dec = Decimal(ref_price_str)
            theoretical_qty = budget / ref_price_dec
            snapped_preview = _snap_qty(theoretical_qty)
            print(
                f"[ENTRY-preview] bot={bot_id} symbol={symbol} side={side_raw} "
                f"budget={budget}USDT -> theoretical qty={theoretical_qty} "
                f"preview snapped={snapped_preview} @ ref {ref_price_str}"
            )
        except Exception as e:
            print("[ENTRY-preview] error:", e)

        # ★ 真正下单：市价开仓 + 2% 限价 TP（reduce_only）
        # 这里的 tp_pct=0.02（+2%）；sl_pct 当前忽略
        order_info = create_market_order_with_tpsl(
            symbol=symbol,
            side=side_raw,
            size_usdt=str(budget),
            tp_pct=Decimal("0.02"),  # +2% TP
            sl_pct=Decimal("0"),     # 暂不使用
            client_id=tv_client_id,
        )

        computed = order_info.get("computed", {}) if isinstance(order_info, dict) else {}
        size_str = computed.get("size")

        if size_str is None:
            print("[ENTRY] WARNING: no 'computed.size' in order_info, skip POSITION update")
            return jsonify({"status": "ok", "mode": "entry", "order": order_info}), 200

        qty = Decimal(str(size_str))

        print(
            f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} "
            f"final snapped qty={qty}, "
            f"entry={computed.get('entry_price')} "
            f"tp={computed.get('tp_price')}"
        )

        # 只跑一个 bot：每个 symbol 记一份仓位
        pos = POSITIONS.get(symbol)
        if pos and pos["side"] == side_raw:
            new_qty = pos["qty"] + qty
        else:
            new_qty = qty
        POSITIONS[symbol] = {"side": side_raw, "qty": new_qty}

        print(f"[ENTRY] POSITION[{symbol}] = {POSITIONS[symbol]}")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "order": order_info,
            "position": {
                "symbol": symbol,
                "side": POSITIONS[symbol]["side"],
                "qty": str(POSITIONS[symbol]["qty"]),
            },
        }), 200

    # -----------------------
    # 3. EXIT：卖出 / 平仓
    # -----------------------
    if signal_type == "exit":
        pos = POSITIONS.get(symbol)

        if not pos or pos["qty"] <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close")
            # 返回 200，避免 TV 一直重发
            return jsonify({"status": "no_position"}), 200

        entry_side = pos["side"]      # 之前是 BUY 还是 SELL
        qty = pos["qty"]              # 之前记录的数量（例如 0.50 ZEC）

        # 平仓方向 = 反向
        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} entry_side={entry_side} "
            f"qty={qty} -> exit_side={exit_side}"
        )

        # 这里只卖出这份记录的数量，用 reduce_only=True
        order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=tv_client_id,
        )

        # 一次 exit 直接清空这个 symbol 的这份仓
        POSITIONS[symbol] = {"side": entry_side, "qty": Decimal("0")}
        print(f"[EXIT] POSITION[{symbol}] -> {POSITIONS[symbol]}")

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "order": order,
        }), 200

    # 其他类型暂时不支持
    return "unsupported signal_type", 400


if __name__ == "__main__":
    # DO / Render 等平台一般会注入 PORT 环境变量
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
