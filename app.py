import os
from decimal import Decimal
from flask import Flask, request, jsonify

# 从 apex_client.py 导入工具函数
from apex_client import (
    create_market_order,
    create_market_order_with_tpsl,
    get_market_price,
)

app = Flask(__name__)

# TradingView 里的 secret，用来校验
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# 记住「每个 bot 在每个 symbol 上自己开的那一份仓位」
# key:  (bot_id, symbol) -> value: {"side": "BUY"/"SELL", "qty": Decimal}
BOT_POSITIONS: dict[tuple[str, str], dict] = {}


@app.route("/", methods=["GET"])
def index():
    # DO health check 就是 GET / 看你有没有回 200
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
    bot_id = str(body.get("bot_id", "default"))             # 每个机器人要传一个 bot_id
    tv_client_id = body.get("client_id")                    # 只用于日志

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

        budget = Decimal(str(size_field))
        if budget <= 0:
            return "size (USDT) must be > 0", 400

        # ★ 真正下单（市价开仓 + 附带 TP/SL 触发价）
        order_info = create_market_order_with_tpsl(
            symbol=symbol,
            side=side_raw,
            size_usdt=budget,
            tp_pct=Decimal("0.019"),  # +1.9% TP
            sl_pct=Decimal("0.006"),  # -0.6% SL
            client_id=tv_client_id,
        )

        # 从返回值里拿出真实撮合后的数量，记到每个 bot 自己的仓位上
        computed = order_info.get("computed", {}) if isinstance(order_info, dict) else {}
        size_str = computed.get("size")
        if size_str is None:
            print("[ENTRY] WARNING: no 'computed.size' in order_info, skip BOT_POSITIONS update")
            return jsonify({"status": "ok", "mode": "entry", "order": order_info}), 200

        qty = Decimal(str(size_str))

        print(
            f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} "
            f"final snapped qty={qty}, "
            f"entry={computed.get('entry_price')} "
            f"tp={computed.get('tp_trigger')} "
            f"sl={computed.get('sl_trigger')}"
        )

        # 记住这个 bot 在这个 symbol 上“自己开的那一份仓位”
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if pos and pos["side"] == side_raw:
            # 同方向加仓：数量累加
            new_qty = pos["qty"] + qty
            BOT_POSITIONS[key] = {"side": side_raw, "qty": new_qty}
        else:
            # 第一次开仓/反向开仓：覆盖
            BOT_POSITIONS[key] = {"side": side_raw, "qty": qty}

        print(f"[ENTRY] BOT_POSITIONS[{key}] = {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "order": order_info,
            "bot_position": {
                "bot_id": bot_id,
                "symbol": symbol,
                "side": BOT_POSITIONS[key]["side"],
                "qty": str(BOT_POSITIONS[key]["qty"]),
            },
        }), 200

    # -----------------------
    # 3. EXIT：卖出 / 平仓
    # -----------------------
    if signal_type == "exit":
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if not pos or pos["qty"] <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close")
            # 返回 200，避免 TV 重试
            return jsonify({"status": "no_position"}), 200

        entry_side = pos["side"]
        qty = pos["qty"]

        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} entry_side={entry_side} "
            f"qty={qty} -> exit_side={exit_side}"
        )

        order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=tv_client_id,
        )

        BOT_POSITIONS[key] = {"side": entry_side, "qty": Decimal("0")}
        print(f"[EXIT] BOT_POSITIONS[{key}] -> {BOT_POSITIONS[key]}")

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
