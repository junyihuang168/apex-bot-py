# app.py
# 简单 Flask 服务：
# - GET  /        -> "OK"
# - GET  /health  -> 测试 ApeX 连接（get_account）
# - POST /webhook -> 接 TradingView webhook，下单到 ApeX

import os
from typing import Any, Dict

from flask import Flask, request, jsonify

from apex_client import create_market_order, get_account

app = Flask(__name__)

# 从环境变量读取配置
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"

# 默认下单数量（如果 webhook 里没带 size，就用这个）
# 例如你想固定 0.001 BTC，可以在 DO 里设 APEX_DEFAULT_ORDER_SIZE=0.001
DEFAULT_ORDER_SIZE = os.getenv("APEX_DEFAULT_ORDER_SIZE", "0.001")


# ----------------------------------------------------------------------
# 根路由：健康检查
# ----------------------------------------------------------------------
@app.route("/", methods=["GET"])
def root() -> tuple[str, int]:
    return "OK", 200


# ----------------------------------------------------------------------
# /health：调用 get_account，看 ApeX 是否连通
# ----------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    try:
        acc = get_account()
        return jsonify({
            "status": "ok",
            "account": acc.get("data", {}),
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
        }), 500


# ----------------------------------------------------------------------
# /webhook：TradingView Webhook 入口
# ----------------------------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    # 1. 解析 JSON
    data = request.get_json(silent=True) or {}
    if not data:
        return jsonify({"error": "No JSON payload received"}), 400

    # 2. 校验 secret（TradingView 发送的 JSON 里要有同样的 secret）
    incoming_secret = str(data.get("secret", ""))
    if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
        return jsonify({"error": "Invalid secret"}), 403

    # 3. 读取 symbol
    # 建议你在 TradingView 的 JSON 里直接写 "symbol": "BTC-USDT" 之类
    symbol = data.get("symbol")
    if not symbol:
        return jsonify({"error": "Missing 'symbol' in payload"}), 400

    # 4. 读取 side（支持两种字段：side / direction）
    #    - side: "BUY" / "SELL"
    #    - direction: "long"/"short"/"buy"/"sell"
    side = data.get("side")
    if not side:
        direction = str(data.get("direction", "")).lower()
        if direction in ("long", "buy"):
            side = "BUY"
        elif direction in ("short", "sell"):
            side = "SELL"

    if side not in ("BUY", "SELL"):
        return jsonify({"error": "Missing or invalid 'side'/'direction'"}), 400

    # 5. 读取 size（如果 webhook 没填，就用默认值）
    size_raw = data.get("size", DEFAULT_ORDER_SIZE)
    try:
        size = str(float(size_raw))  # 转成字符串，确保是数字
    except Exception:
        return jsonify({"error": f"Invalid size value: {size_raw}"}), 400

    # 6. 读取 price（可选；市价单其实可以不传）
    price = data.get("price")
    if price is not None:
        try:
            price = str(float(price))
        except Exception:
            # 传了但不合法，就忽略当市价
            price = None

    # 7. 组装一下我们这边看到的订单信息，方便日志
    payload: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "size": size,
        "price": price,
        "live": ENABLE_LIVE_TRADING,
    }

    # 8. 如果没开实盘，就只做“模拟”——不真正下单，只返回 payload
    if not ENABLE_LIVE_TRADING:
        return jsonify({
            "status": "simulated",
            "message": "ENABLE_LIVE_TRADING is false, not sending order to ApeX",
            "order": payload,
        }), 200

    # 9. 真正发送订单到 ApeX
    try:
        res = create_market_order(
            symbol=symbol,
            side=side,
            size=size,
            price=price,
        )
        return jsonify({
            "status": "ok",
            "order": payload,
            "apex_response": res,
        }), 200
    except Exception as e:
        # 报错也返回 payload，方便你在 DO 日志里对照问题
        return jsonify({
            "status": "error",
            "order": payload,
            "error": str(e),
        }), 500


# ----------------------------------------------------------------------
# 本地测试直接运行：python app.py
# DO 上通常会用 gunicorn app:app
# ----------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
