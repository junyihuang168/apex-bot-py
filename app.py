# app.py
# Flask Webhook 服务：
# - GET  /        -> "OK"
# - GET  /health  -> 测试 ApeX 连接（get_account）
# - POST /webhook -> 接收 TradingView Webhook，下单到 ApeX

import os
from typing import Any, Dict

from flask import Flask, request, jsonify

from apex_client import create_market_order, get_account

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"
DEFAULT_ORDER_SIZE = os.getenv("APEX_DEFAULT_ORDER_SIZE", "0.001")


# ----------------------------------------------------------------------
# 根路由：简单健康检查
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
    # 1. 打印原始 body，方便 debug
    raw_body = request.get_data(as_text=True)
    print("=" * 30, "WEBHOOK", "=" * 30)
    print("[WEBHOOK] raw body:", raw_body)

    # 2. 解析 JSON
    data = request.get_json(silent=True)
    if not data:
        print("[WEBHOOK] error: invalid or empty JSON")
        return jsonify({"error": "Invalid or empty JSON"}), 400

    # 3. 校验 secret
    incoming_secret = str(data.get("secret", ""))
    print("[WEBHOOK] secret from TV:", incoming_secret)
    print("[WEBHOOK] expected secret :", WEBHOOK_SECRET)

    if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
        print("[WEBHOOK] error: invalid secret")
        return jsonify({"error": "Invalid secret"}), 403

    # 4. 读取 symbol
    symbol = data.get("symbol")
    if not symbol:
        print("[WEBHOOK] error: missing symbol")
        return jsonify({"error": "Missing 'symbol' in payload"}), 400

    # 5. 读取 side（优先用 side，没有就用 direction 推断）
    side = data.get("side")
    if not side:
        direction = str(data.get("direction", "")).lower()
        if direction in ("long", "buy"):
            side = "BUY"
        elif direction in ("short", "sell"):
            side = "SELL"

    if side not in ("BUY", "SELL"):
        print("[WEBHOOK] error: invalid side/direction ->", side)
        return jsonify({"error": "Missing or invalid 'side'/'direction'"}), 400

    # 6. 读取 size
    size_raw = data.get("size", DEFAULT_ORDER_SIZE)
    try:
        size = str(float(size_raw))
    except Exception:
        print("[WEBHOOK] error: invalid size ->", size_raw)
        return jsonify({"error": f"Invalid size value: {size_raw}"}), 400

    # 7. 读取 price（可选，市价单可以不传）
    price = data.get("price")
    if price is not None:
        try:
            price = str(float(price))
        except Exception:
            print("[WEBHOOK] warn: invalid price ->", price, " -> treat as MARKET")
            price = None

    # 8. 组装本地订单信息，打印出来
    order_payload: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "size": size,
        "price": price,
        "live": ENABLE_LIVE_TRADING,
    }
    print("[WEBHOOK] payload OK:", order_payload)

    # 9. 如果没开实盘，只模拟返回
    if not ENABLE_LIVE_TRADING:
        print("[WEBHOOK] ENABLE_LIVE_TRADING = false, simulate only")
        return jsonify({
            "status": "simulated",
            "order": order_payload,
        }), 200

    # 10. 真正下单到 ApeX
    try:
        res = create_market_order(
            symbol=symbol,
            side=side,
            size=size,
            price=price,
        )
        print("[WEBHOOK] apex response:", res)

        return jsonify({
            "status": "ok",
            "order": order_payload,
            "apex_response": res,
        }), 200
    except Exception as e:
        print("[WEBHOOK] error when sending order to Apex:", e)
        return jsonify({
            "status": "error",
            "order": order_payload,
            "error": str(e),
        }), 500


# ----------------------------------------------------------------------
# 本地测试：python app.py
# DO 上通常用 gunicorn app:app
# ----------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
