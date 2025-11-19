# app.py
# ------------------------------------------------------------
# Flask Webhook 接收 TradingView 信号，转发至 ApeX 主网
# ------------------------------------------------------------

import os
from flask import Flask, request, jsonify

from apex_client import create_market_order

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"


@app.route("/webhook", methods=["POST"])
def webhook():
    print("=" * 30, "WEBHOOK", "=" * 30)

    try:
        data = request.get_json(force=True)
    except Exception as e:
        print("[WEBHOOK] invalid JSON:", e)
        return jsonify({"status": "error", "msg": "invalid json"}), 400

    print("[WEBHOOK] raw body:", data)

    # 1. 校验 secret
    tv_secret = data.get("secret")
    print("[WEBHOOK] secret from TV:", tv_secret)
    print("[WEBHOOK] expected secret:", WEBHOOK_SECRET)

    if WEBHOOK_SECRET and tv_secret != WEBHOOK_SECRET:
        print("[WEBHOOK] secret mismatch, ignore")
        return jsonify({"status": "ignored", "msg": "bad secret"}), 401

    # 2. 提取字段
    symbol = data.get("symbol")
    side = data.get("side")  # "BUY" / "SELL"
    size = data.get("size")
    price = data.get("price", None)

    # 有些人会把 "None" 当字符串传，这里顺手处理一下
    if price in ("None", "", "null"):
        price = None

    # payload 日志
    payload_view = {
        "symbol": symbol,
        "side": side,
        "size": str(size) if size is not None else None,
        "price": price,
        "live": ENABLE_LIVE_TRADING,
    }
    print("[WEBHOOK] payload OK:", payload_view)

    if not ENABLE_LIVE_TRADING:
        print("[WEBHOOK] ENABLE_LIVE_TRADING = false, simulate only")
        return jsonify({"status": "ok", "live": False, "order": payload_view}), 200

    # 3. 真正下单
    try:
        resp = create_market_order(
            symbol=symbol,
            side=side,
            size=size,
            price=price,  # 这里如果是 None，apex_client 里面会自动不传给 Apex
        )
        print("[WEBHOOK] apex response:", resp)
        return jsonify({"status": "ok", "live": True, "apex": resp}), 200
    except Exception as e:
        print("[WEBHOOK] error when sending order to Apex:", repr(e))
        return jsonify({"status": "error", "msg": str(e)}), 500


@app.route("/", methods=["GET"])
def index():
    return "Apex bot running", 200


if __name__ == "__main__":
    # 本地测试用；DO 上不会跑到这里
    app.run(host="0.0.0.0", port=8080, debug=True)
