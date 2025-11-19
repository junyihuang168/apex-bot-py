import os
import json
from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    create_limit_order,
    get_account,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"

@app.route("/")
def index():
    return "Apex bot is running", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    print("================================ WEBHOOK ================================")

    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception as e:
        print("[WEBHOOK] failed to parse JSON:", repr(e))
        return jsonify({"status": "error", "message": "invalid json"}), 400

    print("[WEBHOOK] raw body:", data)

    # 1) 校验 secret
    secret_from_tv = data.get("secret")
    print("[WEBHOOK] secret from TV:", secret_from_tv)
    print("[WEBHOOK] expected secret:", WEBHOOK_SECRET)

    if WEBHOOK_SECRET and secret_from_tv != WEBHOOK_SECRET:
        print("[WEBHOOK] invalid secret, reject")
        return jsonify({"status": "error", "message": "invalid secret"}), 403

    # 2) 解析 TradingView payload
    bot_id      = data.get("bot_id")
    action      = data.get("action")          # "open" / "close"
    symbol      = data.get("symbol")          # 例如 "ZEC-USDT"
    side        = data.get("side")            # "BUY" / "SELL"
    size        = data.get("size")            # 数量(字符串)
    order_type  = str(data.get("type", "MARKET")).upper()    # "MARKET" / "LIMIT"
    leverage    = data.get("leverage")
    client_id   = data.get("client_id")
    reduce_only = bool(data.get("reduce_only", False))
    signal_type = data.get("signal_type")     # "entry" / "exit"
    live_flag   = bool(data.get("live", True))
    price       = data.get("price")          # ★★ 新增：从 TV 直接拿当前价字符串

    print(f"[WEBHOOK] payload OK: {{'symbol': '{symbol}', 'side': '{side}', "
          f"'size': '{size}', 'type': '{order_type}', 'price': '{price}', "
          f"'reduce_only': {reduce_only}, 'signal_type': '{signal_type}'}}")

    if not ENABLE_LIVE_TRADING:
        print("[WEBHOOK] ENABLE_LIVE_TRADING = false -> 只打印，不真实下单")
        return jsonify({"status": "ok", "live": False})

    # live_flag 允许你以后从 Pine 里做一个开关
    live = ENABLE_LIVE_TRADING and live_flag

    # 3) 调用 Apex SDK 下单
    try:
        if order_type == "LIMIT":
            res = create_limit_order(
                symbol=symbol,
                side=side,
                size=size,
                price=price,
                reduce_only=reduce_only,
                client_id=client_id,
                live=live,
            )
        else:
            # MARKET 单也必须带 price（Omni 要求 price 是 Decimal 字符串）
            res = create_market_order(
                symbol=symbol,
                side=side,
                size=size,
                price=price,
                reduce_only=reduce_only,
                client_id=client_id,
                live=live,
            )

        print("[WEBHOOK] order response from Apex:", res)
        return jsonify({"status": "ok", "live": live, "apex": res}), 200

    except Exception as e:
        print("[WEBHOOK] error when sending order to Apex:", repr(e))
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/account", methods=["GET"])
def account():
    try:
        acc = get_account()
        return jsonify({"status": "ok", "account": acc}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    # 方便本地测试；DO 上会由 gunicorn / buildpack 启动
    app.run(host="0.0.0.0", port=8080, debug=True)
