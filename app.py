import os
import logging
from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    get_account,
)

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 和 DO 的环境变量 WEBHOOK_SECRET 保持一致
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")


@app.route("/")
def index():
    return "Apex bot alive", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    # 1. 解析 JSON
    try:
        data = request.get_json(force=True)
    except Exception as e:
        logger.exception("[WEBHOOK] invalid json")
        return jsonify({"ok": False, "error": "invalid json"}), 400

    logger.info("[WEBHOOK] raw body: %s", data)

    # 2. 校验 secret
    secret = data.get("secret", "")
    logger.info("[WEBHOOK] secret from TV: %s", secret)
    logger.info("[WEBHOOK] expected secret: %s", WEBHOOK_SECRET)

    if not WEBHOOK_SECRET or secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "bad secret"}), 403

    # 3. 提取参数（TradingView 传来的）
    symbol = data.get("symbol")          # 例如 'ZEC-USDT'
    side = data.get("side", "").upper()  # BUY / SELL
    order_type = data.get("type", "MARKET").upper()
    size = str(data.get("size", "0"))
    reduce_only = bool(data.get("reduce_only", False))
    client_id = data.get("client_id")
    signal_type = data.get("signal_type", "entry")

    # TV 传来的 price 只用来打日志，不再作为参数传给 create_market_order
    tv_price = data.get("price")
    logger.info(
        "[WEBHOOK] sending order to Apex: "
        "symbol=%s side=%s size=%s type=%s price=%s reduce_only=%s signal_type=%s",
        symbol, side, size, order_type, tv_price, reduce_only, signal_type,
    )

    # 4. 根据 type 调用我们封装好的下单函数
    try:
        if order_type == "MARKET":
            # ⚠️ 这里不要再传 price=xxx，否则就会出现你刚才的 TypeError
            resp = create_market_order(
                symbol=symbol,
                side=side,
                size=size,
                reduce_only=reduce_only,
                client_id=client_id,
            )
        else:
            # 目前只支持 MARKET，下次要做 LIMIT 再加一个 create_limit_order
            return jsonify({"ok": False, "error": f"unsupported order type: {order_type}"}), 400

    except Exception as e:
        logger.exception("[WEBHOOK] error when sending order to Apex")
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True, "data": resp}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    # DO 会把流量打到这个端口，保持不动就行
    app.run(host="0.0.0.0", port=port)
