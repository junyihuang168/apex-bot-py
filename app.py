# app.py
import os
import json
import logging

from flask import Flask, request, jsonify

from apex_client import create_market_order, get_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"

app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    return "Apex bot running", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    logger.info("================================ WEBHOOK =================================")
    data = request.get_json(force=True, silent=True) or {}
    logger.info("[WEBHOOK] raw body: %s", json.dumps(data))

    # 1) 校验 secret
    secret_from_tv = data.get("secret", "")
    logger.info("[WEBHOOK] secret from TV: %s", secret_from_tv)
    logger.info("[WEBHOOK] expected secret: %s", WEBHOOK_SECRET)

    if WEBHOOK_SECRET and secret_from_tv != WEBHOOK_SECRET:
        logger.warning("[WEBHOOK] invalid secret, reject")
        return jsonify({"status": "error", "error": "invalid secret"}), 401

    logger.info("[WEBHOOK] payload OK: %s", data)

    # 2) 测试模式：只打印，不真实下单
    if not ENABLE_LIVE_TRADING:
        logger.info("[WEBHOOK] live trading disabled, skip sending to Apex")
        return jsonify({"status": "ok", "live": False}), 200

    try:
        symbol = data.get("symbol")
        side = data.get("side", "BUY")
        size_raw = data.get("size", "0")
        order_type = data.get("type", "MARKET").upper()
        price = data.get("price")      # 这里就是从 TV 拿过来的 price
        leverage_raw = data.get("leverage", "1")
        reduce_only = bool(data.get("reduce_only", False))
        signal_type = data.get("signal_type", "entry")
        client_id = data.get("client_id")

        size = float(size_raw)
        leverage = float(leverage_raw)

        if not symbol or size <= 0:
            return jsonify(
                {"status": "error", "error": "missing symbol or bad size"}
            ), 400

        logger.info(
            "[WEBHOOK] sending order to Apex: symbol=%s side=%s size=%s "
            "type=%s price=%s lev=%s reduce_only=%s signal_type=%s",
            symbol,
            side,
            size,
            order_type,
            price,
            leverage,
            reduce_only,
            signal_type,
        )

        # 当前版本统一用 MARKET 下单，如果你以后想区分 limit 再拆函数
        resp = create_market_order(
            symbol=symbol,
            side=side,
            size=size,
            price=price,
            leverage=leverage,
            reduce_only=reduce_only,
            client_id=client_id,
        )

        logger.info("[WEBHOOK] Apex response: %s", resp)
        return jsonify({"status": "ok", "result": resp}), 200

    except Exception as e:
        logger.exception("[WEBHOOK] error when sending order to Apex")
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
