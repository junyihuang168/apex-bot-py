import os
import json
import logging
from flask import Flask, request, jsonify

# ---------- 环境变量 ----------
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
ENABLE_LIVE_TRADING = os.environ.get("ENABLE_LIVE_TRADING", "false").lower() == "true"

# ---------- 日志 ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------- Flask app ----------
app = Flask(__name__)


@app.route("/", methods=["GET"])
def health_check():
    """健康检查"""
    return "apex-bot-py is running", 200


@app.route("/tv-webhook", methods=["POST"])
def tv_webhook():
    """接收 TradingView 警报"""
    logging.info("📩 Incoming request: /tv-webhook")
    logging.info("Headers: %s", dict(request.headers))

    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception as e:
        logging.error("❌ Failed to parse JSON body: %s", e)
        return jsonify({"ok": False, "error": "invalid json"}), 400

    logging.info("Body from TradingView: %s", json.dumps(payload, ensure_ascii=False))

    # --- 校验 secret（如果你在 TV 里有写 "secret"） ---
    if WEBHOOK_SECRET:
        tv_secret = str(payload.get("secret", ""))
        if tv_secret != WEBHOOK_SECRET:
            logging.warning("⚠️ Invalid webhook secret, ignoring alert.")
            return jsonify({"ok": False, "error": "invalid secret"}), 401

    # --- 这里只是 LOG 模式，不真下单 ---
    if not ENABLE_LIVE_TRADING:
        logging.info("🟡 ENABLE_LIVE_TRADING = False, LOG ONLY 模式，不会发送真实订单。")
        return jsonify({"ok": True, "mode": "log_only"}), 200

    # === 将来你要走官方 Python SDK 真正下单，就在这里写 ===
    logging.info("🟢 ENABLE_LIVE_TRADING = True，本来可以在这里调用 ApeX 官方 SDK 下单。")
    # TODO: 调用 ApeX SDK 下单代码（以后再补）

    return jsonify({"ok": True, "mode": "live_trading"}), 200


if __name__ == "__main__":
    # DO 会提供 PORT 环境变量，这里优先用 PORT，默认 8080
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
