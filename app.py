import os
import json
from flask import Flask, request, jsonify

# -------- 环境变量 --------
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # 暂时可以留空
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"

app = Flask(__name__)

# -------- 健康检查 --------
@app.get("/")
def health():
    mode = "LIVE" if ENABLE_LIVE_TRADING else "LOG ONLY"
    return f"Apex Python bot is running ({mode})"


# -------- TradingView Webhook --------
@app.post("/tv-webhook")
def tv_webhook():
    print("📩 Incoming request: POST /tv-webhook")

    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception as e:
        print("❌ Error parsing JSON:", e)
        return jsonify({"ok": False, "error": "invalid json"}), 400

    print("📦 Body from TradingView:", json.dumps(payload, ensure_ascii=False))

    # 1) 可选：校验 secret（你将来可以在 TV 的 JSON 里加 "secret"）
    if WEBHOOK_SECRET:
        if payload.get("secret") != WEBHOOK_SECRET:
            print("❌ Invalid webhook secret, ignoring alert")
            return jsonify({"ok": False, "error": "invalid secret"}), 401

    # 2) 解析基础字段
    bot_id = payload.get("bot_id", "BOT_1")
    symbol = payload.get("symbol", "ZECUSDT")
    side = (payload.get("side") or "").lower()
    position_size = float(payload.get("position_size", 0) or 0)
    order_type = payload.get("order_type", "market")
    leverage = int(payload.get("leverage", 1) or 1)
    signal_type = (payload.get("signal_type") or "").lower()

    print(f"🧠 Parsed alert: bot_id={bot_id}, symbol={symbol}, side={side}, "
          f"size={position_size}, type={order_type}, lev={leverage}, signal={signal_type}")

    # 不合法就忽略
    if not side or position_size <= 0:
        print("⚠️ side 为空 或 position_size <= 0，忽略")
        return jsonify({"ok": True, "msg": "ignored"}), 200

    # 现在先只 LOG，不下单
    print("🟡 ENABLE_LIVE_TRADING =", ENABLE_LIVE_TRADING, "（目前只打印，不真实下单）")

    return jsonify({"ok": True, "mode": "log_only"}), 200


# -------- 本地调试入口 --------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    print(f"🚀 Apex Python bot listening on port {port}")
    app.run(host="0.0.0.0", port=port)
