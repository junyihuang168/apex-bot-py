import os
import json
from flask import Flask, request, jsonify

# -------------------------------------------------
# 创建 Flask 应用
# -------------------------------------------------
app = Flask(__name__)


# -------------------------------------------------
# 基础路由：根路径 + 健康检查
# -------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "ok", "msg": "apex-bot webhook listener running"}), 200


@app.route("/health", methods=["GET"])
def health():
    """
    DigitalOcean / 本地自测用：返回简单的健康状态
    """
    return jsonify({"status": "ok"}), 200


# -------------------------------------------------
# TradingView Webhook 路由（本地 + DO 通用）
# -------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    """
    接收 TradingView 警报：
    - TradingView 那边填的 Webhook URL:  https://xxx.ngrok-free.dev/webhook  或 DO 的地址
    - Pine 代码里 alert() 发送的是 JSON 字符串
    这里统一当成 text 读进来，再自己 json.loads 解析
    """

    print("=" * 30, "WEBHOOK", "=" * 30)

    # 1. 不管 Content-Type，先把原始 body 拿出来
    raw_body = request.get_data(as_text=True)
    print("[WEBHOOK] raw body:", raw_body)

    # 2. 尝试把 body 当成 JSON 解析（TradingView 一般是 text/plain + JSON 字符串）
    try:
        payload = json.loads(raw_body)
    except Exception as e:
        print("[WEBHOOK] JSON parse error:", e)
        # 解析失败，返回 400
        return jsonify({"error": "bad json"}), 400

    # 3. 校验 secret（Pine 输入框 & 服务器环境变量 WEBHOOK_SECRET 必须一致）
    expected_secret = os.environ.get("WEBHOOK_SECRET")
    recv_secret = payload.get("secret")

    print("[WEBHOOK] secret from TV:", recv_secret)
    print("[WEBHOOK] expected secret :", expected_secret)

    if expected_secret and recv_secret != expected_secret:
        print("[WEBHOOK] secret mismatch -> reject")
        return jsonify({"error": "invalid secret"}), 400

    # 4. 打印完整 payload（现在是 Demo 模式，不真正下单）
    print("[WEBHOOK] payload OK:", payload)

    # 👉 以后你想真正下单，再在这里解析 action / side / size，调用 Apex API 即可
    #    目前只返回 200，说明收到了
    return jsonify({"status": "ok"}), 200


# -------------------------------------------------
# 启动方式（本地 & DO 通用）
# -------------------------------------------------
if __name__ == "__main__":
    # DO 会给一个 PORT 环境变量，本地没有时默认 5000
    port = int(os.environ.get("PORT", "5000"))
    # 监听 0.0.0.0，方便本地 + DO + ngrok 都能访问
    app.run(host="0.0.0.0", port=port, debug=True)
