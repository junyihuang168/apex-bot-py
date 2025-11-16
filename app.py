import os
import time
import decimal

from flask import Flask, request, jsonify
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

# 只保留一个 Flask 实例
app = Flask(__name__)

# --------------------------------------------------
# 读取 DigitalOcean 环境变量 & 创建 Apex Client
# --------------------------------------------------
def make_client():
    key        = os.getenv("APEX_API_KEY")
    secret     = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key      = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables:")
    print("API_KEY:",    bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:",       bool(passphrase))
    print("L2KEY:",      bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_TEST,
        # 让 SDK 自己处理 seeds
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )
    return client

# --------------------------------------------------
# 路由 1：健康检查（DO 默认会请求 /）
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200

# --------------------------------------------------
# 路由 2：测试连 Apex + 可选下单
#   手动在浏览器打开  https://你的域名/test  才会触发
# --------------------------------------------------
@app.route("/test")
def test():
    client = make_client()

    # 获取配置信息和账户信息
    configs = client.configs_v3()
    account = client.get_account_v3()

    current_time = time.time()
    try:
        # 这里是一个很小的测试单，你也可以先注释掉
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",
        )
    except Exception as e:
        # 如果下单失败，不要让服务挂掉，返回错误信息即可
        return jsonify({
            "status": "error",
            "error": str(e),
            "configs": configs,
            "account": account,
        }), 500

    return jsonify({
        "status": "ok",
        "configs": configs,
        "account": account,
        "order": order,
    }), 200

# --------------------------------------------------
# 路由 3：TradingView Webhook 接收
#   暂时不做任何 secret 校验，只打印并返回 200
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # 暂时不开启密钥校验，先保证链路稳定
    # 之后要加安全校验，再在这里加 if 判断就行
    return jsonify({
        "status": "ok",
        "message": "Webhook received",
        "data": data,
    }), 200

# --------------------------------------------------
# 主入口：本地运行时使用（DO 上会用 gunicorn/内部方式启动）
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
