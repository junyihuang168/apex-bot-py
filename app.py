import os
import time
import decimal

from flask import Flask, jsonify
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

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
        # 让 SDK 处理 zk_seeds
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
# --------------------------------------------------
@app.route("/test")
def test():
    client = make_client()

    # 获取配置信息和账户信息
    configs = client.configs_v3()
    account = client.get_account_v3()

    # 下一个很小的 MARKET 订单作为测试（你也可以先注释掉）
    current_time = time.time()
    try:
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
# 主入口：启动 Flask
# --------------------------------------------------
if __name__ == "__main__":
    # DO 会把端口传到环境变量 PORT（也可以默认 8080）
    port = int(os.getenv("PORT", "8080"))
    # host 一定要 0.0.0.0 才能被 DO 访问到
    app.run(host="0.0.0.0", port=port)
from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    print("📩 Received Webhook:", data)
    return jsonify({"status": "success", "message": "Webhook received"}), 200
