import os
import time
import decimal  # 目前没用到，但留着也可以

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
# 工具函数：简单把 TV 符号转成 Apex 符号（ZECUSDT -> ZEC-USDT）
# --------------------------------------------------
def normalize_symbol(sym: str) -> str:
    if not isinstance(sym, str):
        return sym
    if "-" in sym:
        return sym
    # 很简单的转换规则：XXXUSDT -> XXX-USDT
    if sym.endswith("USDT"):
        base = sym[:-4]
        return f"{base}-USDT"
    return sym


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
# 路由 3：TradingView Webhook 接收 + 自动下单
#   - 如果设置了 WEBHOOK_SECRET，则必须匹配
#   - 是否真的下单由 ENABLE_LIVE_TRADING 控制
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # 1) 可选的 secret 校验（TV 里填的 Webhook Secret 要和 DO 上的一样）
    expected_secret = os.getenv("WEBHOOK_SECRET")
    recv_secret = data.get("secret")

    if expected_secret:
        if recv_secret != expected_secret:
            print("❌ Webhook secret mismatch")
            return jsonify({
                "status": "forbidden",
                "message": "Webhook secret mismatch",
            }), 403
    else:
        print("⚠️ No WEBHOOK_SECRET set in env, skipping secret check")

    # 2) 读取是否启用真实交易
    enable_live = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"
    if not enable_live:
        print("💤 ENABLE_LIVE_TRADING is not true -> DRY RUN (no real orders)")
        return jsonify({
            "status": "dry-run",
            "message": "Webhook received but live trading is disabled",
            "data": data,
        }), 200

    # 3) 从 TradingView payload 提取参数
    tv_symbol       = data.get("symbol")
    tv_side         = (data.get("side") or "").upper()      # "BUY" / "SELL"
    tv_order_type   = (data.get("order_type") or "market").upper()  # MARKET / LIMIT
    tv_size_raw     = data.get("position_size", 0)
    tv_signal_type  = data.get("signal_type")  # "entry" / "exit" 等，可用于日志

    # 尝试把 size 转成字符串（Apex 接受字符串）
    try:
        size = decimal.Decimal(str(tv_size_raw))
    except Exception:
        size = decimal.Decimal("0")

    if size <= 0:
        print("❌ Invalid position_size from webhook:", tv_size_raw)
        return jsonify({
            "status": "error",
            "message": "Invalid position_size",
        }), 400

    # 转换符号格式
    apex_symbol = normalize_symbol(tv_symbol)

    print(f"🛠 Prepared order -> symbol={apex_symbol}, side={tv_side}, "
          f"type={tv_order_type}, size={str(size)}, signal_type={tv_signal_type}")

    # 4) 调用 Apex 下单
    try:
        client = make_client()
        order = client.create_order_v3(
            symbol=apex_symbol,
            side=tv_side,              # "BUY" / "SELL"
            type=tv_order_type,        # "MARKET" / "LIMIT"
            size=str(size),
            timestampSeconds=time.time(),
            # 市价单 price 不起作用，但 SDK 需要字段，可以随便给个字符串
            price="0",
        )
        print("✅ Order executed:", order)
        return jsonify({
            "status": "ok",
            "message": "Order sent to Apex",
            "order": order,
        }), 200

    except Exception as e:
        print("❌ Error placing order:", e)
        return jsonify({
            "status": "error",
            "message": str(e),
        }), 500


# --------------------------------------------------
# 主入口：本地运行时使用（DO 上会用 gunicorn/内部方式启动）
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
