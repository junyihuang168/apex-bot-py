import os
import time
import decimal  # 目前没用到，将来如果要更精细的数量可以用

from flask import Flask, request, jsonify
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

# --------------------------------------------------
# Flask 实例（只要一个！）
# --------------------------------------------------
app = Flask(__name__)

# 读取环境变量
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "false").lower() == "true"


# --------------------------------------------------
# 工具：创建 Apex Client
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
        # 让 SDK 自己处理 zk 部分
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )
    return client


# 简单的 symbol 转换：ZECUSDT -> ZEC-USDT
def tv_to_apex_symbol(tv_symbol: str) -> str:
    tv_symbol = str(tv_symbol)
    for quote in ("USDT", "USD", "USDC"):
        if tv_symbol.endswith(quote):
            base = tv_symbol[:-len(quote)]
            return f"{base}-{quote}"
    return tv_symbol  # 不匹配就原样返回


# --------------------------------------------------
# 路由 1：健康检查（DigitalOcean 会请求 /）
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200


# --------------------------------------------------
# 路由 2：手动测试连 Apex + 测试下单
#        （浏览器访问 https://你的app/test）
# --------------------------------------------------
@app.route("/test")
def test():
    client = make_client()

    # 获取配置信息和账户信息
    configs = client.configs_v3()
    account = client.get_account_v3()

    current_time = time.time()
    try:
        # 只是一笔很小的测试单，你也可以注释掉
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",
        )
    except Exception as e:
        # 如果下单失败，不要让服务挂掉
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
# 路由 3：TradingView Webhook 入口
#        TV 警报指向 https://你的app/webhook
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    # 解析 JSON
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # 1) 校验 secret（和 TV 里的 secret 字段 & DO 环境变量一致）
    if WEBHOOK_SECRET:
        if data.get("secret") != WEBHOOK_SECRET:
            print("❌ Webhook secret mismatch")
            return jsonify({"status": "error", "message": "invalid secret"}), 403

    # 2) 取出 TradingView 发送的字段
    #    这些字段来自你 Pine 里 makeApexPayload(...) 生成的 JSON
    side_raw      = data.get("side")          # "buy" / "sell"
    symbol_raw    = data.get("symbol")        # 例如 "ZECUSDT"
    pos_size_raw  = data.get("position_size") # 在 TV 输入的 Position Size
    order_type    = data.get("order_type", "market")
    signal_type   = str(data.get("signal_type", "")).lower()  # "entry" / "exit"
    bot_id        = data.get("bot_id")

    if not side_raw or not symbol_raw or not signal_type:
        return jsonify({
            "status": "error",
            "message": "missing required fields (side / symbol / signal_type)"
        }), 400

    side = str(side_raw).upper()            # BUY / SELL
    apex_symbol = tv_to_apex_symbol(symbol_raw)

    # size 直接用 position_size，当作合约数量/币数量
    # （注意：这不是按 USDT 价值换算，只是“几单位”的意思）
    try:
        if pos_size_raw is None:
            size_str = "0.001"  # 兜底
        else:
            size_str = str(decimal.Decimal(str(pos_size_raw)))
    except Exception:
        size_str = "0.001"

    # 3) 如果没有开启真实交易，只打印日志 & 返回 ok
    if not ENABLE_LIVE_TRADING:
        print("⚠️ ENABLE_LIVE_TRADING != true，仅记录信号不下单。")
        return jsonify({
            "status": "ok",
            "mode": "dry-run",
            "received": data,
        }), 200

    # 4) 创建 Apex client
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed:", e)
        return jsonify({
            "status": "error",
            "message": "failed to init apex client",
            "detail": str(e),
        }), 500

    # 5) 根据 signal_type 发送订单（这里简单处理：entry/exit 都是市价单）
    print(f"🚀 Sending order to Apex: {signal_type} {side} {size_str} {apex_symbol}")

    try:
        ts = int(time.time())
        order_resp = client.create_order_v3(
            symbol=apex_symbol,
            side=side,              # BUY / SELL
            type=order_type.upper(),# MARKET / LIMIT（目前 Pine 用的是 market）
            size=size_str,
            timestampSeconds=ts,
            # 市价单一般不会用到 price，这里给个占位
            price="0",
        )
    except Exception as e:
        print("❌ create_order_v3 error:", e)
        return jsonify({
            "status": "error",
            "message": "order failed",
            "detail": str(e),
        }), 500

    print("✅ Order sent:", order_resp)

    return jsonify({
        "status": "ok",
        "mode": "live",
        "bot_id": bot_id,
        "symbol": apex_symbol,
        "side": side,
        "size": size_str,
        "order": order_resp,
    }), 200


# --------------------------------------------------
# 主入口（本地跑 or DO 运行时都会走这里）
# --------------------------------------------------
if __name__ == "__main__":
    # DO 会把端口放在 PORT 变量里，默认 8080
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
