import os
import time
from decimal import Decimal

from flask import Flask, request, jsonify

# Apex Omni Python SDK
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

# --------------------------------------------------
# Flask 实例
# --------------------------------------------------
app = Flask(__name__)


# --------------------------------------------------
# 工具函数：创建 Apex Client（用 DO 环境变量）
# --------------------------------------------------
def make_client():
    """根据环境变量创建 HttpPrivateSign 客户端"""

    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in app.py:")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    # --------------------------------------------------
    # ⚠️ 现在用的是 TESTNET：
    #     APEX_OMNI_HTTP_TEST + NETWORKID_TEST
    # 如果以后要上主网：
    #     1) 换成 APEX_OMNI_HTTP_MAIN
    #     2) 换成 NETWORKID_OMNI_MAIN_ARB  (按官方文档)
    #     3) 使用主网 API key / l2Key
    # --------------------------------------------------
    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,          # TODO: 上主网时改成 APEX_OMNI_HTTP_MAIN
        network_id=NETWORKID_TEST,    # TODO: 上主网时改成 NETWORKID_OMNI_MAIN_ARB
        zk_seeds=None,                # 让 SDK 自己处理 seeds
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    return client


# --------------------------------------------------
# 路由 1：健康检查（DigitalOcean 默认会请求 /）
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200


# --------------------------------------------------
# 路由 2：手动测试连 Apex + 下一个小单
#   手动在浏览器打开 https://你的域名/test 才会触发
# --------------------------------------------------
@app.route("/test")
def test():
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "make_client",
            "error": str(e),
        }), 500

    try:
        # 测试私有接口是否正常
        configs = client.configs_v3()
        print("configs_v3 ok")

        account = client.get_account_v3()
        print("get_account_v3 ok")
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "configs_or_account",
            "error": str(e),
        }), 500

    # 试着在测试网下一个极小的单
    current_time = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",   # 市价单时一般会被忽略
        )
        print("✅ create_order_v3 ok in /test:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "create_order_v3",
            "error": str(e),
            "configs": configs,
            "account": account,
        }), 500

    return jsonify({
        "status": "ok",
        "mode": "testnet",
        "configs": configs,
        "account": account,
        "order": order,
    }), 200


# --------------------------------------------------
# 小工具：把 TV 的 symbol 转成 Apex 的格式
#   例：ZECUSDT -> ZEC-USDT
# --------------------------------------------------
def normalize_symbol(sym: str) -> str:
    if not sym:
        return sym
    sym = sym.upper()
    if "-" in sym:
        return sym  # 已经是 ZEC-USDT 这种，直接返回

    # 最简单规则：最后 4 个字符当作报价货币（USDT / USDC 之类）
    if len(sym) > 4:
        base = sym[:-4]
        quote = sym[-4:]
        return f"{base}-{quote}"
    return sym


# --------------------------------------------------
# 路由 3：TradingView Webhook 接收 + 下单
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # ---------- 1) 校验 Webhook Secret ----------
    env_secret = os.getenv("WEBHOOK_SECRET", "")
    req_secret = str(data.get("secret", "")) if data.get("secret") is not None else ""

    if env_secret:
        if req_secret != env_secret:
            print(
                "❌ Webhook secret mismatch (env=%r, req=%r)"
                % (env_secret, req_secret)
            )
            return jsonify({
                "status": "error",
                "message": "webhook secret mismatch",
            }), 403

    # ---------- 2) 检查是否允许真实交易 ----------
    raw_flag = os.getenv("ENABLE_LIVE_TRADING", "false")
    live_flag = raw_flag.strip().lower() == "true"

    print("ENABLE_LIVE_TRADING raw =", repr(raw_flag))
    print("ENABLE_LIVE_TRADING normalized =", live_flag)

    if not live_flag:
        print("ℹ️ ENABLE_LIVE_TRADING != 'true' -> 只记录，不真实下单")
        return jsonify({
            "status": "ok",
            "mode": "dry_run",
            "message": "Received webhook but live trading is disabled",
            "data": data,
        }), 200

    # ---------- 3) 解析 TradingView 传来的字段 ----------
    side_raw = str(data.get("side", "")).lower()          # 'buy' / 'sell'
    symbol_raw = str(data.get("symbol", "")).upper()      # 例如 ZECUSDT
    size_raw = data.get("position_size", 0)               # 由 Pine 传进来的仓位大小
    order_type_raw = str(data.get("order_type", "market")).lower()  # 'market'/'limit'
    signal_type = str(data.get("signal_type", "entry")).lower()     # 'entry'/'exit'

    if side_raw not in ("buy", "sell"):
        return jsonify({
            "status": "error",
            "message": "invalid side",
            "data": data,
        }), 400

    symbol = normalize_symbol(symbol_raw)
    print(f"✅ Normalized symbol: {symbol_raw} -> {symbol}")

    # 这里假设 TV 传来的 position_size 已经是 Apex 需要的 size（合约张数/币数量）
    try:
        size_dec = Decimal(str(size_raw))
        if size_dec <= 0:
            raise ValueError("size <= 0")
    except Exception as e:
        print("❌ invalid position_size:", e)
        return jsonify({
            "status": "error",
            "message": "invalid position_size",
            "data": data,
        }), 400

    side_api = side_raw.upper()         # BUY / SELL
    type_api = order_type_raw.upper()   # MARKET / LIMIT

    price_raw = data.get("price", None)
    price_str = "0"
    if price_raw is not None:
        try:
            price_str = str(Decimal(str(price_raw)))
        except Exception:
            price_str = "0"

    # ---------- 4) 调用 Apex 下单 ----------
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return jsonify({
            "status": "error",
            "where": "make_client",
            "error": str(e),
        }), 500

    ts = int(time.time())

    try:
        order = client.create_order_v3(
            symbol=symbol,
            side=side_api,          # BUY / SELL
            type=type_api,          # MARKET / LIMIT
            size=str(size_dec),     # 直接用 position_size
            timestampSeconds=ts,
            price=price_str,        # 市价单会忽略
        )
        print("✅ create_order_v3 ok in /webhook:", order)

        return jsonify({
            "status": "ok",
            "mode": "live",
            "signal_type": signal_type,
            "order": order,
        }), 200

    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return jsonify({
            "status": "error",
            "where": "create_order_v3",
            "error": str(e),
        }), 500


# --------------------------------------------------
# 本地调试入口（DigitalOcean 会用自己的方式启动，不走这里）
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
