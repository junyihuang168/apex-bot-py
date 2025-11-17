import os
import time
from decimal import Decimal

from flask import Flask, request, jsonify
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

app = Flask(__name__)


# --------------------------------------------------
# 工具函数：创建带补丁的 Apex Client
# --------------------------------------------------
def make_client():
    key = os.getenv("APEX_API_KEY")
    secret = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,          # 现在先用 TEST 网络
        network_id=NETWORKID_TEST,
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    # ---- 关键补丁：修复 accountv3 / account_v3 不一致的问题 ----
    if hasattr(client, "account_v3") and not hasattr(client, "accountv3"):
        client.accountv3 = client.account_v3
        print("✅ Patched client.accountv3 from client.account_v3")

    return client


# --------------------------------------------------
# 健康检查（DO 会请求 /）
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200


# --------------------------------------------------
# 手动测试路由：浏览器打开 https://你的域名/test
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
        configs = client.configs_v3()
        account = client.get_account_v3()
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "configs_or_account",
            "error": str(e),
        }), 500

    # 这里可以选择是否测试下一笔小单
    current_time = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",
        )
        print("✅ create_order_v3 ok in /test:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in /test:", e)
        order = {"error": str(e)}

    return jsonify({
        "status": "ok",
        "configs": configs,
        "account": account,
        "order": order,
    }), 200


# --------------------------------------------------
# 小工具：把 TradingView 的 symbol 转成 Apex 的格式
#   例：BTCUSDT -> BTC-USDT
# --------------------------------------------------
def normalize_symbol(sym: str) -> str:
    if not sym:
        return sym
    sym = sym.upper()
    if "-" in sym:
        return sym
    if len(sym) > 4:
        base = sym[:-4]
        quote = sym[-4:]
        return f"{base}-{quote}"
    return sym


# --------------------------------------------------
# TradingView Webhook 接收 + 下单
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # ---------- 1) 校验 Webhook Secret ----------
    env_secret = os.getenv("WEBHOOK_SECRET")
    req_secret = str(data.get("secret", "")) if data.get("secret") is not None else ""
    if env_secret:
        if req_secret != env_secret:
            print(f"❌ Webhook secret mismatch (env={env_secret}, req={req_secret})")
            return jsonify({"status": "error", "message": "webhook secret mismatch"}), 403

    # ---------- 2) 检查是否允许真实交易 ----------
    live_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    live_flag = live_raw.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", repr(live_raw))
    print("ENABLE_LIVE_TRADING normalized =", live_flag)

    if not live_flag:
        print("ℹ️ ENABLE_LIVE_TRADING != 'true' -> 只记录, 不真实下单")
        return jsonify({
            "status": "ok",
            "mode": "dry_run",
            "message": "Received webhook but live trading is disabled",
            "data": data,
        }), 200

    # ---------- 3) 解析 TradingView 字段 ----------
    side_raw = str(data.get("side", "")).lower()          # 'buy' / 'sell'
    symbol_raw = str(data.get("symbol", "")).upper()      # e.g. BTCUSDT
    size_raw = data.get("position_size", 0)               # 这里是 TV 传进来的数字或字符串
    order_type_raw = str(data.get("order_type", "market")).lower()
    signal_type = str(data.get("signal_type", "entry")).lower()

    if side_raw not in ("buy", "sell"):
        return jsonify({"status": "error", "message": "invalid side", "data": data}), 400

    symbol = normalize_symbol(symbol_raw)
    print(f"✅ Normalized symbol: {symbol_raw} -> {symbol}")

    # position_size -> Decimal
    try:
        size_dec = Decimal(str(size_raw))
        if size_dec <= 0:
            raise ValueError("size <= 0")
    except Exception as e:
        print("❌ invalid position_size:", e)
        return jsonify({"status": "error", "message": "invalid position_size", "data": data}), 400

    side_api = side_raw.upper()          # BUY / SELL
    type_api = order_type_raw.upper()    # MARKET / LIMIT

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
            side=side_api,
            type=type_api,
            size=str(size_dec),
            timestampSeconds=ts,
            price=price_str,
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
# 本地运行入口（DO 上会用自己的方式启动，这段留着也没问题）
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
