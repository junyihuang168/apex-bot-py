import os
import time
from decimal import Decimal

from flask import Flask, request, jsonify
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign

app = Flask(__name__)

# --------------------------------------------------
# 创建 Apex Client
# --------------------------------------------------
def make_client():
    key        = os.getenv("APEX_API_KEY")
    secret     = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key      = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in app.py:")
    print("API_KEY:",    bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:",       bool(passphrase))
    print("L2KEY:",      bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,   # 目前仍然是 TEST 网络
        network_id=NETWORKID_TEST,
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )

    acct_attrs = [a for a in dir(client) if "account" in a.lower()]
    print("Client attributes containing 'account':", acct_attrs)

    return client

# --------------------------------------------------
# 健康检查
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200

# --------------------------------------------------
# 手动测试：浏览器打开 /test
# --------------------------------------------------
@app.route("/test")
def test():
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    # 先试 configs + account
    try:
        configs = client.configs_v3()
        print("configs_v3 ok in /test")
        account = client.get_account_v3()
        print("get_account_v3 ok in /test")
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return jsonify({"status": "error", "where": "configs_or_account", "error": str(e)}), 500

    # 拿到底层的 account client
    account_client = getattr(client, "accountv3", None) or getattr(client, "account", None)
    if account_client is None:
        msg = "No account client (accountv3/account) on HttpPrivateSign"
        print("❌", msg)
        return jsonify({"status": "error", "where": "account_client", "error": msg}), 500

    print("Using account_client type in /test:", type(account_client))

    current_time = int(time.time())
    try:
        order = account_client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=current_time,
            price="60000",
        )
        print("✅ create_order_v3 ok in /test (via account_client):", order)
    except Exception as e:
        print("❌ create_order_v3 failed in /test (via account_client):", e)
        return jsonify({
            "status": "error",
            "where": "create_order_v3",
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
# 工具：ZECUSDT -> ZEC-USDT
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
# TradingView Webhook 下单
# --------------------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    # 1) 校验 Webhook Secret
    env_secret = os.getenv("WEBHOOK_SECRET")
    req_secret = str(data.get("secret", "")) if data.get("secret") is not None else ""
    if env_secret:
        if req_secret != env_secret:
            print("❌ Webhook secret mismatch (env=%s, req=%s)" % (env_secret, req_secret))
            return jsonify({"status": "error", "message": "webhook secret mismatch"}), 403

    # 2) 是否允许真实交易
    raw_flag = os.getenv("ENABLE_LIVE_TRADING", "false")
    live_flag = raw_flag.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", repr(raw_flag))
    print("ENABLE_LIVE_TRADING normalized =", live_flag)

    if not live_flag:
        print("ℹ️ ENABLE_LIVE_TRADING != 'true' -> 只记录, 不真实下单")
        return jsonify({
            "status": "ok",
            "mode": "dry_run",
            "message": "Received webhook but live trading is disabled",
            "data": data,
        }), 200

    # 3) 解析 TV 传来的字段
    side_raw       = str(data.get("side", "")).lower()
    symbol_raw     = str(data.get("symbol", "")).upper()
    size_raw       = data.get("position_size", 0)
    order_type_raw = str(data.get("order_type", "market")).lower()
    signal_type    = str(data.get("signal_type", "entry")).lower()

    if side_raw not in ["buy", "sell"]:
        return jsonify({"status": "error", "message": "invalid side", "data": data}), 400

    symbol = normalize_symbol(symbol_raw)
    print(f"✅ Normalized symbol: {symbol_raw} -> {symbol}")

    try:
        size_dec = Decimal(str(size_raw))
        if size_dec <= 0:
            raise ValueError("size <= 0")
    except Exception as e:
        print("❌ invalid position_size:", e)
        return jsonify({"status": "error", "message": "invalid position_size", "data": data}), 400

    side_api  = side_raw.upper()
    type_api  = order_type_raw.upper()

    price_raw = data.get("price", None)
    price_str = "0"
    if price_raw is not None:
        try:
            price_str = str(Decimal(str(price_raw)))
        except Exception:
            price_str = "0"

    # 4) 调用 Apex 下单（直接用 account_client）
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    account_client = getattr(client, "accountv3", None) or getattr(client, "account", None)
    if account_client is None:
        msg = "No account client (accountv3/account) on HttpPrivateSign"
        print("❌", msg)
        return jsonify({"status": "error", "where": "account_client", "error": msg}), 500

    print("Using account_client type in /webhook:", type(account_client))

    ts = int(time.time())

    try:
        order = account_client.create_order_v3(
            symbol=symbol,
            side=side_api,
            type=type_api,
            size=str(size_dec),
            timestampSeconds=ts,
            price=price_str,
        )
        print("✅ create_order_v3 ok in /webhook (via account_client):", order)
        return jsonify({
            "status": "ok",
            "mode": "live",
            "signal_type": signal_type,
            "order": order,
        }), 200
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook (via account_client):", e)
        return jsonify({
            "status": "error",
            "where": "create_order_v3",
            "error": str(e),
        }), 500

# --------------------------------------------------
# 本地运行
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
