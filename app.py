import os
import time
from decimal import Decimal

from flask import Flask, request, jsonify
from apexomni.constants import NETWORKID_TEST, APEX_OMNI_HTTP_TEST
from apexomni.http_private_sign import HttpPrivateSign   # 按教程来的

app = Flask(__name__)

def make_client():
    key        = os.getenv("APEX_API_KEY")
    secret     = os.getenv("APEX_API_SECRET")
    passphrase = os.getenv("APEX_API_PASSPHRASE")
    l2key      = os.getenv("APEX_L2KEY_SEEDS")

    print("Loaded env variables in make_client():")
    print("API_KEY:", bool(key))
    print("API_SECRET:", bool(secret))
    print("PASS:", bool(passphrase))
    print("L2KEY:", bool(l2key))

    if not all([key, secret, passphrase, l2key]):
        raise RuntimeError("Missing one or more APEX_* environment variables")

    client = HttpPrivateSign(
        APEX_OMNI_HTTP_TEST,
        network_id=NETWORKID_TEST,
        zk_seeds=None,
        zk_l2Key=l2key,
        api_key_credentials={
            "key": key,
            "secret": secret,
            "passphrase": passphrase,
        },
    )
    return client


@app.route("/")
def health():
    return "ok", 200


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


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Incoming webhook:", data)

    env_secret = os.getenv("WEBHOOK_SECRET")
    req_secret = str(data.get("secret", "")) if data.get("secret") is not None else ""
    if env_secret:
        if req_secret != env_secret:
            print(f"❌ Webhook secret mismatch (env={env_secret}, req={req_secret})")
            return jsonify({"status": "error", "message": "webhook secret mismatch"}), 403

    live_flag_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    live_flag = live_flag_raw.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", repr(live_flag_raw))
    print("ENABLE_LIVE_TRADING normalized =", live_flag)

    if not live_flag:
        print("ℹ️ ENABLE_LIVE_TRADING != 'true' -> 只记录, 不真实下单")
        return jsonify({
            "status": "ok",
            "mode": "dry_run",
            "message": "Received webhook but live trading is disabled",
            "data": data,
        }), 200

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

    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    # 👇 关键：从 client 里拿 v3 的账户对象
    try:
        account_client = client.account_v3()   # 注意要带括号 ()
        print("Using account_client type in /webhook:", type(account_client))
    except AttributeError as e:
        # 如果这里报错，就说明 SDK 版本没有 account_v3()
        print("❌ client 没有 account_v3():", e)
        # 列出所有带 "order" 的属性帮助排查
        order_attrs = [a for a in dir(client) if "order" in a.lower()]
        print("Attributes containing 'order' on client:", order_attrs)
        return jsonify({
            "status": "error",
            "where": "account_v3",
            "error": str(e),
            "order_attrs": order_attrs,
        }), 500

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
        print("✅ create_order_v3 ok:", order)
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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
