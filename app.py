import os
import time

from flask import Flask, jsonify, request

from apex_client import make_client

app = Flask(__name__)


def normalize_symbol(sym: str) -> str:
    """把 TV 的 BTCUSDT 转成 APEX 用的 BTC-USDT"""
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


def populate_account_id_from_response(client, account_res):
    """
    从 get_account_v3 返回里解析 accountId，并写到 client 上，
    以绕过 create_order_v3 里的 “No accountId provided...” 检查。
    """
    try:
        acct_obj = None

        # 常见几种返回形式：
        # 1) {"account": {...}}
        # 2) {"data": {"account": {...}}}
        # 3) 直接就是 {"id": "...", ...}
        if isinstance(account_res, dict):
            if "account" in account_res:
                acct_obj = account_res["account"]
            elif "data" in account_res and isinstance(account_res["data"], dict) and "account" in account_res["data"]:
                acct_obj = account_res["data"]["account"]
            else:
                acct_obj = account_res
        else:
            acct_obj = None

        account_id = None
        if isinstance(acct_obj, dict):
            for k in ("id", "accountId", "account_id", "zkAccountId"):
                v = acct_obj.get(k)
                if v:
                    account_id = str(v)
                    break

        if account_id:
            # 在 client 上挂一个 accountId
            setattr(client, "accountId", account_id)
            print("Set client.accountId from get_account_v3():", account_id)

            # 尽量也给各种 account 客户端对象写上 accountId
            for name in ("accountV3", "account_v3", "accountv3", "account"):
                obj = getattr(client, name, None)
                if obj is not None:
                    try:
                        setattr(obj, "accountId", account_id)
                    except Exception:
                        pass
        else:
            print("Unable to determine accountId from get_account_v3() response, raw:", account_res)
    except Exception as e:
        print("Failed to parse accountId from get_account_v3():", e, "raw:", account_res)


@app.route("/")
def root():
    return "ok", 200


@app.route("/health")
def health():
    return "ok", 200


@app.route("/test")
def test():
    """浏览器手动访问 /test，用来自测 configs/account/order 一条链子"""

    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /test")
        print("Account data in /test:", account)
        populate_account_id_from_response(client, account)
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return (
            jsonify(
                {
                    "status": "error",
                    "where": "configs_or_account",
                    "error": str(e),
                }
            ),
            500,
        )

    now = int(time.time())
    try:
        order = client.create_order_v3(
            symbol="BTC-USDT",
            side="SELL",
            type="MARKET",
            size="0.001",
            timestampSeconds=now,
            price="60000",
        )
        print("✅ create_order_v3 ok in /test:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in /test:", e)
        return (
            jsonify(
                {
                    "status": "error",
                    "where": "create_order_v3",
                    "error": str(e),
                }
            ),
            500,
        )

    return jsonify({"status": "ok", "configs": configs, "account": account, "order": order}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    # 1) 解析 JSON
    try:
        data = request.get_json(force=True, silent=False)
    except Exception as e:
        print("❌ Failed to parse JSON in /webhook:", e)
        return "bad json", 400

    print("📩 Incoming webhook:", data)

    # 2) 校验 secret（和 TradingView 里的一致）
    recv_secret = data.get("secret")
    expected_secret = os.getenv("WEBHOOK_SECRET", "")
    if expected_secret and recv_secret != expected_secret:
        print("❌ Invalid webhook secret")
        return "invalid secret", 403

    # 3) 读取交易参数
    raw_symbol = data.get("symbol", "")
    side = data.get("side", "buy").upper()
    position_size = str(data.get("position_size", "1"))
    order_type = str(data.get("order_type", "market")).upper()
    signal_type = data.get("signal_type", "entry")

    enable_live_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    enable_live = enable_live_raw.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", repr(enable_live_raw))
    print("ENABLE_LIVE_TRADING normalized =", enable_live)

    symbol = normalize_symbol(raw_symbol)
    print("✅ Normalized symbol:", raw_symbol, "->", symbol)

    # 如果只是想测试全链路，又不想真的下单，可以先设 ENABLE_LIVE_TRADING = false
    if not enable_live:
        print("⚠️ Live trading disabled, skip create_order_v3")
        return (
            jsonify(
                {
                    "status": "ok",
                    "live_trading": False,
                    "symbol": symbol,
                    "side": side,
                    "position_size": position_size,
                    "signal_type": signal_type,
                }
            ),
            200,
        )

    # 4) 创建 client
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return "make_client failed", 500

    # 5) 先调用 configs_v3 / get_account_v3，并手动写入 accountId
    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /webhook")
        print("Account data in /webhook:", account)
        populate_account_id_from_response(client, account)
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /webhook:", e)
        return "configs_or_account failed", 500

    # 6) 真正下单
    current_time = int(time.time())
    price = "0"  # 市价单随便给个占位

    try:
        order = client.create_order_v3(
            symbol=symbol,
            side=side,
            type=order_type,
            size=position_size,
            timestampSeconds=current_time,
            price=price,
        )
        print("✅ create_order_v3 ok in /webhook:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return "create_order_v3 failed: " + str(e), 500

    return (
        jsonify(
            {
                "status": "ok",
                "symbol": symbol,
                "side": side,
                "position_size": position_size,
                "signal_type": signal_type,
                "order": order,
            }
        ),
        200,
    )


if __name__ == "__main__":
    # 本地调试用，DO 上不会走到这里
    app.run(host="0.0.0.0", port=8080, debug=True)
