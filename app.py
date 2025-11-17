import os
import time

from flask import Flask, jsonify, request

from apex_client import make_client

app = Flask(__name__)


# ----------------------------------------
# 小工具：把 BTCUSDT -> BTC-USDT
# ----------------------------------------
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


# ----------------------------------------
# 路由 0：健康检查
# ----------------------------------------
@app.route("/")
def root():
    return "ok", 200


@app.route("/health")
def health():
    return "ok", 200


# ----------------------------------------
# 路由 1：手动测试  /test  （浏览器打开）
# ----------------------------------------
@app.route("/test")
def test():
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3 ok in /test")
        print("get_account_v3 ok in /test")
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


# ----------------------------------------
# 路由 2：TradingView Webhook 下单
# ----------------------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    # 1) 解析 JSON
    try:
        data = request.get_json(force=True, silent=False)
    except Exception as e:
        print("❌ Failed to parse JSON in /webhook:", e)
        return "bad json", 400

    print("📩 Incoming webhook:", data)

    # 2) 校验 secret（和 TradingView 里的保持一致）
    recv_secret = data.get("secret")
    expected_secret = os.getenv("WEBHOOK_SECRET", "")
    if expected_secret and recv_secret != expected_secret:
        print("❌ Invalid webhook secret")
        return "invalid secret", 403

    # 3) 读取交易参数
    raw_symbol = data.get("symbol", "")
    side = data.get("side", "buy").upper()
    position_size = str(data.get("position_size", "1"))
    order_type = data.get("order_type", "market").upper()
    signal_type = data.get("signal_type", "entry")

    enable_live_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    enable_live = enable_live_raw.lower() == "true"
    print("ENABLE_LIVE_TRADING raw =", repr(enable_live_raw))
    print("ENABLE_LIVE_TRADING normalized =", enable_live)

    symbol = normalize_symbol(raw_symbol)
    print("✅ Normalized symbol:", raw_symbol, "->", symbol)

    # 4) 如果只是想测试流程，不真正下单，可以把 ENABLE_LIVE_TRADING 设成 false
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

    # 5) 创建 client
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return "make_client failed", 500

    # 🔴 关键修复：必须先调用 configs_v3() 和 get_account_v3()
    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /webhook")
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /webhook:", e)
        return "configs_or_account failed", 500

    # 6) 真正下单
    current_time = int(time.time())
    price = "0"  # 市价单随便填一个 price，SDK 内部会处理

    try:
        order = client.create_order_v3(
            symbol=symbol,
            side=side,
            type=order_type,
            size=str(position_size),
            timestampSeconds=current_time,
            price=price,
        )
        print("✅ create_order_v3 ok in /webhook:", order)
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return "create_order_v3 failed", 500

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
