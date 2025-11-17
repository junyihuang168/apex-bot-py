import os
import time

from flask import Flask, jsonify, request

from apex_client import make_client


app = Flask(__name__)


# --------------------------------------------------
# 路由 1：健康检查
# --------------------------------------------------
@app.route("/")
def health():
    return "ok", 200


# --------------------------------------------------
# 路由 2：手动测试：直接访问 /test 看 Apex 是否正常
# --------------------------------------------------
@app.route("/test")
def test():
    try:
        client = make_client()
    except Exception as e:
        print("✗ make_client() failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "make_client",
            "error": str(e),
        }), 500

    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3 ok:", configs)
        print("get_account_v3 ok:", account)
    except Exception as e:
        print("✗ configs_v3/get_account_v3 failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "configs_or_account",
            "error": str(e),
        }), 500

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
        print("✗ create_order_v3 failed in /test:", e)
        return jsonify({
            "status": "error",
            "where": "create_order_v3",
            "error": str(e),
        }), 500

    return jsonify({
        "status": "ok",
        "configs": configs,
        "account": account,
        "order": order,
    }), 200


# --------------------------------------------------
# 小工具：把 TV 的 symbol 转成 Apex 格式（BTCUSDT -> BTC-USDT）
# --------------------------------------------------
def normalize_symbol(sym: str) -> str:
    if not sym:
        return sym
    sym = sym.upper()
    if "-" in sym:
        return sym

    # 典型现货：BTCUSDT, ETHUSDT ...
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

    # 1) 检查 secret
    expected_secret = os.getenv("WEBHOOK_SECRET") or os.getenv("APEX_WEBHOOK_SECRET")
    recv_secret = data.get("secret")

    if expected_secret and recv_secret != expected_secret:
        print("✗ Webhook secret mismatch, expected:", expected_secret, "got:", recv_secret)
        return jsonify({"status": "error", "error": "invalid secret"}), 403

    # 2) 是否开启真实下单
    enable_live_raw = os.getenv("ENABLE_LIVE_TRADING", "false")
    enable_live = enable_live_raw.strip().lower() in ("1", "true", "yes", "y")
    print("ENABLE_LIVE_TRADING raw =", repr(enable_live_raw))
    print("ENABLE_LIVE_TRADING normalized =", enable_live)

    # 3) 解析 TradingView 传过来的字段
    tv_symbol = data.get("symbol")
    side_raw = (data.get("side") or "").lower()     # "buy" / "sell"
    signal_type = (data.get("signal_type") or "").lower()  # "entry" / "exit" 等
    position_size_str = data.get("position_size") or "0"

    norm_symbol = normalize_symbol(tv_symbol)
    print(f"✅ Normalized symbol: {tv_symbol} -> {norm_symbol}")

    # Apex 需要大写 BUY / SELL
    side = "BUY" if side_raw == "buy" else "SELL"

    # 这里先简单当成“数量”，之后你要改成按 USDT 计算再转 size 也可以
    try:
        size = float(position_size_str)
    except Exception:
        size = 0.0

    # 如果没开真实交易，就直接返回 OK（用于调试）
    if not enable_live:
        print("LIVE trading disabled, skip calling Apex. side=", side, "size=", size)
        return jsonify({
            "status": "ok",
            "live_trading": False,
            "symbol": norm_symbol,
            "side": side,
            "size": size,
        }), 200

    # 4) 创建 Apex 客户端
    try:
        client = make_client()
    except Exception as e:
        print("✗ make_client() failed in /webhook:", e)
        return jsonify({
            "status": "error",
            "where": "make_client",
            "error": str(e),
        }), 500

    # 5) 真正下单
    current_time = int(time.time())
    try:
        order = client.create_order_v3(
            symbol=norm_symbol,
            side=side,
            type="MARKET",
            size=str(size),
            timestampSeconds=current_time,
            price="0",  # 市价单用不到
        )
        print("✅ create_order_v3 ok in /webhook:", order)
    except Exception as e:
        print("✗ create_order_v3 failed in /webhook:", e)
        return jsonify({
            "status": "error",
            "where": "create_order_v3",
            "error": str(e),
        }), 500

    return jsonify({
        "status": "ok",
        "live_trading": True,
        "symbol": norm_symbol,
        "side": side,
        "size": size,
        "order": order,
    }), 200


if __name__ == "__main__":
    # 本地跑 / 调试用；在 DO / Heroku 上通常由 gunicorn 调用
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
