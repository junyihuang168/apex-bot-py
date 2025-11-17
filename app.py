import os
import time

from flask import Flask, jsonify, request

from apex_client import make_client


app = Flask(__name__)


def _str2bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
ENABLE_LIVE_TRADING_RAW = os.getenv("ENABLE_LIVE_TRADING", "false")


def normalize_symbol(sym: str) -> str:
    """把 TV 的 BTCUSDT 之类转换成 APEX 需要的 BTC-USDT"""
    if not sym:
        return sym
    sym = sym.upper()
    if "-" in sym:
        return sym
    if len(sym) >= 6:
        base = sym[:-4]
        quote = sym[-4:]
        return f"{base}-{quote}"
    return sym


@app.route("/")
def root():
    return "ok", 200


@app.route("/health")
def health():
    return "ok", 200


@app.route("/test")
def test():
    """手动测试：直接在 DO 上访问 /test，看能不能成功下单"""

    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /test:", e)
        return jsonify({"status": "error", "where": "make_client", "error": str(e)}), 500

    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /test")
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /test:", e)
        return (
            jsonify({"status": "error", "where": "configs_or_account", "error": str(e)}),
            500,
        )

    try:
        now = int(time.time())
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
        return jsonify({"status": "error", "where": "create_order_v3", "error": str(e)}), 500

    return jsonify({"status": "ok", "configs": configs, "account": account, "order": order}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    print("📨 Incoming webhook:", data)

    # 1) 校验 secret
    secret = data.get("secret")
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        print("❌ invalid WEBHOOK_SECRET in /webhook")
        return "invalid secret", 403

    # 2) 是否开启实盘
    enable_live = _str2bool(ENABLE_LIVE_TRADING_RAW, default=False)
    print("ENABLE_LIVE_TRADING raw =", repr(ENABLE_LIVE_TRADING_RAW))
    print("ENABLE_LIVE_TRADING normalized =", enable_live)
    if not enable_live:
        print("🔕 Live trading disabled, skip create_order_v3")
        return "live trading disabled", 200

    # 3) 解析 TradingView 传来的字段
    symbol_tv = data.get("symbol")          # 例如 BTCUSDT
    side = data.get("side")                # 'buy' / 'sell'
    position_size = str(data.get("position_size", "1"))  # 这里你在 TV 里自己控制数量
    order_type = str(data.get("order_type", "market")).lower()
    signal_type = data.get("signal_type", "entry")       # 目前我们只看 'entry'

    normalized_symbol = normalize_symbol(symbol_tv)
    print(f"✅ Normalized symbol: {symbol_tv} -> {normalized_symbol}")

    side_upper = side.upper() if side else None
    type_upper = "MARKET" if order_type == "market" else "LIMIT"

    # 4) 创建客户端 + 拉取配置 & 账户
    try:
        client = make_client()
    except Exception as e:
        print("❌ make_client() failed in /webhook:", e)
        return "make_client error: " + str(e), 500

    try:
        configs = client.configs_v3()
        account = client.get_account_v3()
        print("configs_v3/get_account_v3 ok in /webhook")
    except Exception as e:
        print("❌ configs_v3/get_account_v3 failed in /webhook:", e)
        return "configs/get_account error: " + str(e), 500

    # 5) 下单 —— 完全按照官方 demo 的写法，不再做任何 accountId 的手动处理
    now = int(time.time())
    price = "0"
    if type_upper == "LIMIT":
        # 如果以后你想做限价单，就在 TradingView 里把价格也一起传过来
        price = str(data.get("price", "0"))

    try:
        order_res = client.create_order_v3(
            symbol=normalized_symbol,
            side=side_upper,
            type=type_upper,
            size=position_size,          # 你这里可以先在 TV 里设置成 1 USDT 对应的 size
            timestampSeconds=now,
            price=price,
        )
        print("✅ create_order_v3 ok in /webhook:", order_res)
    except Exception as e:
        print("❌ create_order_v3 failed in /webhook:", e)
        return "create_order_v3 error: " + str(e), 500

    return "ok", 200
