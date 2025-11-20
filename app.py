import os
from flask import Flask, request, jsonify

from apex_client import create_market_order, get_account

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


@app.route("/account", methods=["GET"])
def account():
    try:
        info = get_account()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify(info)


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    print("[WEBHOOK] raw body:", data)

    # 1）校验 secret
    secret = data.get("secret")
    print("[WEBHOOK] secret from TV:", secret)
    print("[WEBHOOK] expected secret:", WEBHOOK_SECRET)
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        print("[WEBHOOK] invalid secret, ignore")
        return "forbidden", 403

    # 2）从 TradingView JSON 里拿字段
    symbol = data.get("symbol")
    side = data.get("side")
    size_usdt = float(data.get("size", 0) or 0)      # TV 里的 size = USDT 金额
    price_from_tv = float(data.get("price", 0) or 0) # TV 里的 price，用来估算数量
    reduce_only = bool(data.get("reduce_only", False))
    client_id = data.get("client_id") or None

    print(
        "[WEBHOOK] sending order to Apex:",
        f"symbol={symbol} side={side} sizeUSDT={size_usdt} price={price_from_tv}",
        f"reduce_only={reduce_only}",
    )

    # 3）调用 apex_client 下单
    try:
        resp = create_market_order(
            symbol=symbol,
            side=side,
            usdt_size=size_usdt,
            tv_price=price_from_tv,
            reduce_only=reduce_only,
            client_id=client_id,
        )
    except Exception as e:
        print("[WEBHOOK] error when sending order to Apex:", repr(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "ok", "apex": resp})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
