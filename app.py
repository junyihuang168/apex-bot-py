import os
from flask import Flask, request, jsonify

from apex_client import create_market_order

app = Flask(__name__)

# TradingView 警报里带的 "secret" 要和这里一致
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")


@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid JSON:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    # 校验 secret（如果你设置了）
    if WEBHOOK_SECRET:
        if body.get("secret") != WEBHOOK_SECRET:
            print("[WEBHOOK] invalid secret")
            return "forbidden", 403

    symbol = body.get("symbol")
    side = str(body.get("side", "")).upper()
    action = str(body.get("action", "")).lower()
    signal_type = body.get("signal_type")

    if not symbol or side not in ("BUY", "SELL"):
        return "missing symbol or side", 400

    # TradingView 里的 size：我们把它当成 USDT 金额
    size_usdt = body.get("size")
    if size_usdt is None:
        return "missing size", 400

    # 是否 reduce_only：
    # - 如果 payload 里明确给了 reduce_only，就直接用
    # - 否则 action == "close" 视为 reduce_only=True
    if "reduce_only" in body:
        reduce_only = bool(body["reduce_only"])
    else:
        reduce_only = action == "close"

    client_id = body.get("client_id")

    try:
        print(
            f"[WEBHOOK] sending order to Apex: "
            f"symbol={symbol} side={side} sizeUSDT={size_usdt} signal_type={signal_type}"
        )

        order = create_market_order(
            symbol=symbol,
            side=side,
            size_usdt=size_usdt,   # ★ 关键：把 size 作为 USDT 金额传进去
            reduce_only=reduce_only,
            client_id=client_id,
        )

        return jsonify({"status": "ok", "order": order}), 200

    except Exception as e:
        print("[WEBHOOK] error when sending order to Apex:", repr(e))
        return f"error: {e}", 500


if __name__ == "__main__":
    # DO/Heroku 通常会设置 PORT 环境变量
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
