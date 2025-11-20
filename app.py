import os
from flask import Flask, request, jsonify

from apex_client import create_market_order

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True) or {}
    print("[WEBHOOK] raw body:", data)

    # 1) 校验 secret
    tv_secret = data.get("secret")
    if WEBHOOK_SECRET and tv_secret != WEBHOOK_SECRET:
        print("[WEBHOOK] expected secret:", WEBHOOK_SECRET)
        return "invalid secret", 403

    # 2) 解析 TradingView 传来的字段
    symbol = data.get("symbol")          # 例如 'ZEC-USDT'
    side = (data.get("side") or "BUY").upper()
    signal_type = data.get("signal_type", "entry")  # 'entry' / 'exit'
    usdt_size = float(data.get("size", 0))          # 这里是「USDT 数量」

    print(
        "[WEBHOOK] sending order to Apex:",
        f"symbol={symbol} side={side} sizeUSDT={usdt_size} signal_type={signal_type}",
    )

    # exit 信号我们当成 reduce_only（只平不反向）
    reduce_only = signal_type == "exit"

    try:
        resp = create_market_order(
            symbol=symbol,
            side=side,
            usdt_size=usdt_size,
            reduce_only=reduce_only,
        )
        return jsonify({"status": "ok", "resp": resp}), 200
    except Exception as e:
        print("ERROR: [WEBHOOK] error when sending order to Apex:", repr(e))
        return "internal error", 500


if __name__ == "__main__":
    # 本地跑的时候用这个；DO 上会自己注入 PORT
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
