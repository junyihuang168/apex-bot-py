# app.py

import os
from decimal import Decimal

from flask import Flask, request, jsonify

from apex_client import (
    open_with_limit_tp,
    create_market_order,
    cancel_order_by_id,
)

app = Flask(__name__)

# TradingView secret 校验
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ---------------------------
# 单 bot 状态：只记一份仓位 + 一张 TP 限价单
# ---------------------------
# LAST_POSITION: {
#   "symbol": "ZEC-USDT",
#   "side": "BUY",
#   "qty": Decimal("0.11"),
# }
LAST_POSITION: dict | None = None

# LAST_TP_ORDER: {
#   "symbol": "ZEC-USDT",
#   "order_id": "xxxxx"
# }
LAST_TP_ORDER: dict | None = None


@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    global LAST_POSITION, LAST_TP_ORDER

    # ---------------------------
    # 1. 解析 & 校验基础字段
    # ---------------------------
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    # 校验 secret（可选）
    if WEBHOOK_SECRET:
        if body.get("secret") != WEBHOOK_SECRET:
            print("[WEBHOOK] invalid secret")
            return "forbidden", 403

    symbol = body.get("symbol")
    side_raw = str(body.get("side", "")).upper()
    signal_type = str(body.get("signal_type", "")).lower()  # "entry" / "exit"
    action = str(body.get("action", "")).lower()
    size_field = body.get("size")  # 入场时的 USDT 金额
    tv_client_id = body.get("client_id")  # 只用于日志（我们自己生成真正的 apex clientId）

    if not symbol:
        return "missing symbol", 400

    # 兼容：如果没写 signal_type，用 action 来判断 open/close
    if signal_type not in ("entry", "exit"):
        if action in ("open", "entry"):
            signal_type = "entry"
        elif action in ("close", "exit"):
            signal_type = "exit"
        else:
            return "missing signal_type (entry/exit)", 400

    # ===========================
    # 2. ENTRY：市价进场 + 真·2% LIMIT TP
    # ===========================
    if signal_type == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        if size_field is None:
            return "missing size (USDT)", 400

        budget = Decimal(str(size_field))
        if budget <= 0:
            return "size (USDT) must be > 0", 400

        # 如果已经有记录的仓位，简单起见先打印 log（正常 TV 策略不会在持仓中再次 ENTRY）
        if LAST_POSITION is not None:
            print("[ENTRY] WARNING: LAST_POSITION already exists:", LAST_POSITION)

        print(
            f"[ENTRY] symbol={symbol} side={side_raw} budget={budget}USDT, "
            f"tv_client_id={tv_client_id}"
        )

        # ★ 用 apex_client 里的“真·限价 TP 函数”
        try:
            res = open_with_limit_tp(
                symbol=symbol,
                side=side_raw,
                size_usdt=str(budget),
                tp_pct=Decimal("0.02"),  # +2% LIMIT TP
                client_id=tv_client_id,
            )
        except Exception as e:
            print("[ENTRY] open_with_limit_tp error:", e)
            return jsonify({"status": "error", "msg": str(e)}), 500

        computed = res.get("computed", {}) if isinstance(res, dict) else {}
        size_str = computed.get("size")
        entry_side = computed.get("side", side_raw)
        tp_order_id = computed.get("tp_order_id")

        if size_str is None:
            print("[ENTRY] WARNING: no 'size' in computed, res:", res)
            return jsonify({"status": "ok", "mode": "entry", "order": res}), 200

        qty = Decimal(str(size_str))

        # 记录当前唯一仓位
        LAST_POSITION = {
            "symbol": symbol,
            "side": entry_side,
            "qty": qty,
        }

        print(f"[ENTRY] LAST_POSITION = {LAST_POSITION}")

        # 记录 TP 限价单（如果拿到了 order_id）
        if tp_order_id:
            LAST_TP_ORDER = {
                "symbol": symbol,
                "order_id": str(tp_order_id),
            }
            print(f"[ENTRY] LAST_TP_ORDER = {LAST_TP_ORDER}")
        else:
            LAST_TP_ORDER = None
            print("[ENTRY] WARNING: tp_order_id is None, TP cancel later will be skip")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "entry_order": res.get("entry_order"),
            "tp_order": res.get("tp_order"),
            "computed": computed,
        }), 200

    # ===========================
    # 3. EXIT：TV 出场 / 止损信号
    #    → 先撤 TP 限价单，再市价 reduce_only 平仓
    # ===========================
    if signal_type == "exit":
        print(f"[EXIT] recv exit for symbol={symbol}, tv_client_id={tv_client_id}")

        # 3.1 先尝试撤掉 2% TP 限价单
        if LAST_TP_ORDER and LAST_TP_ORDER.get("symbol") == symbol:
            tp_order_id = LAST_TP_ORDER.get("order_id")
            if tp_order_id:
                print(f"[EXIT] try cancel TP order: {tp_order_id}")
                cancel_res = cancel_order_by_id(symbol, tp_order_id)
                print("[EXIT] cancel TP result:", cancel_res)
            else:
                print("[EXIT] LAST_TP_ORDER has no order_id")
        else:
            print("[EXIT] no TP order to cancel (LAST_TP_ORDER=", LAST_TP_ORDER, ")")

        # 3.2 按记录的方向 & 数量市价平仓
        if not LAST_POSITION or LAST_POSITION.get("symbol") != symbol:
            print(f"[EXIT] no LAST_POSITION for symbol={symbol}, nothing to close")
            # 返回 200 避免 TV 一直重发
            return jsonify({"status": "no_position"}), 200

        entry_side = LAST_POSITION["side"]
        qty = LAST_POSITION["qty"]

        if qty <= 0:
            print("[EXIT] qty <= 0, skip close:", qty)
            LAST_POSITION = None
            LAST_TP_ORDER = None
            return jsonify({"status": "no_position"}), 200

        # 平仓方向 = 反向
        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] close position: symbol={symbol}, entry_side={entry_side}, "
            f"exit_side={exit_side}, qty={qty}"
        )

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty),
                size_usdt=None,
                reduce_only=True,   # ✅ 不会反向开新仓，只会减仓
                client_id=tv_client_id,
            )
        except Exception as e:
            print("[EXIT] create_market_order error:", e)
            # 即使平仓失败，状态不要乱清，方便你看日志后手动处理
            return jsonify({"status": "error", "msg": str(e)}), 500

        # 平仓成功后，把本地状态清掉
        LAST_POSITION = None
        LAST_TP_ORDER = None
        print("[EXIT] position & TP cleared")

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "order": order,
        }), 200

    # 其他类型暂不支持
    return "unsupported signal_type", 400


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
