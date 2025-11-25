import os
from decimal import Decimal

from flask import Flask, request, jsonify

from apex_client import (
    create_market_order,
    create_limit_order,
    cancel_order,
)

app = Flask(__name__)

# TradingView 里要带的 secret，用来校验
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ----------------- 多 bot 配置 -----------------
# 环境变量例子： TP_LIMIT_BOTS = "BOT_1,BOT_3,BOT_7"
_tp_limit_bots_env = os.getenv("TP_LIMIT_BOTS", "").strip()
TP_LIMIT_BOTS: set[str] = {
    x.strip() for x in _tp_limit_bots_env.split(",") if x.strip()
}

# 止盈百分比（2%）
TP_PCT = Decimal(os.getenv("TP_PCT", "0.02"))  # 0.02 = 2%

# 只做多？（true 表示只允许 BUY 进场）
LONG_ONLY = os.getenv("LONG_ONLY", "true").lower() in ("1", "true", "yes", "y", "on")

# 记住「每个 bot 在每个 symbol 上自己开的仓位和 TP 限价单」
# key:  (bot_id, symbol) -> value: {
#    "side": "BUY",
#    "qty": Decimal,
#    "tp": {
#         "order_id": str | None,
#         "client_order_id": str | None,
#         "price": Decimal,
#    } | None
# }
BOT_POSITIONS: dict[tuple[str, str], dict] = {}


def _decimal_str(x) -> str:
    return str(Decimal(str(x)))


@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
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
    bot_id = str(body.get("bot_id", "default"))  # 每个机器人要传一个 bot_id
    tv_client_id = body.get("client_id")  # 只用于日志

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

    key = (bot_id, symbol)

    # -----------------------
    # 2. ENTRY：买入 / 开仓
    # -----------------------
    if signal_type == "entry":
        if LONG_ONLY and side_raw != "BUY":
            print(f"[ENTRY] LONG_ONLY 模式下不允许 side={side_raw}")
            return jsonify({"status": "ignored", "reason": "long_only"}), 200

        if size_field is None:
            return "missing size (USDT)", 400

        budget = Decimal(str(size_field))
        if budget <= 0:
            return "size (USDT) must be > 0", 400

        # 简单起见：每个 bot 在同一 symbol 上最多一份仓位
        pos = BOT_POSITIONS.get(key)
        if pos and pos.get("qty", Decimal("0")) > 0:
            print(f"[ENTRY] bot={bot_id} symbol={symbol} already has position, ignore new entry")
            return jsonify({"status": "already_in_position"}), 200

        # 先用 USDT 市价开多
        order_info = create_market_order(
            symbol=symbol,
            side="BUY",
            size_usdt=str(budget),
            reduce_only=False,
            client_id=tv_client_id,
        )

        computed = order_info.get("computed", {}) if isinstance(order_info, dict) else {}
        size_str = computed.get("size")
        entry_price_str = computed.get("price")

        if size_str is None or entry_price_str is None:
            print("[ENTRY] WARNING: create_market_order 没返回 computed.size/price，直接标记仓位，不挂 TP")
            BOT_POSITIONS[key] = {
                "side": "BUY",
                "qty": Decimal("0"),
                "tp": None,
            }
            return jsonify({
                "status": "ok",
                "mode": "entry",
                "order": order_info,
                "bot_position": BOT_POSITIONS[key],
            }), 200

        qty = Decimal(str(size_str))
        entry_price = Decimal(str(entry_price_str))

        BOT_POSITIONS[key] = {
            "side": "BUY",
            "qty": qty,
            "tp": None,
        }

        print(f"[ENTRY] bot={bot_id} symbol={symbol} qty={qty} entry={entry_price}")

        # 是否这个 bot 需要挂真·限价 TP？
        use_limit_tp = bot_id in TP_LIMIT_BOTS
        tp_payload = None

        if use_limit_tp and qty > 0:
            # 计算 2% TP 价格
            tp_price = entry_price * (Decimal("1") + TP_PCT)

            # 这里直接用 Decimal -> str，quantize 在 apex_client.create_limit_order 里由交易所校验
            tp_order = create_limit_order(
                symbol=symbol,
                side="SELL",
                size=str(qty),
                price=str(tp_price),
                reduce_only=True,
                client_id=tv_client_id,
            )

            order_id = None
            client_order_id = None
            if isinstance(tp_order, dict):
                order_id = tp_order.get("order_id")
                client_order_id = tp_order.get("client_order_id")

            BOT_POSITIONS[key]["tp"] = {
                "order_id": order_id,
                "client_order_id": client_order_id,
                "price": tp_price,
            }

            tp_payload = {
                "order": tp_order,
                "tp_price": str(tp_price),
            }

            print(
                f"[ENTRY-TP] bot={bot_id} symbol={symbol} qty={qty} "
                f"tp_price={tp_price} order_id={order_id} client_order_id={client_order_id}"
            )
        else:
            print(f"[ENTRY] bot={bot_id} symbol={symbol} 不挂 limit TP (use_limit_tp={use_limit_tp})")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "order": order_info,
            "bot_position": {
                "bot_id": bot_id,
                "symbol": symbol,
                "side": BOT_POSITIONS[key]["side"],
                "qty": str(BOT_POSITIONS[key]["qty"]),
                "tp": (
                    None
                    if BOT_POSITIONS[key]["tp"] is None
                    else {
                        "order_id": BOT_POSITIONS[key]["tp"]["order_id"],
                        "client_order_id": BOT_POSITIONS[key]["tp"]["client_order_id"],
                        "price": str(BOT_POSITIONS[key]["tp"]["price"]),
                    }
                ),
            },
            "tp_order": tp_payload,
        }), 200

    # -----------------------
    # 3. EXIT：卖出 / 平仓
    # -----------------------
    if signal_type == "exit":
        pos = BOT_POSITIONS.get(key)
        if not pos or pos.get("qty", Decimal("0")) <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close")
            # 返回 200，避免 TV 一直重发
            return jsonify({"status": "no_position"}), 200

        entry_side = pos["side"]      # 之前是 BUY（目前只做多）
        qty = pos["qty"]

        # 平仓方向 = 反向
        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} entry_side={entry_side} "
            f"qty={qty} -> exit_side={exit_side}"
        )

        # 1）先取消这个 bot 在这个 symbol 上挂着的 TP LIMIT
        tp_info = pos.get("tp")
        cancel_res = None
        if tp_info:
            print(f"[EXIT] cancel TP for bot={bot_id} symbol={symbol}: {tp_info}")
            cancel_res = cancel_order(
                symbol=symbol,
                order_id=tp_info.get("order_id"),
                client_order_id=tp_info.get("client_order_id"),
            )

        # 2）再用市价单平掉自己这份仓位（reduce_only=True）
        exit_order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            size_usdt=None,
            reduce_only=True,
            client_id=tv_client_id,
        )

        # 清空这个 bot 的仓记录
        BOT_POSITIONS[key] = {
            "side": entry_side,
            "qty": Decimal("0"),
            "tp": None,
        }
        print(f"[EXIT] BOT_POSITIONS[{key}] -> {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "exit_order": exit_order,
            "cancel_tp": cancel_res,
        }), 200

    # 其他类型暂时不支持
    return "unsupported signal_type", 400


if __name__ == "__main__":
    # DO / Render 等平台一般会注入 PORT 环境变量
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
