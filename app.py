import os
from decimal import Decimal, ROUND_DOWN

from flask import Flask, request, jsonify

# 直接复用你现有的 apex_client.py 里的函数
from apex_client import (
    create_market_order,
    get_market_price,
    create_market_order_with_tpsl,
)

app = Flask(__name__)

# TradingView 里要带的 secret，用来校验
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ------------------------------------------------
# 撮合规则（默认：最小 0.01，步长 0.01，小数 2 位）
# 现在真正生效的是 apex_client 里的 DEFAULT_SYMBOL_RULES，
# 这里主要是注释和备用（逻辑上保持一致）
# ------------------------------------------------
MIN_QTY = Decimal("0.01")
STEP_SIZE = Decimal("0.01")
QTY_DECIMALS = 2

# 记住「每个 bot 在每个 symbol 上自己开的那一份仓位」
# key:  (bot_id, symbol) -> value: {"side": "BUY"/"SELL", "qty": Decimal}
BOT_POSITIONS: dict[tuple[str, str], dict] = {}


def _snap_qty(theoretical_qty: Decimal) -> Decimal:
    """
    把理论数量对齐到交易所允许的网格：
    - 向下取整到 STEP_SIZE 的整数倍（0.01, 0.02, ...）
    - 限制为 QTY_DECIMALS 位小数
    - 小于 MIN_QTY 就认为预算太小，抛错

    注意：真正用于下单的撮合逻辑现在放在 apex_client 里，
    这里的函数主要用于日志/备用。
    """
    if theoretical_qty <= 0:
        raise ValueError("theoretical_qty must be > 0")

    steps = theoretical_qty // STEP_SIZE
    snapped = steps * STEP_SIZE

    quantum = Decimal("1").scaleb(-QTY_DECIMALS)  # 10^-2 = 0.01
    snapped = snapped.quantize(quantum, rounding=ROUND_DOWN)

    if snapped < MIN_QTY:
        raise ValueError(
            f"snapped qty {snapped} < minQty {MIN_QTY} (budget too small)"
        )

    return snapped


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
    tv_client_id = body.get("client_id")  # 只用于日志，真正给 Apex 的 ID 在 apex_client 里处理

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

    # -----------------------
    # 2. ENTRY：买入 / 开仓
    # -----------------------
    if signal_type == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        if size_field is None:
            return "missing size (USDT)", 400

        # 把 size 当作 USDT 预算
        budget = Decimal(str(size_field))
        if budget <= 0:
            return "size (USDT) must be > 0", 400

        # 这里可以简单用 _snap_qty 打个 log，方便你对比 apex_client 里的撮合
        # （真正用于下单的数量和价格完全交给 create_market_order_with_tpsl）
        try:
            # 临时用 MIN_QTY 估个理论数量，只是为了日志
            # 真正的 qty 以后从 order_info["computed"]["size"] 里拿
            # （不会用这个 snapped_qty 下单）
            # 先问一次最小数量的价格，主要也是为了看日志
            ref_price_str = get_market_price(symbol, side_raw, str(MIN_QTY))
            ref_price_dec = Decimal(ref_price_str)
            theoretical_qty = budget / ref_price_dec
            snapped_preview = _snap_qty(theoretical_qty)
            print(
                f"[ENTRY-preview] bot={bot_id} symbol={symbol} side={side_raw} "
                f"budget={budget}USDT -> theoretical qty={theoretical_qty} "
                f"preview snapped={snapped_preview} @ ref {ref_price_str}"
            )
        except Exception as e:
            print("[ENTRY-preview] error:", e)

        # ★ 真正下单（市价开仓 + 挂 TP/SL）
        order_info = create_market_order_with_tpsl(
            symbol=symbol,
            side=side_raw,
            size_usdt=str(budget),
            tp_pct=Decimal("0.019"),  # +1.9%
            sl_pct=Decimal("0.006"),  # -0.6%
            client_id=tv_client_id,
        )

        # 从返回值里拿出真实撮合后的数量，记到每个 bot 自己的仓位上
        computed = order_info.get("computed", {}) if isinstance(order_info, dict) else {}
        size_str = computed.get("size")
        if size_str is None:
            # 兜底：如果未来你把 create_market_order_with_tpsl 改了返回结构，就简单返回 ok
            print("[ENTRY] WARNING: no 'computed.size' in order_info, skip BOT_POSITIONS update")
            return jsonify({"status": "ok", "mode": "entry", "order": order_info}), 200

        qty = Decimal(str(size_str))

        print(
            f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} "
            f"final snapped qty={qty}, "
            f"entry={computed.get('entry_price')} "
            f"tp={computed.get('tp_trigger')} "
            f"sl={computed.get('sl_trigger')}"
        )

        # ⑥ 记住这个 bot 在这个 symbol 上「自己开的那一份仓位」
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if pos and pos["side"] == side_raw:
            # 同方向加仓：把数量累加
            new_qty = pos["qty"] + qty
            BOT_POSITIONS[key] = {"side": side_raw, "qty": new_qty}
        else:
            # 第一次开仓，或者之前是反方向：简单起见直接覆盖
            BOT_POSITIONS[key] = {"side": side_raw, "qty": qty}

        print(f"[ENTRY] BOT_POSITIONS[{key}] = {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "order": order_info,
            "bot_position": {
                "bot_id": bot_id,
                "symbol": symbol,
                "side": BOT_POSITIONS[key]["side"],
                "qty": str(BOT_POSITIONS[key]["qty"]),
            },
        }), 200

    # -----------------------
    # 3. EXIT：卖出 / 平仓
    # -----------------------
    if signal_type == "exit":
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if not pos or pos["qty"] <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close")
            # 返回 200，避免 TV 一直重发
            return jsonify({"status": "no_position"}), 200

        entry_side = pos["side"]      # 之前是 BUY 还是 SELL
        qty = pos["qty"]              # 之前记录的“自己开的数量（例如 0.50 ZEC）”

        # 平仓方向 = 反向
        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} entry_side={entry_side} "
            f"qty={qty} -> exit_side={exit_side}"
        )

        # 这里只卖出“自己记录的那一份数量”，用 reduce_only=True
        order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=tv_client_id,
        )

        # 简单版：一次 exit 把这个 bot 的这份仓直接清零
        BOT_POSITIONS[key] = {"side": entry_side, "qty": Decimal("0")}
        print(f"[EXIT] BOT_POSITIONS[{key}] -> {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "order": order,
        }), 200

    # 其他类型暂时不支持
    return "unsupported signal_type", 400


if __name__ == "__main__":
    # DO / Render 等平台一般会注入 PORT 环境变量
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
