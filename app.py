import os
from decimal import Decimal
from flask import Flask, request, jsonify

# 直接复用你现有的 apex_client.py 里的函数
from apex_client import (
    create_market_order,
    get_market_price,
    _get_symbol_rules,
    _snap_quantity,
)

app = Flask(__name__)

# TradingView 里要带的 secret，用来校验
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ------------------------------------------------
# 记住「每个 bot 在每个 symbol 上自己开的那一份仓位」
# key:  (bot_id, symbol) -> value: {"side": "BUY"/"SELL", "qty": Decimal}
# ------------------------------------------------
BOT_POSITIONS: dict[tuple[str, str], dict] = {}


# ------------------------------------------------
# 小工具：从 TV payload 里取 USDT 预算
# ------------------------------------------------
def _extract_budget_usdt(body: dict) -> Decimal:
    """
    从 payload 中提取 USDT 预算：
    优先级：position_size_usdt > size_usdt > size
    """
    size_field = (
        body.get("position_size_usdt")
        or body.get("size_usdt")
        or body.get("size")
    )

    if size_field is None:
        raise ValueError("missing position_size_usdt / size_usdt / size")

    budget = Decimal(str(size_field))
    if budget <= 0:
        raise ValueError("size_usdt must be > 0")

    return budget


# ------------------------------------------------
# 小工具：根据 USDT 预算算出撮合后的数量（和 apex_client 逻辑保持一致）
# ------------------------------------------------
def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    """
    用和 apex_client.create_market_order(size_usdt=...) 相同的方式，
    根据 USDT 预算算出 snapped_qty（合法数量）。
    """
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    # 先用最小数量问一次 worst price，当作当前市价参考
    ref_price_dec = Decimal(get_market_price(symbol, side, str(min_qty)))

    # 理论数量 = 预算 / 价格
    theoretical_qty = budget / ref_price_dec

    # 对齐到交易所允许的网格（0.01, 0.02, ...）
    snapped_qty = _snap_quantity(symbol, theoretical_qty)
    return snapped_qty


@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    # ---------------------------
    # 1. 解析 JSON & 校验 secret
    # ---------------------------
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    if WEBHOOK_SECRET:
        if body.get("secret") != WEBHOOK_SECRET:
            print("[WEBHOOK] invalid secret")
            return "forbidden", 403

    symbol = body.get("symbol")
    if not symbol:
        return "missing symbol", 400

    bot_id = str(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal_type_raw = str(body.get("signal_type", "")).lower()
    action_raw = str(body.get("action", "")).lower()
    tv_client_id = body.get("client_id")

    # ---------------------------
    # 2. 判定是 ENTRY 还是 EXIT
    # ---------------------------
    mode: str | None = None

    # 明确写 entry / exit 的优先
    if signal_type_raw in ("entry", "open"):
        mode = "entry"
    elif signal_type_raw.startswith("exit"):
        mode = "exit"
    else:
        # 兜底兼容一下 action 字段
        if action_raw in ("open", "entry"):
            mode = "entry"
        elif action_raw in ("close", "exit"):
            mode = "exit"

    if mode is None:
        return "missing or invalid signal_type / action", 400

    # ---------------------------
    # 3. ENTRY：开仓
    # ---------------------------
    if mode == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        try:
            budget = _extract_budget_usdt(body)
        except Exception as e:
            print("[ENTRY] budget error:", e)
            return str(e), 400

        # 计算这次应该下多少币（和 apex_client 规则一致）
        try:
            snapped_qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            print("[ENTRY] qty compute error:", e)
            return "qty compute error", 500

        if snapped_qty <= 0:
            return "snapped qty <= 0", 500

        size_str = str(snapped_qty)
        print(
            f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} "
            f"budget={budget} -> qty={size_str}"
        )

        # 真正下单：这里直接用「size」模式（不再传 size_usdt），
        # 因为数量已经算好了
        try:
            order = create_market_order(
                symbol=symbol,
                side=side_raw,
                size=size_str,
                reduce_only=False,
                client_id=tv_client_id,
            )
        except Exception as e:
            print("[ENTRY] create_market_order error:", e)
            return "order error", 500

        # 记录这个 bot 在这个 symbol 上自己的仓位
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if pos and pos["side"] == side_raw:
            # 同方向加仓：数量累加
            new_qty = pos["qty"] + snapped_qty
            BOT_POSITIONS[key] = {"side": side_raw, "qty": new_qty}
        else:
            # 第一次开仓，或者之前是反方向，简单起见直接覆盖
            BOT_POSITIONS[key] = {"side": side_raw, "qty": snapped_qty}

        print(f"[ENTRY] BOT_POSITIONS[{key}] = {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(BOT_POSITIONS[key]["qty"]),
            "raw_order": order,
        }), 200

    # ---------------------------
    # 4. EXIT：结构出场 / 任意 exit_xxx
    # ---------------------------
    if mode == "exit":
        key = (bot_id, symbol)
        pos = BOT_POSITIONS.get(key)

        if not pos or pos["qty"] <= 0:
            print(f"[EXIT] bot={bot_id} symbol={symbol}: no position to close")
            # 返回 200，避免 TV 一直重发
            return jsonify({"status": "no_position"}), 200

        entry_side = pos["side"]          # 原来是 BUY 还是 SELL
        qty = pos["qty"]                  # 这只 bot 自己记录的数量
        exit_side = "SELL" if entry_side == "BUY" else "BUY"

        print(
            f"[EXIT] bot={bot_id} symbol={symbol} entry_side={entry_side} "
            f"qty={qty} -> exit_side={exit_side}"
        )

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty),
                reduce_only=True,   # 只减仓，不开反向新仓
                client_id=tv_client_id,
            )
        except Exception as e:
            print("[EXIT] create_market_order error:", e)
            return "order error", 500

        # 一次 exit 直接把这个 bot 的这份仓清零
        BOT_POSITIONS[key] = {"side": entry_side, "qty": Decimal("0")}
        print(f"[EXIT] BOT_POSITIONS[{key}] -> {BOT_POSITIONS[key]}")

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(qty),
            "raw_order": order,
        }), 200

    # 理论上不会走到这里
    return "unsupported mode", 400


if __name__ == "__main__":
    # DO / Render 等平台一般会注入 PORT 环境变量
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
