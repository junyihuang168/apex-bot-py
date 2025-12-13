import os
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from flask import Flask, jsonify, request

from apex_client import get_client
import pnl_store

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

TP_PCT = Decimal(os.getenv("TP_PCT", "0.60"))       # 默认 0.60%
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.50"))  # 默认 0.50%

# ladder: 触发阈值 -> 锁定收益阈值（百分比）
# 例：到 +0.15% 就把止损抬到 +0.10%
# 你可以按你最新规则改这里；也可做成 env 字符串解析（你要我再加也行）
LADDER = [
    (Decimal("0.15"), Decimal("0.10")),
    (Decimal("0.30"), Decimal("0.20")),
    (Decimal("0.45"), Decimal("0.30")),
    (Decimal("0.60"), Decimal("0.40")),
]

def _d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def bot_forced_direction(bot_id: int) -> Optional[str]:
    if 1 <= bot_id <= 5:
        return "LONG"
    if 6 <= bot_id <= 10:
        return "SHORT"
    return None


def entry_side_for(direction: str) -> str:
    return "BUY" if direction.upper() == "LONG" else "SELL"


def exit_side_for(direction: str) -> str:
    return "SELL" if direction.upper() == "LONG" else "BUY"


def compute_sl_tp(entry_price: Decimal, direction: str) -> Dict[str, str]:
    """
    SL/TP 全部基于“交易所真实成交均价 entry_price”来算。
    """
    if entry_price <= 0:
        return {"sl": "0", "tp": "0"}

    if direction.upper() == "LONG":
        sl = entry_price * (Decimal("1") - BASE_SL_PCT / Decimal("100"))
        tp = entry_price * (Decimal("1") + TP_PCT / Decimal("100"))
    else:
        sl = entry_price * (Decimal("1") + BASE_SL_PCT / Decimal("100"))
        tp = entry_price * (Decimal("1") - TP_PCT / Decimal("100"))

    return {"sl": str(sl), "tp": str(tp)}


def apply_ladder(pos: Dict[str, Any], mark: Decimal) -> None:
    """
    简化版 ladder：只更新 lock_level_pct 和 sl_price（抬止损）
    """
    direction = pos["direction"].upper()
    entry = _d(pos["entry_price"])
    if entry <= 0 or mark <= 0:
        return

    # profit pct
    if direction == "LONG":
        profit_pct = (mark - entry) / entry * Decimal("100")
    else:
        profit_pct = (entry - mark) / entry * Decimal("100")

    cur_lock = _d(pos.get("lock_level_pct", "0"))
    best_lock = cur_lock

    for trig, lock in LADDER:
        if profit_pct >= trig and lock > best_lock:
            best_lock = lock

    if best_lock > cur_lock:
        # 抬 SL 到 “锁定收益价”
        if direction == "LONG":
            new_sl = entry * (Decimal("1") + best_lock / Decimal("100"))
        else:
            new_sl = entry * (Decimal("1") - best_lock / Decimal("100"))

        pnl_store.update_position(pos["bot_id"], pos["symbol"], {
            "lock_level_pct": str(best_lock),
            "sl_price": str(new_sl),
            "max_profit_pct": str(max(_d(pos.get("max_profit_pct", "0")), profit_pct)),
        })


def should_stop(pos: Dict[str, Any], mark: Decimal) -> bool:
    direction = pos["direction"].upper()
    sl = _d(pos.get("sl_price", "0"))
    tp = _d(pos.get("tp_price", "0"))
    if mark <= 0:
        return False

    if direction == "LONG":
        if sl > 0 and mark <= sl:
            return True
        if tp > 0 and mark >= tp:
            return True
    else:
        if sl > 0 and mark >= sl:
            return True
        if tp > 0 and mark <= tp:
            return True
    return False


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": int(time.time())})


@app.get("/dashboard")
def dashboard():
    return jsonify(pnl_store.list_positions())


@app.get("/api/pnl")
def api_pnl():
    return jsonify(pnl_store.list_positions())


@app.post("/webhook")
def webhook():
    body = request.get_json(silent=True) or {}
    secret = str(body.get("secret", ""))
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "bad secret"}), 403

    bot_id = int(body.get("bot_id", 0))
    action = str(body.get("action", "")).lower()   # open / close
    symbol = str(body.get("symbol", "")).strip()

    if bot_id <= 0 or not symbol or action not in ("open", "close"):
        return jsonify({"ok": False, "error": "bad payload"}), 400

    client = get_client()

    forced = bot_forced_direction(bot_id)

    if action == "open":
        direction = forced or str(body.get("direction", "")).upper() or "LONG"
        if direction not in ("LONG", "SHORT"):
            direction = "LONG"

        side = entry_side_for(direction)

        # 你 TV payload 里经常 size=15（像是 USDT 预算），这里按“预算/现价”换算 qty
        budget = _d(body.get("size", "0"))
        if budget <= 0:
            return jsonify({"ok": False, "error": "size must be >0"}), 400

        mark = client.get_last_price(symbol) or Decimal("0")
        if mark <= 0:
            # 再试 depth
            bid, ask = client.get_best_bid_ask(symbol)
            mark = (ask if side == "BUY" else bid) or Decimal("0")

        if mark <= 0:
            return jsonify({"ok": False, "error": "price unavailable"}), 500

        qty = budget / mark
        qty = client.snap_qty(symbol, qty)
        if qty <= 0:
            return jsonify({"ok": False, "error": "qty too small after snap"}), 400

        resp = client.create_market_order(
            symbol=symbol,
            side=side,
            size=qty,
            reduce_only=False,
            client_order_id=str(body.get("client_id") or ""),
        )

        data = resp.get("data") or resp
        order_id = str(data.get("id") or "")

        fill = {"ok": False}
        if order_id:
            fill = client.get_fill_summary(symbol=symbol, order_id=order_id, max_wait_sec=3.0)

        entry_price = _d(fill.get("avg_price", "0"))
        if entry_price <= 0:
            # 最后兜底：用下单时 mark（但你要求“尽量用交易所成交价”，这里只是兜底）
            entry_price = mark

        sltp = compute_sl_tp(entry_price, direction)
        pnl_store.set_position(
            bot_id=str(bot_id),
            symbol=symbol,
            direction=direction,
            qty=str(qty),
            entry_price=str(entry_price),
            sl_price=sltp["sl"],
            tp_price=sltp["tp"],
        )

        return jsonify({
            "ok": True,
            "action": "open",
            "bot_id": bot_id,
            "symbol": symbol,
            "direction": direction,
            "qty": str(qty),
            "order_id": order_id,
            "fill": fill,
        })

    # close
    pos = pnl_store.get_position(str(bot_id), symbol)
    if not pos:
        return jsonify({"ok": False, "error": "no local position"}), 200

    direction = pos["direction"].upper()
    side = exit_side_for(direction)
    qty = _d(pos.get("qty", "0"))
    if qty <= 0:
        pnl_store.clear_position(str(bot_id), symbol)
        return jsonify({"ok": True, "action": "close", "note": "qty=0 cleared"}), 200

    resp = client.create_market_order(
        symbol=symbol,
        side=side,
        size=qty,
        reduce_only=True,
        client_order_id=str(body.get("client_id") or ""),
    )

    pnl_store.clear_position(str(bot_id), symbol)

    return jsonify({
        "ok": True,
        "action": "close",
        "bot_id": bot_id,
        "symbol": symbol,
        "direction": direction,
        "order": resp,
    })


def _risk_loop():
    """
    简化风控线程：轮询本地仓位 -> 更新 ladder -> 触发 SL/TP 自动平仓
    """
    client = None
    while True:
        try:
            if client is None:
                client = get_client()

            state = pnl_store.list_positions()
            positions = list((state.get("positions") or {}).values())

            for pos in positions:
                symbol = pos["symbol"]
                bot_id = pos["bot_id"]
                direction = pos["direction"].upper()

                mark = client.get_last_price(symbol) or Decimal("0")
                if mark <= 0:
                    bid, ask = client.get_best_bid_ask(symbol)
                    mark = (bid if direction == "LONG" else ask) or Decimal("0")

                if mark <= 0:
                    continue

                apply_ladder(pos, mark)

                # reload (可能刚被 ladder 更新过)
                pos2 = pnl_store.get_position(bot_id, symbol)
                if not pos2:
                    continue

                if should_stop(pos2, mark):
                    side = exit_side_for(direction)
                    qty = _d(pos2.get("qty", "0"))
                    if qty > 0:
                        client.create_market_order(
                            symbol=symbol,
                            side=side,
                            size=qty,
                            reduce_only=True,
                            client_order_id=f"risk-{bot_id}-{int(time.time())}",
                        )
                    pnl_store.clear_position(bot_id, symbol)

        except Exception as e:
            # 不让线程死；你要更详细日志我再加
            print(f"[RISK] loop error: {e}")

        time.sleep(float(os.getenv("RISK_POLL_SEC", "0.5")))


# 启动风控线程
import threading
threading.Thread(target=_risk_loop, daemon=True).start()
print("[RISK] ladder/baseSL thread started")


if __name__ == "__main__":
    # 本地调试用；DO 上用 gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
