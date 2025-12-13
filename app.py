import os
import json
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from flask import Flask, request, jsonify

from apex_client import get_client
import pnl_store
from risk_manager import start_risk_thread

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
ENABLE_LIVE_TRADING = os.getenv("ENABLE_LIVE_TRADING", "true").lower() == "true"

DEFAULT_BUDGET_USDT = Decimal(os.getenv("DEFAULT_BUDGET_USDT", "15"))

# Bot direction rules:
# BOT_1~5 => LONG only
# BOT_6~10 => SHORT only
BOT_DIR_RULES: Dict[str, str] = {}
for i in range(1, 6):
    BOT_DIR_RULES[f"BOT_{i}"] = "LONG"
for i in range(6, 11):
    BOT_DIR_RULES[f"BOT_{i}"] = "SHORT"


def _json_error(msg: str, code: int = 400):
    return jsonify({"ok": False, "error": msg}), code


def _parse_json_body() -> Dict[str, Any]:
    """
    更强健的 JSON 解析：能把导致 400 的原因打印出来。
    """
    raw = request.get_data(cache=False, as_text=True) or ""
    if not raw.strip():
        raise ValueError("empty body")

    # try flask json
    try:
        obj = request.get_json(force=True, silent=False)
        if isinstance(obj, dict):
            return obj
        raise ValueError("json is not an object")
    except Exception:
        # fallback manual json
        try:
            obj2 = json.loads(raw)
            if isinstance(obj2, dict):
                return obj2
            raise ValueError("json is not an object")
        except Exception as e:
            raise ValueError(f"invalid json: {e}")


def _infer_action(payload: Dict[str, Any]) -> str:
    a = (payload.get("action") or payload.get("signal_type") or "").strip().lower()
    if a in ("open", "entry", "enter", "buy", "sell"):
        return "open"
    if a in ("close", "exit", "reduce"):
        return "close"
    return ""


def _infer_direction(payload: Dict[str, Any]) -> str:
    d = (payload.get("direction") or "").strip().upper()
    if d in ("LONG", "SHORT"):
        return d

    side = (payload.get("side") or "").strip().upper()
    if side == "BUY":
        return "LONG"
    if side == "SELL":
        return "SHORT"
    return ""


def _infer_open_side(direction: str, payload: Dict[str, Any]) -> str:
    side = (payload.get("side") or "").strip().upper()
    if side in ("BUY", "SELL"):
        return side
    return "BUY" if direction == "LONG" else "SELL"


def _infer_close_side(direction: str) -> str:
    return "SELL" if direction == "LONG" else "BUY"


def _get_size_from_payload(symbol: str, direction: str, payload: Dict[str, Any]) -> Decimal:
    """
    支持两种：
    - size: 直接合约数量
    - position_size_usdt / budget: 用 USDT 预算换算成 qty
    """
    client = get_client()

    if payload.get("size") is not None:
        try:
            return Decimal(str(payload["size"]))
        except Exception:
            return Decimal("0")

    budget = payload.get("position_size_usdt") or payload.get("budget") or None
    if budget is None:
        budget = DEFAULT_BUDGET_USDT

    try:
        budget_d = Decimal(str(budget))
    except Exception:
        budget_d = DEFAULT_BUDGET_USDT

    # 用盘口估算 qty
    bid, ask = client.get_best_bid_ask(symbol)
    px = bid if direction == "LONG" else ask
    if px is None or px <= 0:
        last = client.get_last_price(symbol)
        if last is None or last <= 0:
            return Decimal("0")
        px = last

    qty = (budget_d / Decimal(str(px)))
    return qty


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.post("/webhook")
def tv_webhook():
    # parse
    try:
        payload = _parse_json_body()
    except Exception as e:
        print(f"[WEBHOOK] invalid json: {e}")
        return _json_error(f"invalid json: {e}", 400)

    # secret
    secret = str(payload.get("secret") or "").strip()
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        print(f"[WEBHOOK] forbidden: secret mismatch, got='{secret}'")
        return _json_error("forbidden", 401)

    bot_id = str(payload.get("bot_id") or "").strip()
    symbol = str(payload.get("symbol") or "").strip()
    if not bot_id or not symbol:
        print(f"[WEBHOOK] missing bot_id/symbol payload={payload}")
        return _json_error("missing bot_id or symbol", 400)

    action = _infer_action(payload)
    if not action:
        print(f"[WEBHOOK] missing action/signal_type payload={payload}")
        return _json_error("missing action/signal_type", 400)

    direction = _infer_direction(payload)
    if not direction:
        print(f"[WEBHOOK] missing direction/side payload={payload}")
        return _json_error("missing direction or side", 400)

    # enforce bot rule (only for listed bots)
    rule = BOT_DIR_RULES.get(bot_id)
    if rule and rule != direction:
        print(f"[WEBHOOK] ignored by bot rule bot={bot_id} rule={rule} got={direction}")
        return jsonify({"ok": True, "ignored": True, "reason": "bot_direction_rule"}), 200

    client = get_client()

    if action == "open":
        side = _infer_open_side(direction, payload)
        qty = _get_size_from_payload(symbol, direction, payload)
        qty = client.snap_qty(symbol, qty)

        if qty <= 0:
            print(f"[ENTRY] qty compute error bot={bot_id} symbol={symbol} payload={payload}")
            return _json_error("qty compute error", 400)

        client_oid = str(payload.get("client_id") or f"tv-{bot_id}-{int(time.time())}")
        print(f"[ENTRY] bot={bot_id} symbol={symbol} dir={direction} side={side} qty={qty} oid={client_oid}")

        if not ENABLE_LIVE_TRADING:
            pnl_store.set_position(bot_id, symbol, {
                "direction": direction,
                "qty": str(qty),
                "entry_price": "0",
                "lock_level_pct": "0",
                "is_closing": False,
            })
            return jsonify({"ok": True, "paper": True}), 200

        try:
            resp = client.create_market_order(symbol=symbol, side=side, size=qty, reduce_only=False, client_order_id=client_oid)
            data = resp.get("data") or resp
            order_id = str(data.get("id") or data.get("orderId") or "")

            fill = client.get_fill_summary(symbol=symbol, order_id=order_id, max_wait_sec=3.0, poll_interval=0.2)
            print(f"[ENTRY] order_id={order_id} fill={fill}")

            entry_px = str(fill.get("avg_price") or "0")
            filled_qty = str(fill.get("filled_qty") or str(qty))

            pnl_store.set_position(bot_id, symbol, {
                "direction": direction,
                "qty": filled_qty,
                "entry_price": entry_px,
                "lock_level_pct": "0",
                "is_closing": False,
            })

            return jsonify({"ok": True, "order_id": order_id, "fill": fill}), 200

        except Exception as e:
            print(f"[ENTRY] create_market_order error: {e}")
            return _json_error(f"create_market_order error: {e}", 500)

    # close
    pos = pnl_store.get_position(bot_id, symbol)
    close_side = _infer_close_side(direction)

    # qty priority: payload.size > local store > remote account
    qty = Decimal("0")
    if payload.get("size") is not None:
        try:
            qty = Decimal(str(payload["size"]))
        except Exception:
            qty = Decimal("0")
    if qty <= 0 and pos:
        try:
            qty = Decimal(str(pos.get("qty") or "0"))
        except Exception:
            qty = Decimal("0")
    if qty <= 0:
        qty = client.get_open_position_size(symbol, direction)

    qty = client.snap_qty(symbol, qty)
    if qty <= 0:
        print(f"[EXIT] bot={bot_id} symbol={symbol}: no position qty to close")
        return jsonify({"ok": True, "closed": False, "reason": "no_position"}), 200

    client_oid = str(payload.get("client_id") or f"tv-close-{bot_id}-{int(time.time())}")
    print(f"[EXIT] bot={bot_id} symbol={symbol} dir={direction} side={close_side} qty={qty} oid={client_oid}")

    if not ENABLE_LIVE_TRADING:
        pnl_store.clear_position(bot_id, symbol)
        return jsonify({"ok": True, "paper": True}), 200

    try:
        pnl_store.mark_closing(bot_id, symbol, True)
        resp = client.create_market_order(symbol=symbol, side=close_side, size=qty, reduce_only=True, client_order_id=client_oid)
        data = resp.get("data") or resp
        order_id = str(data.get("id") or data.get("orderId") or "")

        fill = client.get_fill_summary(symbol=symbol, order_id=order_id, max_wait_sec=3.0, poll_interval=0.2)
        print(f"[EXIT] order_id={order_id} fill={fill}")

        pnl_store.clear_position(bot_id, symbol)
        return jsonify({"ok": True, "order_id": order_id, "fill": fill}), 200

    except Exception as e:
        pnl_store.mark_closing(bot_id, symbol, False)
        print(f"[EXIT] create_market_order error: {e}")
        return _json_error(f"close error: {e}", 500)


if __name__ == "__main__":
    # DO App Platform 生产环境请用 gunicorn，不建议 python app.py
    start_risk_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
else:
    # gunicorn import app:app 时也启动风控线程
    start_risk_thread()
