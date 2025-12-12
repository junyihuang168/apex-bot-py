# app.py
import os
import threading
from decimal import Decimal

from flask import Flask, request, jsonify

import pnl_store
import apex_client
import risk_manager

app = Flask(__name__)

WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
PORT = int(os.getenv("PORT", "8080"))

ENABLE_LIVE_TRADING = (os.getenv("ENABLE_LIVE_TRADING") or "true").lower() in ("1", "true", "yes", "y", "on")


def _bot_group_direction(bot_id: str):
    # BOT_1~5 LONG only, BOT_6~10 SHORT only (your latest requirement)
    if bot_id in ("BOT_1", "BOT_2", "BOT_3", "BOT_4", "BOT_5"):
        return "LONG"
    if bot_id in ("BOT_6", "BOT_7", "BOT_8", "BOT_9", "BOT_10"):
        return "SHORT"
    return "ANY"


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    body = request.get_json(force=True, silent=True) or {}
    secret = str(body.get("secret") or "")
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "err": "bad secret"}), 403

    bot_id = str(body.get("bot_id") or body.get("botId") or "")
    action = str(body.get("action") or "").lower()   # open / close
    symbol = str(body.get("symbol") or "")
    side = str(body.get("side") or "").upper()       # BUY / SELL
    budget = Decimal(str(body.get("size") or "0"))   # you send "15" etc

    if not bot_id or not action or not symbol:
        return jsonify({"ok": False, "err": "missing bot_id/action/symbol"}), 400

    # Direction enforcement
    grp = _bot_group_direction(bot_id)
    if action == "open":
        if grp == "LONG" and side != "BUY":
            return jsonify({"ok": False, "err": f"{bot_id} LONG only; reject side={side}"}), 400
        if grp == "SHORT" and side != "SELL":
            return jsonify({"ok": False, "err": f"{bot_id} SHORT only; reject side={side}"}), 400

    if not ENABLE_LIVE_TRADING:
        return jsonify({"ok": True, "dry_run": True, "bot_id": bot_id}), 200

    # Compute qty by budget / mark
    mark = apex_client.get_ticker_price(symbol)
    if mark is None or mark <= 0:
        return jsonify({"ok": False, "err": f"ticker price unavailable for {symbol}"}), 500

    raw_qty = (budget / mark)
    qty = apex_client._snap_quantity(raw_qty, symbol)

    if action == "open":
        res, client_id = apex_client.create_market_order(
            symbol=symbol,
            side=side,
            size=qty,
            reduce_only=False,
        )
        order_id = (res.get("data") or {}).get("id") if isinstance(res, dict) else None

        fs = apex_client.wait_for_fill(order_id or "", client_id, fast_wait_s=3.0, slow_wait_s=60.0)
        if fs["filled_size"] <= 0 or fs["avg_price"] <= 0:
            # do not fake-fill; keep it pending rather than wrong accounting
            return jsonify({"ok": True, "pending_fill": True, "order_id": order_id, "client_id": client_id}), 200

        filled_qty = fs["filled_size"]
        fill_px = fs["avg_price"]

        pnl_store.upsert_entry(bot_id, symbol, side, filled_qty, fill_px)
        pnl_store.add_trade(bot_id, symbol, "OPEN", side, filled_qty, fill_px, None, order_id, client_id)

        return jsonify({"ok": True, "order_id": order_id, "client_id": client_id, "fill_px": str(fill_px), "filled_qty": str(filled_qty)}), 200

    if action == "close":
        pos = pnl_store.get_position(bot_id, symbol)
        if not pos:
            return jsonify({"ok": True, "msg": "no local position"}), 200

        pos_side = pos["side"]
        qty = Decimal(pos["qty"])
        exit_side = "SELL" if pos_side == "LONG" else "BUY"

        pnl_store.mark_closing(bot_id, symbol, True)

        res, client_id = apex_client.create_market_order(
            symbol=symbol,
            side=exit_side,
            size=qty,
            reduce_only=True,
        )
        order_id = (res.get("data") or {}).get("id") if isinstance(res, dict) else None

        fs = apex_client.wait_for_fill(order_id or "", client_id, fast_wait_s=3.0, slow_wait_s=60.0)
        exit_px = fs["avg_price"] if fs["avg_price"] > 0 else mark

        pnl_store.add_trade(bot_id, symbol, "CLOSE", exit_side, qty, exit_px, "TV_CLOSE", order_id, client_id)
        pnl_store.delete_position(bot_id, symbol)

        return jsonify({"ok": True, "order_id": order_id, "client_id": client_id, "exit_px": str(exit_px)}), 200

    return jsonify({"ok": False, "err": f"unknown action={action}"}), 400


def _start_threads():
    pnl_store.init_db()
    t = threading.Thread(target=risk_manager.run_loop, daemon=True)
    t.start()


_start_threads()

if __name__ == "__main__":
    # DO App Platform will set PORT; must listen on 0.0.0.0
    app.run(host="0.0.0.0", port=PORT, debug=False)
