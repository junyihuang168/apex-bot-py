import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_reference_price,
    get_fill_summary,
    get_open_position_for_symbol,
    _get_symbol_rules,
    _snap_quantity,
    start_private_ws,
    start_order_rest_poller,
    pop_fill_event,
    pop_order_event,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
    clear_lock_level_pct,
    is_signal_processed,
    mark_signal_processed,
    set_protective_orders,
    get_protective_orders,
    clear_protective_orders,
)

app = Flask(__name__)

# ----------------------------
# Env / config
# ----------------------------
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

ENABLE_WS = str(os.getenv("ENABLE_WS", "1")).strip() in ("1", "true", "True", "YES", "yes")
ENABLE_REST_BACKUP = str(os.getenv("ENABLE_REST_BACKUP", "1")).strip() in ("1", "true", "True", "YES", "yes")

# If you want a clear separation:
# - web process can have ENABLE_WS=0
# - worker process should have ENABLE_WS=1
#
# Real execution price must still come from fills (WS + REST fallback).

# ----------------------------
# Bot groups（按你要求）
# ----------------------------

_ALLOWED_LONG_TPSL = {f"BOT_{i}" for i in range(1, 6)}
_ALLOWED_SHORT_TPSL = {f"BOT_{i}" for i in range(11, 16)}

# “TPSL bots” here means: bots that are allowed to have bot-side SL/TP logic enabled.
# (In this repo, the only bot-side SL/TP is the ladder stop loop below.)
def _parse_bot_list(s: str) -> Set[str]:
    out = set()
    for it in (s or "").split(","):
        it = it.strip()
        if not it:
            continue
        out.add(it.upper())
    return out


LONG_TPSL_BOTS = _parse_bot_list(os.getenv("LONG_TPSL_BOTS", ",".join(sorted(_ALLOWED_LONG_TPSL)))) & _ALLOWED_LONG_TPSL
SHORT_TPSL_BOTS = _parse_bot_list(os.getenv("SHORT_TPSL_BOTS", ",".join(sorted(_ALLOWED_SHORT_TPSL)))) & _ALLOWED_SHORT_TPSL

# “PNL only” means: no bot-side SL/TP; only record real fills and follow strategy exit signals.
LONG_PNL_ONLY_BOTS = _parse_bot_list(
    os.getenv(
        "LONG_PNL_ONLY_BOTS",
        ",".join([*(f"BOT_{i}" for i in range(6, 11)), *(f"BOT_{i}" for i in range(21, 31))]),
    )
) - _ALLOWED_LONG_TPSL

SHORT_PNL_ONLY_BOTS = _parse_bot_list(
    os.getenv(
        "SHORT_PNL_ONLY_BOTS",
        ",".join([*(f"BOT_{i}" for i in range(16, 21)), *(f"BOT_{i}" for i in range(31, 41))]),
    )
) - _ALLOWED_SHORT_TPSL


# ----------------------------
# ✅ Ladder Stop (bot-side only; no exchange protective orders)
# BOT1-5: LONG ladder
# BOT11-15: SHORT ladder
# ----------------------------

LADDER_LONG_BOTS = _parse_bot_list(os.getenv("LADDER_LONG_BOTS", ",".join(sorted(_ALLOWED_LONG_TPSL)))) & _ALLOWED_LONG_TPSL
LADDER_SHORT_BOTS = _parse_bot_list(os.getenv("LADDER_SHORT_BOTS", ",".join(sorted(_ALLOWED_SHORT_TPSL)))) & _ALLOWED_SHORT_TPSL

RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.0"))

# Base ladder config (example)
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.6"))  # %
# Ladder levels: profit% -> lock% (move stop to)
LADDER_LEVELS = [
    (Decimal("0.15"), Decimal("0.125")),
    (Decimal("0.35"), Decimal("0.15")),
    (Decimal("0.45"), Decimal("0.25")),
    (Decimal("0.55"), Decimal("0.35")),
]

# ----------------------------
# Helpers
# ----------------------------

def _d(x, default: Decimal = Decimal("0")) -> Decimal:
    if x is None:
        return default
    try:
        return Decimal(str(x))
    except Exception:
        return default


def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    # Reference price for sizing: mark -> oracle -> index -> last (see apex_client.get_reference_price)
    ref_price_dec = get_reference_price(symbol)
    if ref_price_dec is None or ref_price_dec <= 0:
        raise ValueError(f"invalid reference price for qty compute: symbol={symbol} side={side} price={ref_price_dec}")

    theoretical_qty = budget / ref_price_dec
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    if snapped_qty <= 0:
        raise ValueError(f"snapped_qty <= 0, symbol={symbol}, budget={budget}, ref_price={ref_price_dec}")

    # Sanity: ensure min_qty
    if snapped_qty < Decimal(str(min_qty)):
        snapped_qty = Decimal(str(min_qty))

    return snapped_qty


def _order_status_and_reason(order: dict):
    data = (order or {}).get("data", {}) or {}
    status = str(data.get("status", "")).upper()
    cancel_reason = str(
        data.get("cancelReason")
        or data.get("rejectReason")
        or data.get("errorMessage")
        or ""
    )
    return status, cancel_reason


# ----------------------------
# Startup
# ----------------------------
init_db()

if ENABLE_REST_BACKUP:
    start_order_rest_poller()
    print("[SYSTEM] REST order poller enabled (backup path)")

if ENABLE_WS:
    start_private_ws()
    print("[SYSTEM] WS enabled in this process (ENABLE_WS=1)")
else:
    print("[SYSTEM] WS disabled in this process (ENABLE_WS=0)")


# ----------------------------
# Webhook endpoint
# ----------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(force=True, silent=True) or {}
    print(f"[WEBHOOK] raw body: {body}")

    secret = str(body.get("secret", "")).strip()
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "bad secret"}), 401

    action = str(body.get("action", "")).strip().lower()  # entry / exit / close / open
    side = str(body.get("side", "")).strip().upper()      # BUY / SELL
    bot_id = str(body.get("bot_id", "")).strip().upper()
    symbol = str(body.get("symbol", "")).strip().upper()

    order_type = str(body.get("order_type", body.get("type", "market"))).strip().lower()  # market
    size_usdt = _d(body.get("size_usdt", body.get("size", "0")), Decimal("0"))
    reduce_only = bool(body.get("reduce_only", False))

    if not bot_id or not symbol or not side or size_usdt <= 0:
        return jsonify({"ok": False, "error": "missing fields"}), 400

    # Idempotency (optional)
    signal_id = str(body.get("client_id", body.get("signal_id", "")) or "").strip()
    if signal_id:
        if is_signal_processed(signal_id):
            return jsonify({"ok": True, "dup": True, "signal_id": signal_id}), 200
        mark_signal_processed(signal_id)

    try:
        # ENTRY path
        if action in ("entry", "open"):
            if order_type != "market":
                return jsonify({"ok": False, "error": "only market supported"}), 400

            qty = _compute_entry_qty(symbol, side, size_usdt)

            # Place order (market). Price bound is computed inside apex_client using reference price + buffer.
            res = create_market_order(
                symbol=symbol,
                side=side,
                size=str(qty),
                reduce_only=reduce_only,
                client_id=signal_id or None,
            )

            # Record "intent" entry; real fill price will be updated by WS/REST fill capture
            record_entry(bot_id=bot_id, symbol=symbol, side=side, qty=str(qty), client_id=signal_id or "")

            return jsonify({"ok": True, "order": res}), 200

        # EXIT path
        if action in ("exit", "close"):
            # On exit, we rely on your TV logic to send reduce_only close orders.
            qty = _compute_entry_qty(symbol, side, size_usdt)

            res = create_market_order(
                symbol=symbol,
                side=side,
                size=str(qty),
                reduce_only=True,
                client_id=signal_id or None,
            )

            # PnL finalization is done when fills arrive; here we record an exit intent
            record_exit_fifo(bot_id=bot_id, symbol=symbol, side=side, qty=str(qty), client_id=signal_id or "")

            return jsonify({"ok": True, "order": res}), 200

        return jsonify({"ok": False, "error": f"unknown action: {action}"}), 400

    except Exception as e:
        # Bubble the exact error to logs
        print(f"[WEBHOOK] error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ----------------------------
# Monitor thread: consume WS + REST poll events
# ----------------------------
def _monitor_loop():
    while True:
        try:
            # WS fill/order events (from apex_client private stream)
            evt = pop_fill_event()
            if evt:
                # You can parse & update pnl_store here depending on your WS payload format.
                # For now we keep the lightweight pipeline: pnl_store uses get_fill_summary (REST) as fallback
                pass

            # REST poll events (orders list)
            od = pop_order_event()
            if od:
                # Optional: handle order status updates
                pass

        except Exception:
            pass

        time.sleep(0.2)


_monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
_monitor_thread.start()
print("[worker] monitor thread started")


# ----------------------------
# Simple status endpoints
# ----------------------------
@app.route("/", methods=["GET"])
def home():
    return Response("ok", mimetype="text/plain")


@app.route("/bots", methods=["GET"])
def bots():
    return jsonify({"bots": list_bots_with_activity()}), 200


@app.route("/summary/<bot_id>", methods=["GET"])
def summary(bot_id: str):
    return jsonify(get_bot_summary(bot_id.upper())), 200


@app.route("/positions/<bot_id>", methods=["GET"])
def positions(bot_id: str):
    return jsonify(get_bot_open_positions(bot_id.upper())), 200


if __name__ == "__main__":
    # local only
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
