import os
import time
import threading
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, List

from flask import Flask, request, jsonify

from apex_client import get_client
import pnl_store


app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

# Risk params (percent)
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))      # 0.5% stop
LOCK_TRIGGER_1 = Decimal(os.getenv("LOCK_TRIGGER_1", "0.15"))  # +0.15%
LOCK_LOCK_1 = Decimal(os.getenv("LOCK_LOCK_1", "0.10"))        # lock to +0.10%
LOCK_TRIGGER_2 = Decimal(os.getenv("LOCK_TRIGGER_2", "0.45"))  # +0.45%
LOCK_LOCK_2 = Decimal(os.getenv("LOCK_LOCK_2", "0.20"))        # lock to +0.20%
LOCK_STEP = Decimal(os.getenv("LOCK_STEP", "0.10"))            # +0.10% step after trigger_2

FILL_WAIT_SEC = float(os.getenv("FILL_WAIT_SEC", "3.0"))
FILL_POLL_SEC = float(os.getenv("FILL_POLL_SEC", "0.2"))

# Bot groups (NO more BOT_1~10 hard-coding in code; only defaults)
# You can override via env:
#   LONG_RISK_BOTS="BOT_1,BOT_2,BOT_3,BOT_4,BOT_5"
#   SHORT_RISK_BOTS="BOT_6,BOT_7,BOT_8,BOT_9,BOT_10"
def _parse_bot_list(s: str) -> List[str]:
    out = []
    for part in (s or "").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out


LONG_RISK_BOTS = _parse_bot_list(os.getenv("LONG_RISK_BOTS", "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5"))
SHORT_RISK_BOTS = _parse_bot_list(os.getenv("SHORT_RISK_BOTS", "BOT_6,BOT_7,BOT_8,BOT_9,BOT_10"))


# Pending fill tracker (order placed but fills not immediately available)
_PENDING_LOCK = threading.Lock()
_PENDING: Dict[Tuple[str, str, str], Dict[str, Any]] = {}  # (bot_id, symbol, order_id) -> meta


def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    """
    Your ladder rules:
      - pnl >= 0.15 => lock 0.10
      - pnl >= 0.45 => lock 0.20
      - then every +0.10 pnl => lock +0.10 (0.55->0.30, 0.65->0.40 ...)
    """
    if pnl_pct < LOCK_TRIGGER_1:
        return Decimal("0")
    if pnl_pct < LOCK_TRIGGER_2:
        return LOCK_LOCK_1

    # pnl >= 0.45
    # steps of 0.10 from 0.45
    steps = (pnl_pct - LOCK_TRIGGER_2) // LOCK_STEP
    return LOCK_LOCK_2 + (steps * LOCK_STEP)


def _direction_from_entry_side(entry_side: str) -> str:
    return "LONG" if entry_side.upper() == "BUY" else "SHORT"


def _exit_side_from_direction(direction: str) -> str:
    return "SELL" if direction.upper() == "LONG" else "BUY"


def _bot_is_long_risk(bot_id: str) -> bool:
    return bot_id in LONG_RISK_BOTS


def _bot_is_short_risk(bot_id: str) -> bool:
    return bot_id in SHORT_RISK_BOTS


def _validate_bot_side(bot_id: str, action: str, side: str) -> Optional[str]:
    """
    Enforce your current rule:
      - BOT_1~5: LONG only (entry side must be BUY)
      - BOT_6~10: SHORT only (entry side must be SELL)
    For close actions, we don't block (we try to close what's open).
    """
    if action != "open":
        return None
    if _bot_is_long_risk(bot_id) and side.upper() != "BUY":
        return f"{bot_id} is configured LONG-only but received entry side={side}"
    if _bot_is_short_risk(bot_id) and side.upper() != "SELL":
        return f"{bot_id} is configured SHORT-only but received entry side={side}"
    return None


def _compute_qty_from_budget(symbol: str, budget_usdt: Decimal, entry_side: str) -> Decimal:
    """
    Compute qty using public depth/ticker price (no worst-price).
    """
    client = get_client()
    # Use price_for_market_order as a robust "tradeable" estimate
    px = client.price_for_market_order(symbol, entry_side.upper())
    if px <= 0:
        raise RuntimeError("ticker price unavailable")
    qty = budget_usdt / px
    return client.snap_qty(symbol, qty)


def _register_pending_fill(bot_id: str, symbol: str, order_id: str, entry_side: str) -> None:
    with _PENDING_LOCK:
        _PENDING[(bot_id, symbol, order_id)] = {
            "bot_id": bot_id,
            "symbol": symbol,
            "order_id": order_id,
            "entry_side": entry_side.upper(),
            "created_at": int(time.time() * 1000),
        }


def _pending_fill_worker() -> None:
    """
    Background: keep trying to fetch fills for pending orders and record entries once available.
    """
    while True:
        time.sleep(0.5)
        to_remove = []
        with _PENDING_LOCK:
            items = list(_PENDING.items())

        if not items:
            continue

        client = get_client()
        for key, meta in items:
            bot_id = meta["bot_id"]
            symbol = meta["symbol"]
            order_id = meta["order_id"]
            entry_side = meta["entry_side"]

            fs = client.get_fill_summary(symbol=symbol, order_id=order_id, max_wait_sec=2.0, poll_interval=0.25)
            if fs.get("ok"):
                filled_qty = Decimal(fs["filled_qty"])
                avg_price = Decimal(fs["avg_price"])
                if filled_qty > 0 and avg_price > 0:
                    pnl_store.record_entry(bot_id, symbol, entry_side, filled_qty, avg_price)
                    to_remove.append(key)
                    continue

            # give up after 60s
            age_ms = int(time.time() * 1000) - int(meta["created_at"])
            if age_ms > 60000:
                to_remove.append(key)

        if to_remove:
            with _PENDING_LOCK:
                for k in to_remove:
                    _PENDING.pop(k, None)


def _risk_loop() -> None:
    """
    Risk loop for LONG_RISK_BOTS + SHORT_RISK_BOTS:
    - Base SL: -0.5%
    - Ladder lock: per your rules
    - Uses mark price derived from bid/ask/ticker
    - Exits reduce-only, then records exit at fill price (or best-effort)
    """
    client = get_client()

    while True:
        time.sleep(1.0)

        # only risk-manage these bots
        bots = list(set(LONG_RISK_BOTS + SHORT_RISK_BOTS))
        for bot_id in bots:
            positions = pnl_store.get_all_open_positions(bot_id)
            for pos in positions:
                symbol = pos["symbol"]
                entry_side = pos["entry_side"]  # BUY or SELL
                direction = _direction_from_entry_side(entry_side)

                # enforce direction scope: long-risk bots only manage LONG; short-risk bots only manage SHORT
                if _bot_is_long_risk(bot_id) and direction != "LONG":
                    continue
                if _bot_is_short_risk(bot_id) and direction != "SHORT":
                    continue

                qty = Decimal(pos["qty"])
                entry_price = Decimal(pos["avg_entry_price"])
                if qty <= 0 or entry_price <= 0:
                    continue

                mark = client.get_mark_price(symbol, direction)
                if not mark or mark <= 0:
                    continue

                # pnl% (positive means profit)
                if direction == "LONG":
                    pnl_pct = (Decimal(mark) - entry_price) / entry_price * Decimal("100")
                else:
                    pnl_pct = (entry_price - Decimal(mark)) / entry_price * Decimal("100")

                # update lock level when ladder advances
                desired_lock = _desired_lock_level_pct(pnl_pct)
                current_lock = pnl_store.get_lock_level_pct(bot_id, symbol, direction)
                if desired_lock > current_lock:
                    pnl_store.set_lock_level_pct(bot_id, symbol, direction, desired_lock)
                    current_lock = desired_lock

                # exit conditions
                stop_hit = pnl_pct <= (Decimal("0") - BASE_SL_PCT)
                lock_hit = (current_lock > 0 and pnl_pct <= current_lock)

                if not (stop_hit or lock_hit):
                    continue

                reason = "SL" if stop_hit else f"LOCK_{str(current_lock)}"

                # send reduce-only close (close the whole qty)
                exit_side = _exit_side_from_direction(direction)

                try:
                    resp = client.create_market_order(
                        symbol=symbol,
                        side=exit_side,
                        size=qty,
                        reduce_only=True,
                        client_order_id=None,
                    )
                    order_id = (resp.get("data") or resp).get("id") or (resp.get("data") or resp).get("orderId")
                    if not order_id:
                        continue

                    fs = client.get_fill_summary(symbol=symbol, order_id=str(order_id), max_wait_sec=FILL_WAIT_SEC, poll_interval=FILL_POLL_SEC)
                    exit_price = Decimal(fs["avg_price"]) if fs.get("ok") else Decimal(mark)

                    pnl_store.record_exit_fifo(
                        bot_id=bot_id,
                        symbol=symbol,
                        entry_side=entry_side,
                        exit_side=exit_side,
                        exit_qty=qty,
                        exit_price=exit_price,
                        reason=reason,
                    )
                    pnl_store.clear_lock_level_pct(bot_id, symbol, direction)

                except Exception:
                    # keep loop alive
                    continue


@app.get("/health")
def health() -> Any:
    return jsonify({"ok": True})


@app.post("/webhook")
def tv_webhook() -> Any:
    payload = request.get_json(force=True, silent=True) or {}
    secret = str(payload.get("secret") or "").strip()
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({"ok": False, "error": "bad secret"}), 403

    bot_id = str(payload.get("bot_id") or payload.get("bot") or "").strip()
    action = str(payload.get("action") or "").strip().lower()  # open / close
    symbol = str(payload.get("symbol") or "").strip()
    side = str(payload.get("side") or "").strip().upper()      # BUY / SELL
    reduce_only = bool(payload.get("reduce_only") or False)

    if not bot_id or not action or not symbol or not side:
        return jsonify({"ok": False, "error": "missing fields: bot_id/action/symbol/side"}), 400

    # enforce your bot direction for entries
    err = _validate_bot_side(bot_id, action, side)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    client = get_client()

    # OPEN
    if action == "open":
        budget = Decimal(str(payload.get("size") or payload.get("budget") or "0"))
        if budget <= 0:
            return jsonify({"ok": False, "error": "size/budget must be > 0"}), 400

        try:
            qty = _compute_qty_from_budget(symbol, budget, side)
            if qty <= 0:
                return jsonify({"ok": False, "error": "qty too small after snap"}), 400

            resp = client.create_market_order(symbol=symbol, side=side, size=qty, reduce_only=False, client_order_id=None)
            data = resp.get("data") or resp
            order_id = data.get("id") or data.get("orderId")
            if not order_id:
                return jsonify({"ok": False, "error": "order_id missing in response", "resp": resp}), 500

            # fetch fill quickly; if not available, register pending and async fix
            fs = client.get_fill_summary(symbol=symbol, order_id=str(order_id), max_wait_sec=FILL_WAIT_SEC, poll_interval=FILL_POLL_SEC)

            if fs.get("ok"):
                filled_qty = Decimal(fs["filled_qty"])
                avg_price = Decimal(fs["avg_price"])
                if filled_qty > 0 and avg_price > 0:
                    pnl_store.record_entry(bot_id, symbol, side, filled_qty, avg_price)
                else:
                    _register_pending_fill(bot_id, symbol, str(order_id), side)
            else:
                _register_pending_fill(bot_id, symbol, str(order_id), side)

            return jsonify({"ok": True, "order_id": str(order_id), "fill": fs})

        except Exception as e:
            return jsonify({"ok": False, "error": f"[ENTRY] {e}"}), 500

    # CLOSE
    if action == "close":
        # Decide which direction to close:
        # - if side=SELL => usually closing LONG
        # - if side=BUY  => usually closing SHORT
        preferred_entry_side = "BUY" if side == "SELL" else "SELL"

        # find open position in that direction first
        open_qty, avg_entry = pnl_store.get_open_position(bot_id, symbol, preferred_entry_side)
        entry_side = preferred_entry_side

        if open_qty <= 0:
            # try the other side
            other = "SELL" if preferred_entry_side == "BUY" else "BUY"
            open_qty, avg_entry = pnl_store.get_open_position(bot_id, symbol, other)
            entry_side = other

        if open_qty <= 0:
            return jsonify({"ok": True, "message": "no local open position to close"}), 200

        direction = _direction_from_entry_side(entry_side)
        exit_side = _exit_side_from_direction(direction)

        try:
            resp = client.create_market_order(symbol=symbol, side=exit_side, size=open_qty, reduce_only=True, client_order_id=None)
            data = resp.get("data") or resp
            order_id = data.get("id") or data.get("orderId")
            if not order_id:
                return jsonify({"ok": False, "error": "order_id missing in response", "resp": resp}), 500

            fs = client.get_fill_summary(symbol=symbol, order_id=str(order_id), max_wait_sec=FILL_WAIT_SEC, poll_interval=FILL_POLL_SEC)
            exit_price = Decimal(fs["avg_price"]) if fs.get("ok") else (client.get_mark_price(symbol, direction) or Decimal("0"))

            res = pnl_store.record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_side=exit_side,
                exit_qty=open_qty,
                exit_price=exit_price if exit_price > 0 else Decimal(avg_entry),
                reason=str(payload.get("reason") or "strategy_exit"),
            )

            pnl_store.clear_lock_level_pct(bot_id, symbol, direction)

            return jsonify({"ok": True, "order_id": str(order_id), "fill": fs, "pnl": res})

        except Exception as e:
            return jsonify({"ok": False, "error": f"[EXIT] {e}"}), 500

    return jsonify({"ok": False, "error": "unknown action"}), 400


@app.get("/api/pnl")
def api_pnl() -> Any:
    bots = pnl_store.list_bots_with_activity()
    out = []
    for b in bots:
        out.append(pnl_store.get_bot_summary(b))
    return jsonify({"ok": True, "bots": out})


@app.get("/dashboard")
def dashboard() -> Any:
    # minimal readable dashboard (no templates)
    bots = pnl_store.list_bots_with_activity()
    lines = []
    lines.append("<html><body><h2>PNL Dashboard</h2>")
    for b in bots:
        s = pnl_store.get_bot_summary(b)
        lines.append(f"<h3>{b}</h3>")
        lines.append(f"<div>Realized PnL: {s['realized_pnl']}</div>")
        lines.append("<ul>")
        for p in s["open_positions"]:
            lines.append(f"<li>{p['symbol']} {p['entry_side']} qty={p['qty']} avg={p['avg_entry_price']}</li>")
        lines.append("</ul>")
    lines.append("</body></html>")
    return "\n".join(lines), 200, {"Content-Type": "text/html; charset=utf-8"}


def _start_background_threads() -> None:
    pnl_store.init_db()

    t1 = threading.Thread(target=_pending_fill_worker, daemon=True)
    t1.start()

    t2 = threading.Thread(target=_risk_loop, daemon=True)
    t2.start()


_start_background_threads()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
