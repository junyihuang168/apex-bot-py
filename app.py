import os
import json
import time
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, List

from flask import Flask, request, jsonify

import apex_client
import pnl_store

app = Flask(__name__)

# ----------------------------
# Env / Config
# ----------------------------
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
PORT = int(os.getenv("PORT", "8080"))

DEFAULT_SIZE_USDT = os.getenv("DEFAULT_SIZE_USDT", "60")

# Optional: allow bots outside 1-20 (default off)
ALLOW_OTHER_BOTS = os.getenv("ALLOW_OTHER_BOTS", "false").lower() in ("1", "true", "yes", "y", "on")

# ----------------------------
# Init DB
# ----------------------------
pnl_store.init_db()


def _d(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def _log(event: str, **fields):
    rec = {"ts": int(time.time()), "event": event, **fields}
    print(json.dumps(rec, ensure_ascii=False))


# ----------------------------
# Bot profiles / Rules
# ----------------------------
def _parse_bot_number(bot_id: str) -> Optional[int]:
    s = str(bot_id).strip().lower().replace("bot", "").replace("_", "")
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def bot_profile(bot_id: str) -> Dict[str, Any]:
    """
    Required:
    - BOT_1..BOT_10 => LONG only (BUY entries)
    - BOT_11..BOT_20 => SHORT only (SELL entries)
    - No SL/TP for any bot
    """
    n = _parse_bot_number(bot_id)
    if n is None:
        return {"bot_num": None, "force_direction": "NONE"}

    if 1 <= n <= 10:
        return {"bot_num": n, "force_direction": "LONG"}
    if 11 <= n <= 20:
        return {"bot_num": n, "force_direction": "SHORT"}

    return {"bot_num": n, "force_direction": "ANY" if ALLOW_OTHER_BOTS else "NONE"}


def _direction_from_side(side: str) -> str:
    return "LONG" if str(side).upper() == "BUY" else "SHORT"


def _entry_side_for_direction(direction: str) -> str:
    return "BUY" if direction.upper() == "LONG" else "SELL"


def _exit_side_for_direction(direction: str) -> str:
    return "SELL" if direction.upper() == "LONG" else "BUY"


# ----------------------------
# Idempotency
# ----------------------------
def get_signal_id(payload: Dict[str, Any]) -> str:
    sid = payload.get("signal_id") or payload.get("alert_id") or payload.get("id")
    if sid:
        return str(sid)

    ts = payload.get("ts") or payload.get("time") or int(time.time())
    return f"auto:{ts}:{payload.get('symbol')}:{payload.get('action')}:{payload.get('bot_id')}"


# ----------------------------
# Auth
# ----------------------------
def _auth_ok() -> bool:
    if not WEBHOOK_SECRET:
        return True
    got = request.headers.get("X-WEBHOOK-SECRET") or request.args.get("secret") or ""
    return str(got) == str(WEBHOOK_SECRET)


# ----------------------------
# Single-position policy (per bot + symbol)
# ----------------------------
def _has_local_open_lot(bot_id: str, symbol: str) -> bool:
    try:
        opens = pnl_store.get_bot_open_positions(bot_id)
        for (sym, _dir), v in opens.items():
            if str(sym).upper() == str(symbol).upper() and _d(v.get("qty", 0)) > 0:
                return True
        return False
    except Exception:
        return False


# ----------------------------
# Core handlers
# ----------------------------
def handle_entry(payload: Dict[str, Any]) -> Dict[str, Any]:
    bot_id = str(payload.get("bot_id") or payload.get("bot") or "bot0")
    symbol = str(payload.get("symbol") or "").strip()
    side = str(payload.get("side") or payload.get("entry_side") or "").upper()  # BUY/SELL
    size_usdt = payload.get("size_usdt") or payload.get("usdt") or DEFAULT_SIZE_USDT

    if not symbol or side not in ("BUY", "SELL"):
        return {"ok": False, "error": "missing symbol or invalid side (BUY/SELL required)"}

    prof = bot_profile(bot_id)
    if prof["force_direction"] == "NONE":
        return {"ok": False, "error": "bot_id not allowed (only BOT_1..BOT_20 by default)"}

    direction = _direction_from_side(side)

    # enforce long-only/short-only
    if prof["force_direction"] == "LONG" and direction != "LONG":
        return {"ok": False, "error": "BOT_1..BOT_10 are LONG only (BUY only)"}
    if prof["force_direction"] == "SHORT" and direction != "SHORT":
        return {"ok": False, "error": "BOT_11..BOT_20 are SHORT only (SELL only)"}

    signal_id = get_signal_id(payload)

    # idempotency
    if pnl_store.is_signal_processed(bot_id, signal_id):
        return {"ok": True, "dedup": True, "signal_id": signal_id}

    pnl_store.mark_signal_processed(bot_id, signal_id)

    # enforce "one position per bot per symbol" (local)
    if _has_local_open_lot(bot_id, symbol):
        _log("entry_blocked_local_open", bot_id=bot_id, symbol=symbol, direction=direction, signal_id=signal_id)
        return {"ok": False, "error": "local_open_position_exists_for_bot_symbol", "signal_id": signal_id}

    _log("entry_submit", bot_id=bot_id, symbol=symbol, side=side, size_usdt=str(size_usdt), signal_id=signal_id)

    # Place order (simple, no fill polling)
    try:
        res = apex_client.create_market_order_simple(
            symbol=symbol,
            side=side,
            size_usdt=size_usdt,
            reduce_only=False,
            client_tag=f"{bot_id}:{signal_id}",
        )
    except Exception as e:
        _log("entry_order_error", bot_id=bot_id, symbol=symbol, side=side, signal_id=signal_id, err=str(e))
        return {"ok": False, "error": "order_failed", "detail": str(e), "signal_id": signal_id}

    # reference entry price (worstPrice used for placement)
    placed = res.get("placed", {})
    qty = _d(placed.get("size"))
    px = _d(placed.get("price_for_order"))

    # record entry as reference PnL baseline
    pnl_store.record_entry(
        bot_id=bot_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=px,
        reason="entry_reference_price",
    )

    _log(
        "entry_accepted",
        bot_id=bot_id,
        symbol=symbol,
        direction=direction,
        qty=str(qty),
        ref_entry=str(px),
        order_id=res.get("order_id"),
        client_order_id=res.get("client_order_id"),
        signal_id=signal_id,
    )

    return {
        "ok": True,
        "signal_id": signal_id,
        "qty": str(qty),
        "ref_entry": str(px),
        "order_id": res.get("order_id"),
        "client_order_id": res.get("client_order_id"),
    }


def handle_exit(payload: Dict[str, Any]) -> Dict[str, Any]:
    bot_id = str(payload.get("bot_id") or payload.get("bot") or "bot0")
    symbol = str(payload.get("symbol") or "").strip()

    if not symbol:
        return {"ok": False, "error": "missing symbol"}

    signal_id = get_signal_id(payload)

    if pnl_store.is_signal_processed(bot_id, signal_id):
        return {"ok": True, "dedup": True, "signal_id": signal_id}

    pnl_store.mark_signal_processed(bot_id, signal_id)

    # Determine direction: prefer exchange position if available, else payload direction
    pos = apex_client.get_open_position_for_symbol(symbol)
    if pos and pos.get("side") in ("LONG", "SHORT") and pos.get("size") and _d(pos["size"]) > 0:
        pos_side = str(pos["side"]).upper()
        qty = _d(pos["size"])
    else:
        # fallback: payload must provide direction + qty
        pos_side = str(payload.get("direction") or "").upper()
        qty = _d(payload.get("qty") or payload.get("size") or "0")
        if pos_side not in ("LONG", "SHORT") or qty <= 0:
            _log("exit_need_direction_qty", bot_id=bot_id, symbol=symbol, signal_id=signal_id, pos=pos)
            return {"ok": False, "error": "need_exchange_position_or_payload_direction_and_qty", "signal_id": signal_id}

    exit_side = _exit_side_for_direction(pos_side)
    entry_side = _entry_side_for_direction(pos_side)

    _log("exit_submit", bot_id=bot_id, symbol=symbol, direction=pos_side, qty=str(qty), signal_id=signal_id)

    # Place reduce-only close (simple)
    try:
        res = apex_client.create_market_order_simple(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_tag=f"{bot_id}:{signal_id}:EXIT",
        )
    except Exception as e:
        _log("exit_order_error", bot_id=bot_id, symbol=symbol, direction=pos_side, signal_id=signal_id, err=str(e))
        return {"ok": False, "error": "order_failed", "detail": str(e), "signal_id": signal_id}

    placed = res.get("placed", {})
    exit_qty = _d(placed.get("size"))
    exit_px = _d(placed.get("price_for_order"))

    # record exit as reference PnL
    pnl_store.record_exit_fifo(
        bot_id=bot_id,
        symbol=symbol,
        entry_side=entry_side,
        exit_qty=exit_qty,
        exit_price=exit_px,
        reason="exit_reference_price",
    )

    _log(
        "exit_accepted",
        bot_id=bot_id,
        symbol=symbol,
        direction=pos_side,
        qty=str(exit_qty),
        ref_exit=str(exit_px),
        order_id=res.get("order_id"),
        client_order_id=res.get("client_order_id"),
        signal_id=signal_id,
    )

    return {
        "ok": True,
        "signal_id": signal_id,
        "qty": str(exit_qty),
        "ref_exit": str(exit_px),
        "order_id": res.get("order_id"),
        "client_order_id": res.get("client_order_id"),
    }


# ----------------------------
# Routes
# ----------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/webhook", methods=["POST"])
def webhook():
    if not _auth_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

    action = str(payload.get("action") or "").lower().strip()
    if action not in ("entry", "exit"):
        return jsonify({"ok": False, "error": "action must be 'entry' or 'exit'"}), 400

    try:
        out = handle_entry(payload) if action == "entry" else handle_exit(payload)
    except Exception as e:
        _log("webhook_exception", err=str(e), payload=payload)
        return jsonify({"ok": False, "error": "server_error", "detail": str(e)}), 500

    code = 200 if out.get("ok") else 409
    return jsonify(out), code


@app.route("/pnl", methods=["GET"])
def pnl_page():
    """
    Mobile-friendly PnL dashboard (reference-price based).
    """
    bots = pnl_store.list_bots_with_activity()
    if not bots:
        bots = [f"bot{i}" for i in range(1, 21)]

    summaries = []
    for b in bots:
        try:
            s = pnl_store.get_bot_summary(b)
            opens = pnl_store.get_bot_open_positions(b)
            s["open_positions"] = [
                {"symbol": sym, "direction": d, "qty": str(v["qty"]), "weighted_entry": str(v["weighted_entry"])}
                for (sym, d), v in opens.items()
                if _d(v.get("qty", 0)) > 0
            ]
            summaries.append(s)
        except Exception:
            continue

    # simple HTML
    lines: List[str] = []
    lines.append("<html><head><meta name='viewport' content='width=device-width, initial-scale=1' />")
    lines.append("<title>Bot PnL</title>")
    lines.append("<style>body{font-family:Arial;padding:12px} .card{border:1px solid #ddd;border-radius:10px;padding:10px;margin:10px 0} .small{color:#666;font-size:12px} table{width:100%;border-collapse:collapse} td,th{padding:6px;border-bottom:1px solid #eee;text-align:left}</style>")
    lines.append("</head><body>")
    lines.append("<h2>Bot PnL (Reference Price)</h2>")
    lines.append("<div class='small'>Entry/Exit prices are reference (worstPrice used for placement), not exact exchange fill price.</div>")

    # sort by bot number if possible
    def _bot_key(x):
        n = _parse_bot_number(x.get("bot_id", ""))
        return (9999 if n is None else n)

    summaries.sort(key=_bot_key)

    for s in summaries:
        lines.append("<div class='card'>")
        lines.append(f"<b>{s['bot_id']}</b><div class='small'>trades: {s['trades_count']}</div>")
        lines.append("<table>")
        lines.append("<tr><th>24h</th><th>7d</th><th>Total</th></tr>")
        lines.append(f"<tr><td>{s['realized_day']}</td><td>{s['realized_week']}</td><td>{s['realized_total']}</td></tr>")
        lines.append("</table>")

        ops = s.get("open_positions") or []
        if ops:
            lines.append("<div class='small' style='margin-top:8px'>Open positions (local lots)</div>")
            lines.append("<table>")
            lines.append("<tr><th>Symbol</th><th>Dir</th><th>Qty</th><th>W.Entry</th></tr>")
            for o in ops:
                lines.append(f"<tr><td>{o['symbol']}</td><td>{o['direction']}</td><td>{o['qty']}</td><td>{o['weighted_entry']}</td></tr>")
            lines.append("</table>")
        lines.append("</div>")

    lines.append("</body></html>")
    return "\n".join(lines), 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/pnl/json", methods=["GET"])
def pnl_json():
    bots = pnl_store.list_bots_with_activity()
    if not bots:
        bots = [f"bot{i}" for i in range(1, 21)]

    out = []
    for b in bots:
        s = pnl_store.get_bot_summary(b)
        opens = pnl_store.get_bot_open_positions(b)
        s["open_positions"] = {
            f"{sym}:{d}": {"qty": str(v["qty"]), "weighted_entry": str(v["weighted_entry"])}
            for (sym, d), v in opens.items()
        }
        out.append(s)

    return jsonify({"ok": True, "bots": out})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
