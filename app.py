import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set, Any

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    wait_fills_for_order,
    place_fixed_tpsl_orders,
    cancel_order,
    set_on_fill_callback,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
    is_signal_processed,
    mark_signal_processed,
    # NEW mappings
    record_order_map,
    find_order_map,
    record_bracket,
    find_bracket_by_order,
    is_fill_seen,
    mark_fill_seen,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

WS_FILL_WAIT_SEC = float(os.getenv("WS_FILL_WAIT_SEC", "5"))

TPSL_SL_PCT = Decimal(os.getenv("TPSL_SL_PCT", "0.5"))
TPSL_TP_PCT = Decimal(os.getenv("TPSL_TP_PCT", "1.0"))

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# local cache (辅助显示)
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}


def _require_token() -> bool:
    if not DASHBOARD_TOKEN:
        return True
    token = request.args.get("token") or request.headers.get("X-Dashboard-Token")
    return token == DASHBOARD_TOKEN


def _parse_bot_list(env_val: str) -> Set[str]:
    return {b.strip().upper() for b in env_val.split(",") if b.strip()}


def _canon_bot_id(bot_id: str) -> str:
    s = str(bot_id or "").strip().upper().replace(" ", "").replace("-", "_")
    if not s:
        return "BOT_0"
    if s.startswith("BOT_"):
        core = s[4:]
    elif s.startswith("BOT"):
        core = s[3:]
    else:
        core = s
    core = core.replace("_", "")
    if core.isdigit():
        return f"BOT_{int(core)}"
    return s


# -----------------------------
# BOT 分组（你新需求）
# -----------------------------
BOT_1_10_LONG_TPSL = _parse_bot_list(os.getenv("BOT_1_10_LONG_TPSL", ",".join([f"BOT_{i}" for i in range(1, 11)])))
BOT_11_20_SHORT_TPSL = _parse_bot_list(os.getenv("BOT_11_20_SHORT_TPSL", ",".join([f"BOT_{i}" for i in range(11, 21)])))
BOT_21_30_LONG_NO_TPSL = _parse_bot_list(os.getenv("BOT_21_30_LONG_NO_TPSL", ",".join([f"BOT_{i}" for i in range(21, 31)])))
BOT_31_40_SHORT_NO_TPSL = _parse_bot_list(os.getenv("BOT_31_40_SHORT_NO_TPSL", ",".join([f"BOT_{i}" for i in range(31, 41)])))


def _bot_profile(bot_id: str) -> Dict[str, Any]:
    b = _canon_bot_id(bot_id)
    if b in BOT_1_10_LONG_TPSL:
        return {"direction": "LONG", "tpsl": True}
    if b in BOT_11_20_SHORT_TPSL:
        return {"direction": "SHORT", "tpsl": True}
    if b in BOT_21_30_LONG_NO_TPSL:
        return {"direction": "LONG", "tpsl": False}
    if b in BOT_31_40_SHORT_NO_TPSL:
        return {"direction": "SHORT", "tpsl": False}
    # default: deny unless you want permissive
    return {"direction": None, "tpsl": False}


def _extract_budget_usdt(body: dict) -> Decimal:
    size_field = body.get("position_size_usdt") or body.get("size_usdt") or body.get("size")
    if size_field is None:
        raise ValueError("missing position_size_usdt / size_usdt / size")
    budget = Decimal(str(size_field))
    if budget <= 0:
        raise ValueError("size_usdt must be > 0")
    return budget


def _order_status_and_reason(order: dict):
    data = (order or {}).get("raw_order", {}) or {}
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        data = data["data"]
    status = str((data or {}).get("status", "")).upper()
    cancel_reason = str((data or {}).get("cancelReason") or (data or {}).get("rejectReason") or (data or {}).get("errorMessage") or "")
    return status, cancel_reason


# -----------------------------
# WS fill 回调：负责真实记账
# -----------------------------
def _on_ws_fill(fill: dict):
    """
    fill schema in ws_zk_accounts_v3 push includes id/orderId/price/size/side/symbol ... 
    """
    try:
        fill_id = str(fill.get("id") or "")
        order_id = str(fill.get("orderId") or "")
        symbol = str(fill.get("symbol") or "").upper().strip()
        side = str(fill.get("side") or "").upper().strip()
        price = Decimal(str(fill.get("price") or "0"))
        size = Decimal(str(fill.get("size") or "0"))
    except Exception:
        return

    if not fill_id or not order_id or not symbol or price <= 0 or size <= 0:
        return

    # dedup
    if is_fill_seen(fill_id):
        return
    mark_fill_seen(fill_id, order_id)

    omap = find_order_map(order_id)
    if not omap:
        # not ours
        return

    bot_id = _canon_bot_id(omap["bot_id"])
    intent = str(omap["intent"]).upper()
    entry_side = str(omap["entry_side"]).upper()
    direction = str(omap["direction"]).upper()

    if intent == "ENTRY":
        # 每个 fill 记一条 lot（更精确，且不怕多次成交）
        try:
            record_entry(
                bot_id=bot_id,
                symbol=symbol,
                side=entry_side,   # BUY->LONG, SELL->SHORT
                qty=size,
                price=price,
                reason="ws_entry_fill",
            )
        except Exception as e:
            print("[PNL] record_entry(ws) error:", e)

        BOT_POSITIONS[(bot_id, symbol)] = {"side": entry_side, "qty": size, "entry_price": price}
        return

    # EXIT/TP/SL -> FIFO 出场
    reason = "exchange_exit"
    if intent == "TP":
        reason = "exchange_tp"
    elif intent == "SL":
        reason = "exchange_sl"
    elif intent == "EXIT":
        reason = "exchange_exit"

    try:
        record_exit_fifo(
            bot_id=bot_id,
            symbol=symbol,
            entry_side=entry_side,
            exit_qty=size,
            exit_price=price,
            reason=reason,
        )
    except Exception as e:
        print("[PNL] record_exit_fifo(ws) error:", e)

    # 如果这是 bracket 触发，尽量取消另一侧单（避免残留）
    br = find_bracket_by_order(order_id)
    if br:
        tp_oid = str(br.get("tp_order_id") or "")
        sl_oid = str(br.get("sl_order_id") or "")
        other = ""
        if order_id == tp_oid:
            other = sl_oid
        elif order_id == sl_oid:
            other = tp_oid
        if other:
            cancel_order(other)


def _ensure_monitor_thread():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_THREAD_STARTED:
            return
        init_db()
        set_on_fill_callback(_on_ws_fill)
        _MONITOR_THREAD_STARTED = True
        print("[SYSTEM] monitor/ws ready")


@app.route("/", methods=["GET"])
def index():
    _ensure_monitor_thread()
    return "OK", 200


@app.route("/api/pnl", methods=["GET"])
def api_pnl():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    bots = list_bots_with_activity()
    only_bot = request.args.get("bot_id")
    if only_bot:
        only_bot = _canon_bot_id(only_bot)
        bots = [b for b in bots if _canon_bot_id(b) == only_bot]

    out = []
    for bot_id in bots:
        bot_id = _canon_bot_id(bot_id)
        base = get_bot_summary(bot_id)
        opens = get_bot_open_positions(bot_id)

        unrealized = Decimal("0")
        open_rows = []

        for (symbol, direction), v in opens.items():
            qty = v["qty"]
            wentry = v["weighted_entry"]
            if qty <= 0:
                continue

            # 这里不再强行拉 mark（避免额外 API 波动）；只展示账面
            open_rows.append({
                "symbol": str(symbol).upper(),
                "direction": direction.upper(),
                "qty": str(qty),
                "weighted_entry": str(wentry),
            })

        base.update({
            "unrealized": str(unrealized),
            "open_positions": open_rows,
        })
        out.append(base)

    def _rt(x):
        try:
            return Decimal(str(x.get("realized_total", "0")))
        except Exception:
            return Decimal("0")

    out.sort(key=_rt, reverse=True)
    return jsonify({"ts": int(time.time()), "bots": out}), 200


@app.route("/dashboard", methods=["GET"])
def dashboard():
    if not _require_token():
        return Response("Forbidden", status=403)
    _ensure_monitor_thread()
    return Response("OK (use /api/pnl)", mimetype="text/plain")


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()

    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    if not isinstance(body, dict):
        return "bad payload", 400

    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        return "forbidden", 403

    symbol = str(body.get("symbol") or "").upper().strip()
    if not symbol:
        return "missing symbol", 400

    bot_id = _canon_bot_id(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper()
    signal_type_raw = str(body.get("signal_type", "")).lower()
    action_raw = str(body.get("action", "")).lower()

    # mode
    mode: Optional[str] = None
    if signal_type_raw in ("entry", "open"):
        mode = "entry"
    elif signal_type_raw.startswith("exit"):
        mode = "exit"
    else:
        if action_raw in ("open", "entry"):
            mode = "entry"
        elif action_raw in ("close", "exit"):
            mode = "exit"

    if mode is None:
        return "missing or invalid signal_type / action", 400

    # webhook idempotency (保持你原逻辑)
    sig_id = f"{mode}:{bot_id}:{symbol}:{body.get('id') or body.get('signal_id') or int(time.time())}"
    if is_signal_processed(bot_id, sig_id):
        return jsonify({"status": "dedup", "mode": mode, "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}), 200
    mark_signal_processed(bot_id, sig_id, kind=f"webhook_{mode}")

    prof = _bot_profile(bot_id)
    allowed_dir = prof["direction"]
    use_tpsl = bool(prof["tpsl"])

    # -------------------------
    # ENTRY
    # -------------------------
    if mode == "entry":
        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        # enforce direction
        if allowed_dir == "LONG" and side_raw != "BUY":
            return jsonify({"status": "rejected", "reason": "bot long-only", "bot_id": bot_id}), 200
        if allowed_dir == "SHORT" and side_raw != "SELL":
            return jsonify({"status": "rejected", "reason": "bot short-only", "bot_id": bot_id}), 200
        if allowed_dir is None:
            return jsonify({"status": "rejected", "reason": "bot not in allowed groups", "bot_id": bot_id}), 200

        try:
            budget = _extract_budget_usdt(body)
        except Exception as e:
            return str(e), 400

        # 直接用 size_usdt 下 market（qty snapping 在 apex_client 内完成）
        try:
            order = create_market_order(
                symbol=symbol,
                side=side_raw,
                size_usdt=str(budget),
                reduce_only=False,
                client_id=None,
            )
        except Exception as e:
            print("[ENTRY] create_market_order error:", e)
            return "order error", 500

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id") or order.get("apex_client_id") or ""

        if not order_id:
            return jsonify({"status": "order_no_id", "bot_id": bot_id, "symbol": symbol}), 200

        direction = "LONG" if side_raw == "BUY" else "SHORT"

        # map order -> bot for WS attribution
        record_order_map(
            order_id=order_id,
            client_order_id=str(client_order_id),
            bot_id=bot_id,
            symbol=symbol,
            direction=direction,
            entry_side=side_raw,
            intent="ENTRY",
            reduce_only=False,
        )

        # wait WS fills
        try:
            fill = wait_fills_for_order(order_id, timeout_sec=WS_FILL_WAIT_SEC)
            entry_price = Decimal(str(fill["avg_fill_price"]))
            filled_qty = Decimal(str(fill["filled_qty"]))
        except Exception as e:
            print("[ENTRY] wait WS fills error:", e)
            return jsonify({"status": "ws_fill_timeout", "order_id": order_id, "bot_id": bot_id, "symbol": symbol}), 200

        # place exchange TP/SL if needed
        tp_order_id = ""
        sl_order_id = ""
        tp_trigger = None
        sl_trigger = None

        if use_tpsl:
            try:
                tpsl = place_fixed_tpsl_orders(
                    symbol=symbol,
                    direction=direction,
                    qty=filled_qty,
                    entry_price=entry_price,
                    sl_pct=TPSL_SL_PCT,
                    tp_pct=TPSL_TP_PCT,
                )

                sl = tpsl["sl"]
                tp = tpsl["tp"]

                sl_order_id = sl.get("order_id") or ""
                tp_order_id = tp.get("order_id") or ""
                sl_trigger = tpsl.get("sl_trigger")
                tp_trigger = tpsl.get("tp_trigger")

                if sl_order_id:
                    record_order_map(sl_order_id, sl.get("client_order_id") or "", bot_id, symbol, direction, side_raw, "SL", True)
                if tp_order_id:
                    record_order_map(tp_order_id, tp.get("client_order_id") or "", bot_id, symbol, direction, side_raw, "TP", True)

                record_bracket(bot_id, symbol, direction, entry_order_id=order_id, tp_order_id=tp_order_id, sl_order_id=sl_order_id)

            except Exception as e:
                print("[TPSL] place_fixed_tpsl_orders error:", e)

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "direction": direction,
            "use_tpsl": use_tpsl,
            "entry_price": str(entry_price),
            "filled_qty": str(filled_qty),
            "entry_order_id": order_id,
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "tp_trigger": tp_trigger,
            "sl_trigger": sl_trigger,
            "signal_id": sig_id,
        }), 200

    # -------------------------
    # EXIT (manual/strategy close)
    # -------------------------
    if mode == "exit":
        if allowed_dir is None:
            return jsonify({"status": "rejected", "reason": "bot not in allowed groups", "bot_id": bot_id}), 200

        # figure direction from bot profile (simplified)
        direction = allowed_dir
        entry_side = "BUY" if direction == "LONG" else "SELL"
        exit_side = "SELL" if direction == "LONG" else "BUY"

        opens = get_bot_open_positions(bot_id)
        k = (symbol, direction)
        qty_to_close = opens.get(k, {}).get("qty", Decimal("0"))

        if qty_to_close <= 0:
            return jsonify({"status": "no_position", "bot_id": bot_id, "symbol": symbol}), 200

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty_to_close),
                reduce_only=True,
                client_id=None,
            )
        except Exception as e:
            print("[EXIT] create_market_order error:", e)
            return "order error", 500

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id") or order.get("apex_client_id") or ""

        if not order_id:
            return jsonify({"status": "order_no_id", "bot_id": bot_id, "symbol": symbol}), 200

        record_order_map(
            order_id=order_id,
            client_order_id=str(client_order_id),
            bot_id=bot_id,
            symbol=symbol,
            direction=direction,
            entry_side=entry_side,
            intent="EXIT",
            reduce_only=True,
        )

        # best-effort: if bot had bracket, cancel them now
        br = find_bracket_by_order(order_id)
        if br:
            tp_oid = str(br.get("tp_order_id") or "")
            sl_oid = str(br.get("sl_order_id") or "")
            if tp_oid:
                cancel_order(tp_oid)
            if sl_oid:
                cancel_order(sl_oid)

        # wait WS fills for response
        try:
            fill = wait_fills_for_order(order_id, timeout_sec=WS_FILL_WAIT_SEC)
            exit_price = Decimal(str(fill["avg_fill_price"]))
            filled_qty = Decimal(str(fill["filled_qty"]))
        except Exception:
            exit_price = None
            filled_qty = None

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "direction": direction,
            "exit_order_id": order_id,
            "exit_price": str(exit_price) if exit_price else None,
            "filled_qty": str(filled_qty) if filled_qty else None,
            "signal_id": sig_id,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
