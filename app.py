import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set, Any, List

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    create_trigger_order,
    cancel_order,
    pop_order_event,
    get_market_price,
    get_fill_summary,
    get_open_position_for_symbol,
    _get_symbol_rules,
    _snap_quantity,
    start_private_ws,
    start_order_rest_poller,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
    get_symbol_open_directions,
    get_lock_level_pct,
    set_lock_level_pct,
    clear_lock_level_pct,
    is_signal_processed,
    mark_signal_processed,
    # exchange protective orders
    set_protective_orders,
    get_protective_orders,
    clear_protective_orders,
    get_protective_owner_by_order_id,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ✅ 只让 worker 进程启用 WS/Fills（supervisord.conf 里给 worker 设置 ENABLE_WS="1"，web 设置 "0"）
ENABLE_WS = str(os.getenv("ENABLE_WS", "0")).strip() == "1"

# ✅ Exchange-side protective orders (STOP/TP) managed by worker
ENABLE_EXCHANGE_PROTECTIVE = str(os.getenv("ENABLE_EXCHANGE_PROTECTIVE", "1")).strip() == "1"
# Optional TP percent (blank/0 disables). Example: 0.2 means +0.2% TP for LONG, -0.2% for SHORT
EXCHANGE_TP_PCT = str(os.getenv("EXCHANGE_TP_PCT", "")).strip()
TRIGGER_PRICE_TYPE = str(os.getenv("TRIGGER_PRICE_TYPE", "")).strip()  # e.g. LAST_PRICE / MARK_PRICE / INDEX_PRICE (if supported)

# 退出互斥窗口（秒）：防止重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}

# ✅ 说明：本版本不在交易所挂真实 TP/SL 单；仅使用真实 fills 进行记账，并由机器人在后台按规则触发平仓。

# 本地 cache（仅辅助）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ✅ 退出互斥：bot+symbol 粒度 cooldown
_EXIT_LOCK = threading.Lock()
_LAST_EXIT_TS: Dict[Tuple[str, str], float] = {}


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


def _bot_num(bot_id: str) -> int:
    b = _canon_bot_id(bot_id)
    try:
        return int(b.split("_", 1)[1])
    except Exception:
        return 0


def _to_decimal(x: Any, default: Decimal = Decimal("0")) -> Decimal:
    """Best-effort Decimal coercion used by monitor loops.

    Keeps app resilient when WS payload fields are empty strings / None.
    """
    if x is None:
        return default
    if isinstance(x, Decimal):
        return x
    try:
        s = str(x).strip()
        if not s:
            return default
        return Decimal(s)
    except Exception:
        return default


def _exit_cooldown_ok(bot_id: str, symbol: str) -> bool:
    key = (_canon_bot_id(bot_id), str(symbol).upper())
    now = time.time()
    with _EXIT_LOCK:
        last = _LAST_EXIT_TS.get(key, 0.0)
        if now - last < EXIT_COOLDOWN_SEC:
            return False
        return True


def _mark_exit_now(bot_id: str, symbol: str) -> None:
    key = (_canon_bot_id(bot_id), str(symbol).upper())
    with _EXIT_LOCK:
        _LAST_EXIT_TS[key] = time.time()


def _start_monitor_threads_once():
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_THREAD_STARTED:
            return
        _MONITOR_THREAD_STARTED = True

    # DB init/migrate
    init_db()

    # Only worker should run WS + REST pollers
    if ENABLE_WS:
        print("[SYSTEM] WS enabled in this process (ENABLE_WS=1)")
        start_private_ws()
        start_order_rest_poller(float(os.getenv("REST_POLL_INTERVAL", "5.0")))
    else:
        print("[SYSTEM] WS disabled in this process (ENABLE_WS=0)")

    print("[SYSTEM] monitor/ws threads ready")


# ----------------------------
# BOT risk scope
# ----------------------------
LADDER_LONG_BOTS = _parse_bot_list(os.getenv("LADDER_LONG_BOTS", ",".join([f"BOT_{i}" for i in range(1, 6)])))
LADDER_SHORT_BOTS = _parse_bot_list(os.getenv("LADDER_SHORT_BOTS", ",".join([f"BOT_{i}" for i in range(11, 16)])))

# Base stop-loss (percent). Example 0.5 -> -0.5% initial lock.
LADDER_BASE_SL_PCT = Decimal(os.getenv("LADDER_BASE_SL_PCT", "0.5"))

# Profit% : lock% mapping (both in percent, not decimals)
_DEFAULT_LADDER_LEVELS = "0.15:0.125,0.35:0.15,0.45:0.25,0.55:0.35,0.75:0.55,0.95:0.75"
LADDER_LEVELS_RAW = os.getenv("LADDER_LEVELS", _DEFAULT_LADDER_LEVELS)

# Risk poll interval (seconds)
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.0"))


def _parse_ladder_levels(s: str):
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        a, b = part.split(":", 1)
        try:
            profit = Decimal(a.strip())
            lock = Decimal(b.strip())
        except Exception:
            continue
        out.append((profit, lock))
    # sort by profit threshold ascending
    out.sort(key=lambda x: x[0])
    return out


LADDER_LEVELS = _parse_ladder_levels(LADDER_LEVELS_RAW)


def _bot_uses_ladder(bot_id: str, direction: str) -> bool:
    b = _canon_bot_id(bot_id)
    d = str(direction or "").upper()
    if d == "LONG":
        return b in LADDER_LONG_BOTS
    if d == "SHORT":
        return b in LADDER_SHORT_BOTS
    return False


def _get_mark_price(symbol: str) -> Optional[Decimal]:
    """Best-effort mark/index/oracle price from account positions (no worstPrice quote)."""
    try:
        pos = get_open_position_for_symbol(symbol)
        if not isinstance(pos, dict):
            return None
        mp = pos.get("markPrice") or pos.get("oraclePrice")
        if mp is None:
            return None
        return Decimal(str(mp))
    except Exception:
        return None


def _compute_profit_pct(direction: str, entry: Decimal, mark: Decimal) -> Decimal:
    if entry <= 0 or mark <= 0:
        return Decimal("0")
    d = str(direction).upper()
    if d == "LONG":
        return (mark - entry) / entry * Decimal("100")
    else:
        return (entry - mark) / entry * Decimal("100")


def _compute_stop_price(direction: str, entry: Decimal, lock_pct: Decimal) -> Decimal:
    d = str(direction).upper()
    if d == "LONG":
        return entry * (Decimal("1") + (lock_pct / Decimal("100")))
    else:
        return entry * (Decimal("1") - (lock_pct / Decimal("100")))


def _better_lock_level(current: Optional[Decimal], new: Decimal) -> bool:
    if current is None:
        return True
    return new > current


def _ladder_target_lock(profit_pct: Decimal) -> Decimal:
    """Given current profit%, return lock% per ladder; else base SL negative."""
    target = None
    for p, lock in LADDER_LEVELS:
        if profit_pct >= p:
            target = lock
        else:
            break
    if target is None:
        return Decimal("0") - LADDER_BASE_SL_PCT
    return target


def _parse_budget_usdt(body: dict) -> Decimal:
    # prefer explicit position_size_usdt / size_usdt / size
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


# ----------------------------
# 预算 -> snapped qty（修复 ConversionSyntax）
# ----------------------------
def _compute_entry_qty(symbol: str, side: str, budget: Decimal, ref_price: Optional[Decimal] = None) -> Decimal:
    """Budget (USDT) -> snapped base-asset qty.

    Priority for reference price:
    1) ref_price from webhook payload (if provided and > 0)
    2) get_market_price() worstPrice quote (numeric strict in apex_client)

    This avoids Decimal ConversionSyntax when SDK returns non-numeric dict payloads.
    """
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    ref_px = None
    if ref_price is not None:
        try:
            ref_px = Decimal(str(ref_price))
        except Exception:
            ref_px = None

    if ref_px is None or ref_px <= 0:
        px_raw = get_market_price(symbol, side, str(min_qty))
        try:
            ref_px = Decimal(str(px_raw))
        except Exception:
            ref_px = Decimal("0")

    if ref_px <= 0:
        raise ValueError(f"invalid reference price for qty compute: symbol={symbol} side={side} price={ref_px}")

    theoretical_qty = budget / ref_px
    snapped_qty = _snap_quantity(symbol, theoretical_qty)

    if snapped_qty <= 0:
        raise ValueError(f"snapped_qty <= 0, symbol={symbol}, budget={budget}")

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
# 幂等 Signal ID（webhook）
# ----------------------------
def _get_signal_id(body: dict, mode: str, bot_id: str, symbol: str) -> str:
    for k in ("signal_id", "alert_id", "id", "tv_id", "client_id"):
        v = body.get(k)
        if v:
            return f"{mode}:{bot_id}:{symbol}:{str(v)}"

    ts = body.get("ts") or body.get("time") or int(time.time())
    try:
        ts_int = int(float(str(ts)))
    except Exception:
        ts_int = int(time.time())

    sig = str(body.get("signal_type") or body.get("action") or "").lower().strip()
    side = str(body.get("side") or "").upper().strip()

    return f"{mode}:{bot_id}:{symbol}:{sig}:{side}:{ts_int}"


# ----------------------------
# ✅ Ladder risk loop (bot-side close via reduceOnly market)
# ----------------------------
_RISK_THREAD_STARTED = False
_RISK_LOCK = threading.Lock()


def _risk_loop():
    print(f"[LADDER] risk loop started (interval={RISK_POLL_INTERVAL}s)")
    while True:
        try:
            # Iterate bots with open positions in DB
            for bot_id in list_bots_with_activity():
                opens = get_bot_open_positions(bot_id)
                for pos in opens:
                    symbol = str(pos.get("symbol") or "").upper()
                    direction = str(pos.get("direction") or "").upper()
                    if not symbol or direction not in {"LONG", "SHORT"}:
                        continue

                    if not _bot_uses_ladder(bot_id, direction):
                        continue

                    qty = _to_decimal(pos.get("qty"))
                    entry = _to_decimal(pos.get("avg_entry"))
                    if qty <= 0 or entry <= 0:
                        continue

                    mark = _get_mark_price(symbol)
                    if mark is None or mark <= 0:
                        continue

                    profit_pct = _compute_profit_pct(direction, entry, mark)
                    target_lock = _ladder_target_lock(profit_pct)

                    cur_lock = get_lock_level_pct(bot_id, symbol, direction)
                    if _better_lock_level(cur_lock, target_lock):
                        set_lock_level_pct(bot_id, symbol, direction, target_lock)
                        print(f"[LADDER] {bot_id} {symbol} {direction} profit={profit_pct:.4f}% -> lock={target_lock}%")

                    # Check stop trigger
                    lock_now = get_lock_level_pct(bot_id, symbol, direction)
                    if lock_now is None:
                        continue

                    stop_px = _compute_stop_price(direction, entry, lock_now)
                    # LONG: mark <= stop -> exit ; SHORT: mark >= stop -> exit
                    should_exit = (direction == "LONG" and mark <= stop_px) or (direction == "SHORT" and mark >= stop_px)
                    if should_exit:
                        if not _exit_cooldown_ok(bot_id, symbol):
                            continue
                        _mark_exit_now(bot_id, symbol)
                        side = "SELL" if direction == "LONG" else "BUY"
                        print(f"[LADDER] STOP HIT: {bot_id} {symbol} {direction} mark={mark} stop={stop_px} -> close market reduceOnly")
                        o = create_market_order(symbol, side, str(qty), reduce_only=True)
                        oid = o.get("order_id")
                        cid = o.get("client_order_id")
                        summ = get_fill_summary(order_id=oid, client_order_id=cid, timeout_sec=3.0)
                        fill_px = _to_decimal(summ.get("avg_price"))
                        filled_qty = _to_decimal(summ.get("filled_qty"))
                        if fill_px > 0 and filled_qty > 0:
                            record_exit_fifo(bot_id, symbol, direction, filled_qty, fill_px, order_id=oid, client_order_id=cid, reason="ladder_stop")
                        clear_lock_level_pct(bot_id, symbol, direction)

        except Exception:
            pass

        time.sleep(max(0.2, RISK_POLL_INTERVAL))


def _start_risk_thread_once():
    global _RISK_THREAD_STARTED
    with _RISK_LOCK:
        if _RISK_THREAD_STARTED:
            return
        _RISK_THREAD_STARTED = True

    t = threading.Thread(target=_risk_loop, daemon=True)
    t.start()
    print("[LADDER] risk thread created")


@app.before_request
def _before_req():
    _start_monitor_threads_once()
    if ENABLE_WS:
        _start_risk_thread_once()


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/bots")
def bots():
    if not _require_token():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify({"bots": list_bots_with_activity()})


@app.get("/bot/<bot_id>")
def bot_summary(bot_id: str):
    if not _require_token():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(get_bot_summary(_canon_bot_id(bot_id)))


@app.get("/bot/<bot_id>/open")
def bot_open(bot_id: str):
    if not _require_token():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify({"open_positions": get_bot_open_positions(_canon_bot_id(bot_id))})


@app.post("/webhook")
def webhook():
    body = request.get_json(force=True, silent=True) or {}
    print(f"[WEBHOOK] raw body: {body}")

    if WEBHOOK_SECRET and str(body.get("secret", "")) != WEBHOOK_SECRET:
        return jsonify({"error": "bad secret"}), 403

    bot_id = _canon_bot_id(body.get("bot_id") or body.get("bot") or "BOT_0")
    symbol = str(body.get("symbol") or "").upper().strip()
    if not symbol:
        return jsonify({"error": "missing symbol"}), 400

    action = str(body.get("action") or body.get("signal_type") or "").lower().strip()
    side_raw = str(body.get("side") or "").upper().strip()  # BUY / SELL
    direction = str(body.get("direction") or "").upper().strip()  # LONG / SHORT

    # Compatibility: allow action=entry/exit or open/close
    if action in {"open", "entry", "long", "short"}:
        mode = "entry"
    elif action in {"close", "exit"}:
        mode = "exit"
    else:
        mode = action or "entry"

    sig_id = _get_signal_id(body, mode, bot_id, symbol)
    if is_signal_processed(sig_id):
        return jsonify({"ok": True, "deduped": True, "signal_id": sig_id})

    try:
        if mode == "entry":
            if side_raw not in {"BUY", "SELL"}:
                return jsonify({"error": "missing side"}), 400

            budget = _parse_budget_usdt(body)

            # ✅ 用 TV payload 自带 price 作为优先参考价（没有就 fallback worstPrice）
            tv_px = _to_decimal(body.get('price') or body.get('entry_price') or body.get('last_price') or body.get('close'), default=Decimal('0'))
            snapped_qty = _compute_entry_qty(symbol, side_raw, budget, ref_price=(tv_px if tv_px > 0 else None))

            client_id = str(body.get("client_id") or body.get("clientOrderId") or f"tv-{symbol}-{int(time.time())}")
            o = create_market_order(symbol, side_raw, str(snapped_qty), reduce_only=False, client_id=client_id)
            oid = o.get("order_id")
            cid = o.get("client_order_id")

            summ = get_fill_summary(order_id=oid, client_order_id=cid, timeout_sec=3.0)
            fill_px = _to_decimal(summ.get("avg_price"))
            filled_qty = _to_decimal(summ.get("filled_qty"))

            if fill_px <= 0 or filled_qty <= 0:
                raise RuntimeError(f"no fills yet (source={summ.get('source')})")

            # Determine direction if missing
            if direction not in {"LONG", "SHORT"}:
                direction = "LONG" if side_raw == "BUY" else "SHORT"

            record_entry(bot_id, symbol, direction, filled_qty, fill_px, order_id=oid, client_order_id=cid)
            mark_signal_processed(sig_id)

            return jsonify({"ok": True, "mode": "entry", "bot_id": bot_id, "symbol": symbol, "direction": direction, "qty": str(filled_qty), "entry_price": str(fill_px), "fill_source": summ.get("source")})

        elif mode == "exit":
            # direction required to pick correct close side
            if direction not in {"LONG", "SHORT"}:
                # infer from side if provided
                if side_raw in {"BUY", "SELL"}:
                    direction = "LONG" if side_raw == "SELL" else "SHORT"  # if you send exit side, invert
                else:
                    # if still unknown, try DB open directions
                    dirs = get_symbol_open_directions(bot_id, symbol)
                    direction = next(iter(dirs), "LONG")

            if not _exit_cooldown_ok(bot_id, symbol):
                return jsonify({"ok": True, "cooldown": True})

            _mark_exit_now(bot_id, symbol)

            open_dirs = get_symbol_open_directions(bot_id, symbol)
            if direction not in open_dirs and open_dirs:
                direction = next(iter(open_dirs))

            qty_str = body.get("qty") or body.get("size") or body.get("close_qty")
            if qty_str:
                qty = _to_decimal(qty_str)
            else:
                # if qty not provided, close all from DB summary
                opens = get_bot_open_positions(bot_id)
                qty = Decimal("0")
                for p in opens:
                    if str(p.get("symbol")).upper() == symbol and str(p.get("direction")).upper() == direction:
                        qty = _to_decimal(p.get("qty"))
                        break
            if qty <= 0:
                return jsonify({"error": "no qty to close"}), 400

            close_side = "SELL" if direction == "LONG" else "BUY"
            o = create_market_order(symbol, close_side, str(qty), reduce_only=True)
            oid = o.get("order_id")
            cid = o.get("client_order_id")

            summ = get_fill_summary(order_id=oid, client_order_id=cid, timeout_sec=3.0)
            fill_px = _to_decimal(summ.get("avg_price"))
            filled_qty = _to_decimal(summ.get("filled_qty"))

            if fill_px > 0 and filled_qty > 0:
                record_exit_fifo(bot_id, symbol, direction, filled_qty, fill_px, order_id=oid, client_order_id=cid, reason=str(body.get("reason") or "signal_exit"))
            clear_lock_level_pct(bot_id, symbol, direction)
            mark_signal_processed(sig_id)

            return jsonify({"ok": True, "mode": "exit", "bot_id": bot_id, "symbol": symbol, "direction": direction, "qty": str(filled_qty), "exit_price": str(fill_px), "fill_source": summ.get("source")})

        else:
            return jsonify({"error": f"unknown mode {mode}"}), 400

    except Exception as e:
        print(f"[WEBHOOK] error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Local dev only; production uses gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
