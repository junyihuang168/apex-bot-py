import os
import time
import threading
from decimal import Decimal
from typing import Dict, Tuple, Optional, Set

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    create_limit_order,
    get_market_price,
    get_fill_summary,
    get_open_position_for_symbol,
    _get_symbol_rules,
    _snap_quantity,
    start_private_ws,
    start_order_rest_poller,
    pop_order_event,
    create_trigger_order,
    snap_price_for_order,
    cancel_order,
)

from pnl_store import (
    init_db,
    record_entry,
    record_exit_fifo,
    list_bots_with_activity,
    get_bot_summary,
    get_bot_open_positions,
    get_symbol_open_directions,
    clear_lock_level_pct,
    is_signal_processed,
    mark_signal_processed,
    # ✅ protective orders
    set_protective_orders,
    get_protective_orders,
    clear_protective_orders,
    find_protective_owner_by_order_id,
    get_lock_level_pct,
    set_lock_level_pct,
    list_recent_trades,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ✅ 只让 worker 进程启用 WS/Fills（supervisord.conf 里给 worker 设置 ENABLE_WS="1"，web 设置 "0"）
ENABLE_WS = str(os.getenv("ENABLE_WS", "0")).strip() == "1"

# 退出互斥窗口（秒）：防止重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}

# ✅ 固定止损/止盈（百分比）
FIXED_SL_PCT = Decimal(os.getenv("FIXED_SL_PCT", "0.5"))  # -0.5% base SL (restore BOT_1-5 / BOT_11-15 ladder)
FIXED_TP_PCT = Decimal(os.getenv("FIXED_TP_PCT", "2.5"))  # +2.5% fixed TP (restore)
# ✅ Ladder trailing SL (percent, based on TRUE entry price)
# - Initial SL: -0.5%
# - Profit >= 0.125% -> lock SL to +0.10%
# - Profit >= 0.35%  -> lock SL to +0.15%
# - Then every +0.20% profit -> lock +0.20% (only raise, never lower)
LADDER_PROFIT_1_PCT = Decimal(os.getenv("LADDER_PROFIT_1_PCT", "0.125"))
LADDER_LOCK_1_PCT   = Decimal(os.getenv("LADDER_LOCK_1_PCT",   "0.10"))
LADDER_BASE_PROFIT_PCT = Decimal(os.getenv("LADDER_BASE_PROFIT_PCT", "0.35"))
LADDER_BASE_LOCK_PCT   = Decimal(os.getenv("LADDER_BASE_LOCK_PCT",   "0.15"))
LADDER_STEP_PCT        = Decimal(os.getenv("LADDER_STEP_PCT",        "0.20"))
TRAIL_POLL_INTERVAL_SEC = float(os.getenv("TRAIL_POLL_INTERVAL_SEC", "1.0"))


# ✅ 保护单模式：MARKET（默认稳） or LIMIT
PROTECTIVE_ORDER_MODE = str(os.getenv("PROTECTIVE_ORDER_MODE", "MARKET")).upper().strip()
PROTECTIVE_SLIPPAGE_PCT = Decimal(os.getenv("PROTECTIVE_SLIPPAGE_PCT", "0.15"))  # LIMIT 模式滑点 0.15%

# ✅ TP/SL 订单有效期（秒时间戳），默认 30 天
PROTECTIVE_EXPIRE_SEC = int(os.getenv("PROTECTIVE_EXPIRE_SEC", str(30 * 24 * 3600)))

# 本地 cache（仅辅助）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ✅ 退出互斥：bot+symbol 粒度 cooldown
_EXIT_LOCK = threading.Lock()
_LAST_EXIT_TS: Dict[Tuple[str, str], float] = {}

# ✅ WS fills 处理线程
_FILLS_THREAD_STARTED = False
_FILLS_LOCK = threading.Lock()


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


# ----------------------------
# ✅ BOT 分组（按你要求）
# ----------------------------
# Profile A (Ladder SL + optional fixed TP): BOT_1–5 (LONG), BOT_11–15 (SHORT)
LADDER_LONG_BOTS  = _parse_bot_list(os.getenv("LADDER_LONG_BOTS",  ",".join([f"BOT_{i}" for i in range(1, 6)])))
LADDER_SHORT_BOTS = _parse_bot_list(os.getenv("LADDER_SHORT_BOTS", ",".join([f"BOT_{i}" for i in range(11, 16)])))

# Profile B (Fixed maker TP + fixed SL): BOT_6–10 (LONG), BOT_16–20 (SHORT)
FIXED_LONG_BOTS   = _parse_bot_list(os.getenv("FIXED_LONG_BOTS",   ",".join([f"BOT_{i}" for i in range(6, 11)])))
FIXED_SHORT_BOTS  = _parse_bot_list(os.getenv("FIXED_SHORT_BOTS",  ",".join([f"BOT_{i}" for i in range(16, 21)])))

# PnL-only bots (no protective orders): default BOT_21–40 (override via env if needed)
LONG_PNL_ONLY_BOTS  = _parse_bot_list(os.getenv("LONG_PNL_ONLY_BOTS",  ",".join([f"BOT_{i}" for i in range(21, 31)])))
SHORT_PNL_ONLY_BOTS = _parse_bot_list(os.getenv("SHORT_PNL_ONLY_BOTS", ",".join([f"BOT_{i}" for i in range(31, 41)])))

def _bot_profile(bot_id: str) -> str:
    b = _canon_bot_id(bot_id)
    if b in LADDER_LONG_BOTS or b in LADDER_SHORT_BOTS:
        return "LADDER"
    if b in FIXED_LONG_BOTS or b in FIXED_SHORT_BOTS:
        return "FIXED"
    return "NONE"


def _bot_expected_entry_side(bot_id: str) -> Optional[str]:
    b = _canon_bot_id(bot_id)
    if b in LADDER_LONG_BOTS or b in FIXED_LONG_BOTS or b in LONG_PNL_ONLY_BOTS:
        return "BUY"
    if b in LADDER_SHORT_BOTS or b in FIXED_SHORT_BOTS or b in SHORT_PNL_ONLY_BOTS:
        return "SELL"
    return None


def _bot_has_exchange_brackets(bot_id: str) -> bool:
    b = _canon_bot_id(bot_id)
    return (b in LADDER_LONG_BOTS) or (b in LADDER_SHORT_BOTS) or (b in FIXED_LONG_BOTS) or (b in FIXED_SHORT_BOTS)



def _exit_guard_allow(bot_id: str, symbol: str) -> bool:
    key = (_canon_bot_id(bot_id), str(symbol or "").upper().strip())
    now = time.time()
    with _EXIT_LOCK:
        last = _LAST_EXIT_TS.get(key, 0.0)
        if now - last < EXIT_COOLDOWN_SEC:
            return False
        _LAST_EXIT_TS[key] = now
        return True


# ----------------------------
# 预算提取
# ----------------------------
def _extract_budget_usdt(body: dict) -> Decimal:
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
# 预算 -> snapped qty
# ----------------------------
def _compute_entry_qty(symbol: str, side: str, budget: Decimal) -> Decimal:
    rules = _get_symbol_rules(symbol)
    min_qty = rules["min_qty"]

    ref_price_dec = Decimal(get_market_price(symbol, side, str(min_qty)))
    theoretical_qty = budget / ref_price_dec
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
# ✅ 交易所 TP/SL（固定百分比）下单
# ----------------------------
def _compute_fixed_bracket_prices(direction: str, entry_price: Decimal) -> Tuple[Decimal, Decimal]:
    """
    return (sl_trigger, tp_trigger)
    """
    if entry_price <= 0:
        return Decimal("0"), Decimal("0")

    sl_pct = FIXED_SL_PCT / Decimal("100")
    tp_pct = FIXED_TP_PCT / Decimal("100")

    if direction.upper() == "LONG":
        sl = entry_price * (Decimal("1") - sl_pct)
        tp = entry_price * (Decimal("1") + tp_pct)
    else:
        sl = entry_price * (Decimal("1") + sl_pct)
        tp = entry_price * (Decimal("1") - tp_pct)

    return sl, tp


def _apply_limit_slippage(direction: str, trigger: Decimal) -> Decimal:
    """
    LIMIT 模式：给“更差一点”的限价，提升成交概率。
    LONG 平仓 SELL：更低一点
    SHORT 平仓 BUY：更高一点
    """
    slip = (PROTECTIVE_SLIPPAGE_PCT / Decimal("100"))
    d = direction.upper()
    if d == "LONG":
        return trigger * (Decimal("1") - slip)
    else:
        return trigger * (Decimal("1") + slip)


def _cancel_existing_brackets(bot_id: str, symbol: str, direction: str):
    po = get_protective_orders(bot_id, symbol, direction)
    if not po:
        return
    sl_oid = po.get("sl_order_id")
    tp_oid = po.get("tp_order_id")
    if sl_oid:
        try:
            cancel_order(str(sl_oid))
        except Exception:
            pass
    if tp_oid:
        try:
            cancel_order(str(tp_oid))
        except Exception:
            pass
    try:
        clear_protective_orders(bot_id, symbol, direction)
    except Exception:
        pass


def _sl_trigger_from_lock(entry_price: Decimal, direction: str, lock_pct: Decimal) -> Decimal:
    """
    lock_pct is percent relative to entry.
      LONG:  lock +0.10% => trigger = entry * (1 + 0.0010)
      SHORT: lock +0.10% => trigger = entry * (1 - 0.0010)
    """
    ep = Decimal(entry_price)
    lp = Decimal(lock_pct) / Decimal("100")
    if str(direction).upper() == "LONG":
        return ep * (Decimal("1") + lp)
    return ep * (Decimal("1") - lp)


def _tp_trigger_from_entry(entry_price: Decimal, direction: str) -> Decimal:
    ep = Decimal(entry_price)
    tp = Decimal(FIXED_TP_PCT) / Decimal("100")
    if str(direction).upper() == "LONG":
        return ep * (Decimal("1") + tp)
    return ep * (Decimal("1") - tp)


def _snap_price_tick(symbol: str, price: Decimal) -> Decimal:
    """
    Snap a price to the symbol's tick size (ROUND_DOWN).
    """
    rules = _get_symbol_rules(symbol)
    tick = Decimal(str(rules.get("tick_size") or rules.get("tickSize") or "0"))
    if tick <= 0:
        return Decimal(price)
    p = Decimal(price)
    return (p / tick).to_integral_value(rounding="ROUND_DOWN") * tick


def _cancel_existing_protective_orders(bot_id: str, symbol: str, direction: str):
    po = get_protective_orders(bot_id, symbol, direction) or {}
    for oid_key in ("sl_order_id", "tp_order_id"):
        oid = po.get(oid_key)
        if oid:
            try:
                cancel_order(str(oid))
            except Exception:
                pass
    try:
        clear_protective_orders(bot_id, symbol, direction)
    except Exception:
        pass


def _profit_pct(entry_price: Decimal, mark_price: Decimal, direction: str) -> Decimal:
    ep = Decimal(entry_price)
    mp = Decimal(mark_price)
    if ep <= 0 or mp <= 0:
        return Decimal("0")
    if str(direction).upper() == "LONG":
        return (mp - ep) / ep * Decimal("100")
    return (ep - mp) / ep * Decimal("100")


def _desired_lock_pct_from_profit(profit_pct: Decimal, current_lock_pct: Decimal) -> Decimal:
    """
    Ladder logic (only raise):
      - Start: -0.5%
      - profit >= 0.125% => lock to +0.10%
      - profit >= 0.35%  => lock to +0.15%
      - then for each +0.20% profit => lock +0.20%
    """
    p = Decimal(profit_pct)
    cur = Decimal(current_lock_pct)

    # base (initial lock is stored as -0.5)
    desired = cur

    if p >= LADDER_PROFIT_1_PCT:
        desired = max(desired, LADDER_LOCK_1_PCT)

    if p >= LADDER_BASE_PROFIT_PCT:
        # base lock
        desired = max(desired, LADDER_BASE_LOCK_PCT)

        # steps above base
        extra = p - LADDER_BASE_PROFIT_PCT
        if extra > 0:
            n = int((extra / LADDER_STEP_PCT).to_integral_value(rounding="ROUND_FLOOR"))
            desired = max(desired, LADDER_BASE_LOCK_PCT + (LADDER_STEP_PCT * Decimal(n)))

    return desired


def _next_lock_target(current_lock_pct: Decimal) -> tuple[Decimal, Decimal]:
    """
    Returns (next_profit_threshold_pct, next_lock_pct) for dashboard display.
    """
    cur = Decimal(current_lock_pct)
    if cur < LADDER_LOCK_1_PCT:
        return (LADDER_PROFIT_1_PCT, LADDER_LOCK_1_PCT)
    if cur < LADDER_BASE_LOCK_PCT:
        return (LADDER_BASE_PROFIT_PCT, LADDER_BASE_LOCK_PCT)

    next_lock = cur + LADDER_STEP_PCT
    next_profit = next_lock + LADDER_STEP_PCT  # lock = profit - step
    return (next_profit, next_lock)


def _place_fixed_brackets(bot_id: str, symbol: str, direction: str, entry_price: Decimal, qty: Decimal):
    """Place exchange-side protective orders after we have a real entry price.

    Profiles:
      - FIXED  (BOT_6–10 LONG, BOT_16–20 SHORT): maker TP +0.2% (LIMIT) + SL -0.3% (STOP_LIMIT)
      - LADDER (BOT_1–5 LONG, BOT_11–15 SHORT): TP +2.5% (TAKE_PROFIT_MARKET) + SL -0.5% (STOP_MARKET) + ladder trail handled separately
      - NONE   : no protective orders
    """
    b = _canon_bot_id(bot_id)
    direction = str(direction or "").upper()
    if direction not in ("LONG", "SHORT"):
        return

    prof = _bot_profile(b)
    if prof == "NONE":
        return

    qty = _to_decimal(qty)
    entry_price = _to_decimal(entry_price)
    if qty is None or entry_price is None or qty <= 0 or entry_price <= 0:
        return

    exit_side = "SELL" if direction == "LONG" else "BUY"

    # Always clear stale records first (in case of restart/crash).
    try:
        clear_protective_orders(b, symbol, direction)
    except Exception:
        pass

    # ----------------------------
    # Profile B: FIXED maker TP + fixed SL
    # ----------------------------
    if prof == "FIXED":
        tp_pct = _to_decimal(os.getenv("FIXED_MAKER_TP_PCT", "0.2")) or Decimal("0.2")
        sl_pct = _to_decimal(os.getenv("FIXED_SL_PCT", "0.3")) or Decimal("0.3")
        buf_pct = _to_decimal(os.getenv("STOP_LIMIT_BUFFER_PCT", "0.05")) or Decimal("0.05")  # 0.05% price buffer

        # TP LIMIT price
        if direction == "LONG":
            tp_px = entry_price * (Decimal("1") + (tp_pct / Decimal("100")))
        else:
            tp_px = entry_price * (Decimal("1") - (tp_pct / Decimal("100")))
        tp_px = snap_price_for_order(symbol, exit_side, "LIMIT", tp_px)

        # SL STOP_LIMIT
        if direction == "LONG":
            sl_trigger = entry_price * (Decimal("1") - (sl_pct / Decimal("100")))
            sl_limit = sl_trigger * (Decimal("1") - (buf_pct / Decimal("100")))  # lower limit for SELL
        else:
            sl_trigger = entry_price * (Decimal("1") + (sl_pct / Decimal("100")))
            sl_limit = sl_trigger * (Decimal("1") + (buf_pct / Decimal("100")))  # higher limit for BUY

        sl_trigger = snap_price_for_order(symbol, exit_side, "STOP_LIMIT", sl_trigger)
        sl_limit = snap_price_for_order(symbol, exit_side, "STOP_LIMIT", sl_limit)

        tp_order = None
        sl_order = None

        try:
            tp_order = create_limit_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty),
                price=str(tp_px),
                reduce_only=True,
                time_in_force=os.getenv("TP_TIME_IN_FORCE") or None,
            )
        except Exception as e:
            print(f"[PROTECT][FIXED] TP limit create failed bot={b} {direction} {symbol}: {e}")

        try:
            sl_order = create_trigger_order(
                symbol=symbol,
                side=exit_side,
                order_type="STOP_LIMIT",
                size=str(qty),
                trigger_price=str(sl_trigger),
                price=str(sl_limit),
                reduce_only=True,
            )
        except Exception as e:
            print(f"[PROTECT][FIXED] SL stop-limit create failed bot={b} {direction} {symbol}: {e}")

        tp_oid = (tp_order or {}).get("order_id") or (tp_order or {}).get("id")
        sl_oid = (sl_order or {}).get("order_id") or (sl_order or {}).get("id")

        set_protective_orders(
            bot_id=b,
            symbol=symbol,
            direction=direction,
            sl_order_id=str(sl_oid) if sl_oid else None,
            tp_order_id=str(tp_oid) if tp_oid else None,
            sl_price=sl_trigger,
            tp_price=tp_px,
        )

        print(f"[PROTECT][FIXED] bot={b} {direction} {symbol} qty={qty} entry={entry_price} TP={tp_px} SL_trigger={sl_trigger} SL_limit={sl_limit}")
        return

    # ----------------------------
    # Profile A: LADDER base brackets (trail loop will later raise SL)
    # ----------------------------
    tp_pct = _to_decimal(os.getenv("FIXED_TP_PCT", "2.5")) or Decimal("2.5")
    sl_pct = _to_decimal(os.getenv("BASE_SL_PCT", "-0.5")) or Decimal("-0.5")  # negative

    if direction == "LONG":
        tp_trigger = entry_price * (Decimal("1") + (tp_pct / Decimal("100")))
        sl_trigger = entry_price * (Decimal("1") + (sl_pct / Decimal("100")))  # e.g. 1 - 0.5%
    else:
        tp_trigger = entry_price * (Decimal("1") - (tp_pct / Decimal("100")))
        sl_trigger = entry_price * (Decimal("1") - (sl_pct / Decimal("100")))  # for SHORT, sl_pct is negative so becomes +0.5%

    tp_trigger = snap_price_for_order(symbol, exit_side, "TAKE_PROFIT_MARKET", tp_trigger)
    sl_trigger = snap_price_for_order(symbol, exit_side, "STOP_MARKET", sl_trigger)

    tp_order = None
    sl_order = None

    # Keep default behavior as MARKET triggers for ladder bots (more reliable execution).
    try:
        tp_order = create_trigger_order(
            symbol=symbol,
            side=exit_side,
            order_type="TAKE_PROFIT_MARKET",
            size=str(qty),
            trigger_price=str(tp_trigger),
            reduce_only=True,
        )
    except Exception as e:
        print(f"[PROTECT][LADDER] TP create failed bot={b} {direction} {symbol}: {e}")

    try:
        sl_order = create_trigger_order(
            symbol=symbol,
            side=exit_side,
            order_type="STOP_MARKET",
            size=str(qty),
            trigger_price=str(sl_trigger),
            reduce_only=True,
        )
    except Exception as e:
        print(f"[PROTECT][LADDER] SL create failed bot={b} {direction} {symbol}: {e}")

    tp_oid = (tp_order or {}).get("order_id") or (tp_order or {}).get("id")
    sl_oid = (sl_order or {}).get("order_id") or (sl_order or {}).get("id")

    set_protective_orders(
        bot_id=b,
        symbol=symbol,
        direction=direction,
        sl_order_id=str(sl_oid) if sl_oid else None,
        tp_order_id=str(tp_oid) if tp_oid else None,
        sl_price=sl_trigger,
        tp_price=tp_trigger,
    )

    print(f"[PROTECT][LADDER] bot={b} {direction} {symbol} qty={qty} entry={entry_price} TP_trigger={tp_trigger} SL_trigger={sl_trigger}")


def _ensure_trailing_thread():
    global _TRAIL_THREAD_STARTED
    with _TRAIL_LOCK:
        if _TRAIL_THREAD_STARTED:
            return
        t = threading.Thread(target=_trail_loop, daemon=True)
        t.start()
        _TRAIL_THREAD_STARTED = True
        print("[TRAIL] thread created")


def _recreate_sl_only(bot_id: str, symbol: str, direction: str, qty: Decimal, entry_price: Decimal, lock_pct: Decimal):
    bot_id = _canon_bot_id(bot_id)
    symbol = str(symbol).upper().strip()
    direction = str(direction).upper().strip()
    qty = Decimal(qty)
    entry_price = Decimal(entry_price)
    lock_pct = Decimal(lock_pct)

    if qty <= 0 or entry_price <= 0:
        return

    exit_side = "SELL" if direction == "LONG" else "BUY"

    sl_trigger = _sl_trigger_from_lock(entry_price, direction, lock_pct)

    if PROTECTIVE_ORDER_MODE == "LIMIT":
        sl_type = "STOP_LIMIT"
        if direction == "LONG":
            sl_price = sl_trigger * (Decimal("1") - (PROTECTIVE_SLIPPAGE_PCT / Decimal("100")))
        else:
            sl_price = sl_trigger * (Decimal("1") + (PROTECTIVE_SLIPPAGE_PCT / Decimal("100")))
    else:
        sl_type = "STOP_MARKET"
        sl_price = None

    try:
        sl_trigger = _snap_price_tick(symbol, sl_trigger)
        if sl_price is not None:
            sl_price = _snap_price_tick(symbol, sl_price)
    except Exception:
        pass

    po = get_protective_orders(bot_id, symbol, direction)
    old_sl = po.get("sl_order_id") if isinstance(po, dict) else None
    if old_sl:
        try:
            cancel_order(str(old_sl))
        except Exception:
            pass

    sl = create_trigger_order(
        symbol=symbol,
        side=exit_side,
        order_type=sl_type,
        size=qty,
        trigger_price=sl_trigger,
        price=sl_price,
        reduce_only=True,
        client_id=None,
    )
    sl_oid = str((sl or {}).get("id") or "")
    sl_cid = str((sl or {}).get("clientId") or (sl or {}).get("clientOrderId") or "")

    try:
        set_protective_orders(
            bot_id=bot_id,
            symbol=symbol,
            direction=direction,
            sl_order_id=sl_oid,
            tp_order_id=str(po.get("tp_order_id") or ""),
            sl_client_id=sl_cid,
            tp_client_id=str(po.get("tp_client_id") or ""),
            sl_price=str(sl_trigger),
            tp_price=str(po.get("tp_price") or ""),
        )
        set_lock_level_pct(bot_id, symbol, direction, str(lock_pct))
    except Exception:
        pass

    print(f"[TRAIL] raise SL bot={bot_id} {direction} {symbol} lock={lock_pct}% trigger={sl_trigger} oid={sl_oid}")


def _trail_loop():
    print("[TRAIL] loop started")
    while True:
        try:
            # Iterate over bots with exchange brackets only
            bots = sorted(set(list(LADDER_LONG_BOTS) + list(LADDER_SHORT_BOTS)))
            for bot_id in bots:
                opens = get_bot_open_positions(bot_id)
                if not opens:
                    continue
                for (symbol, direction), info in opens.items():
                    try:
                        qty = Decimal(str(info.get("qty") or "0"))
                        entry_price = Decimal(str(info.get("weighted_entry") or info.get("entry_price") or "0"))
                        if qty <= 0 or entry_price <= 0:
                            continue
                        # mark price (not used as fill price)
                        # worstPrice is used only as a conservative mark/exit estimate (NOT as fill price)
                        rules = _get_symbol_rules(symbol)
                        min_qty = str(rules.get("min_qty") or "0.01")
                        exit_side = "SELL" if direction == "LONG" else "BUY"
                        mp = get_market_price(symbol, exit_side, min_qty)
                        mp = Decimal(str(mp))
                        p = _profit_pct(entry_price, mp, direction)

                        cur_lock = Decimal(str(get_lock_level_pct(bot_id, symbol, direction) or (-Decimal(FIXED_SL_PCT))))
                        desired = _desired_lock_pct_from_profit(p, cur_lock)

                        if desired > cur_lock:
                            _recreate_sl_only(bot_id, symbol, direction, qty, entry_price, desired)

                    except Exception as e:
                        print("[TRAIL] per-position error:", e)
            time.sleep(TRAIL_POLL_INTERVAL_SEC)
        except Exception as e:
            print("[TRAIL] loop error:", e)
            time.sleep(2.0)

def _orders_loop():
    """Background consumer for WS/REST order delta events.

    We subscribe to the private WS order stream (main path) and also run a REST poller (backup path).
    Whenever we receive an order delta fill, we map it back to (bot_id, symbol, direction, kind)
    via pnl_store.protective_orders and then record_exit_fifo() using the real delta fill price.

    This avoids using market quote / worstPrice as execution price.
    """
    print("[WS-ORDERS] loop started (consume order_delta events)")
    while True:
        try:
            evt = pop_order_event(timeout=1.0)
            if not evt:
                continue

            if str(evt.get("type") or "") != "order_delta":
                continue

            order_id = str(evt.get("order_id") or "").strip()
            if not order_id:
                continue

            delta_qty = _to_decimal(evt.get("delta_qty"))
            delta_px = _to_decimal(evt.get("delta_price"))
            if delta_qty is None or delta_px is None:
                continue
            if delta_qty <= 0 or delta_px <= 0:
                continue

            owner = find_protective_owner_by_order_id(order_id)
            if not owner:
                # Not a tracked protective order (could be entry/exit market order); ignore
                continue

            bot_id = str(owner.get("bot_id") or "")
            symbol = str(owner.get("symbol") or "")
            direction = str(owner.get("direction") or "").upper()
            kind = str(owner.get("kind") or "").upper()

            if not bot_id or not symbol or direction not in ("LONG", "SHORT"):
                continue

            entry_side = "BUY" if direction == "LONG" else "SELL"

            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                qty=delta_qty,
                exit_price=delta_px,
                reason=f"protective_{kind or 'order'}",
            )
            print(f"[P&L] exit recorded from protective order bot={bot_id} {direction} {symbol} qty={delta_qty} px={delta_px} kind={kind}")

            # If position is flat after this fill, clean up protective orders + lock levels.
            try:
                open_pos = get_bot_open_positions(bot_id)
                key = f"{symbol}:{direction}"
                remain = open_pos.get(key, {}).get("qty", Decimal("0"))
                if isinstance(remain, str):
                    remain = Decimal(remain)
                if remain is None:
                    remain = Decimal("0")

                if remain <= 0:
                    # cancel the other leg (if still live)
                    other_oid = None
                    if kind == "SL":
                        other_oid = owner.get("tp_order_id")
                    elif kind == "TP":
                        other_oid = owner.get("sl_order_id")
                    if other_oid:
                        try:
                            cancel_order(str(other_oid))
                        except Exception:
                            pass

                    clear_protective_orders(bot_id, symbol, direction)
                    clear_lock_level_pct(bot_id, symbol, direction)
                    print(f"[P&L] cleared protective/lock bot={bot_id} {direction} {symbol} (position flat)")
            except Exception:
                pass

        except Exception as e:
            print("[WS-ORDERS] loop error:", e)
            time.sleep(0.5)


def _ensure_ws_orders_thread():
    global _FILLS_THREAD_STARTED
    with _FILLS_LOCK:
        if _FILLS_THREAD_STARTED:
            return
        t = threading.Thread(target=_orders_loop, daemon=True)
        t.start()
        _FILLS_THREAD_STARTED = True
        print("[WS-ORDERS] thread created")


def _ensure_monitor_thread():
    """
    ✅ 关键：只在 ENABLE_WS=1 的进程里启动 WS + orders 线程
    web(gunicorn) 进程只 init_db，不开 WS
    worker 进程 init_db + 开 WS + 开 orders
    """
    global _MONITOR_THREAD_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_THREAD_STARTED:
            return

        init_db()

        if ENABLE_WS:
            start_private_ws()
            _ensure_ws_orders_thread()

            # ✅ Backup path: REST poll orders every N seconds (main path still WS)
            if str(os.getenv("ENABLE_REST_POLL", "1")).strip() == "1":
                try:
                    start_order_rest_poller(poll_interval=float(os.getenv("REST_ORDER_POLL_INTERVAL", "5.0")))
                    print("[SYSTEM] REST order poller enabled (backup path)")
                except Exception as e:
                    print("[SYSTEM] REST order poller failed to start:", e)

            print("[SYSTEM] WS enabled in this process (ENABLE_WS=1)")
        else:
            print("[SYSTEM] WS disabled in this process (ENABLE_WS=0)")

        _MONITOR_THREAD_STARTED = True
        print("[SYSTEM] monitor/ws threads ready")


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

            try:
                rules = _get_symbol_rules(symbol)
                min_qty = rules["min_qty"]
                exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
                px_str = get_market_price(symbol, exit_side, str(min_qty))
                px = Decimal(str(px_str))
            except Exception:
                continue

            if direction.upper() == "LONG":
                unrealized += (px - wentry) * qty
            else:
                unrealized += (wentry - px) * qty
            po = {}
            try:
                po = get_protective_orders(bot_id, symbol, direction) or {}
            except Exception:
                po = {}

            cur_lock = ""
            next_profit = ""
            next_lock = ""
            next_sl = ""
            try:
                cur_lock = str(get_lock_level_pct(bot_id, symbol, direction) or "")
                if cur_lock != "":
                    np, nl = _next_lock_target(Decimal(cur_lock))
                    next_profit = str(np)
                    next_lock = str(nl)
                    next_sl = str(_sl_trigger_from_lock(wentry, direction, nl))
            except Exception:
                pass

            open_rows.append({
                "symbol": str(symbol).upper(),
                "direction": direction.upper(),
                "qty": str(qty),
                "weighted_entry": str(wentry),
                "mark_price": str(px),
                "profit_pct": str(_profit_pct(wentry, px, direction)),
                "sl_trigger": str(po.get("sl_price") or ""),
                "tp_trigger": str(po.get("tp_price") or ""),
                "lock_level_pct": cur_lock,
                "next_profit_threshold_pct": next_profit,
                "next_lock_level_pct": next_lock,
                "next_sl_trigger": next_sl,
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


@app.route("/api/trades", methods=["GET"])
def api_trades():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    only_bot = request.args.get("bot_id")
    limit = request.args.get("limit", "200")
    try:
        limit_i = int(limit)
        if limit_i < 1:
            limit_i = 200
        limit_i = min(limit_i, 500)
    except Exception:
        limit_i = 200

    if only_bot:
        only_bot = _canon_bot_id(only_bot)

    rows = list_recent_trades(bot_id=only_bot, limit=limit_i)
    return jsonify({"ts": int(time.time()), "trades": rows}), 200


@app.route("/dashboard", methods=["GET"])
def dashboard():
    # ✅ Mobile-friendly dashboard (HTML)
    # Security model:
    # - /api/pnl is protected by DASHBOARD_TOKEN (if set).
    # - /dashboard can load without a token; the page will ask for token if needed.
    _ensure_monitor_thread()

    html = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Apex Bot PnL Dashboard</title>
  <style>
    :root { --bg:#0b1020; --card:#121a33; --muted:#9aa4b2; --text:#eef2ff; --line:#24304f; --bad:#ff6b6b; --good:#6bff95; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; background:var(--bg); color:var(--text); }
    a { color:inherit; }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 16px; }
    .top { display:flex; flex-direction:column; gap:12px; }
    .title { font-size: 20px; font-weight: 700; }
    .sub { color:var(--muted); font-size: 12px; }
    .row { display:flex; gap:12px; flex-wrap:wrap; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:12px; box-shadow: 0 6px 18px rgba(0,0,0,.25); }
    .controls { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
    label { display:block; color:var(--muted); font-size: 12px; margin-bottom:6px; }
    input, select, button { border-radius:10px; border:1px solid var(--line); background:#0f1730; color:var(--text); padding:10px 10px; font-size:14px; }
    input { width: 260px; }
    input.small { width: 140px; }
    button { cursor:pointer; }
    button.primary { background:#1b2a55; }
    button.danger { background:#3a1530; }
    .grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    @media (min-width: 860px) { .grid { grid-template-columns: repeat(5, minmax(0, 1fr)); } }
    .k { color:var(--muted); font-size: 12px; }
    .v { font-size: 18px; font-weight: 700; margin-top: 6px; }
    .v.good { color: var(--good); }
    .v.bad { color: var(--bad); }
    .err { color: var(--bad); font-size: 13px; white-space: pre-wrap; }
    .ok { color: var(--good); font-size: 13px; }
    .tableWrap { overflow:auto; }
    table { width:100%; border-collapse: collapse; min-width: 860px; }
    th, td { text-align:left; padding:10px 10px; border-bottom:1px solid var(--line); font-size: 13px; }
    th { position: sticky; top: 0; background: #0f1730; z-index: 1; }
    tr:hover td { background: rgba(255,255,255,0.03); }
    details { border:1px solid var(--line); border-radius:12px; padding:10px; background:#0f1730; }
    summary { cursor:pointer; font-weight: 700; }
    .pill { display:inline-block; padding:2px 8px; border-radius: 999px; border:1px solid var(--line); color:var(--muted); font-size:12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <div class="title">Apex Bot PnL Dashboard</div>
        <div class="sub">Mobile-friendly view. Data loads from <span class="pill">/api/pnl</span>. If you set <span class="pill">DASHBOARD_TOKEN</span>, paste it below.</div>
      </div>

      <div class="card">
        <div class="controls">
          <div>
            <label>Token (optional; required if DASHBOARD_TOKEN is set)</label>
            <input id="token" placeholder="paste DASHBOARD_TOKEN" />
          </div>
          <div>
            <label>Bot filter (optional)</label>
            <input id="bot" class="small" placeholder="BOT_1" />
          </div>
          <div>
            <label>Auto refresh</label>
            <select id="refresh">
              <option value="0">Off</option>
              <option value="2">2s</option>
              <option value="5" selected>5s</option>
              <option value="10">10s</option>
              <option value="30">30s</option>
            </select>
          </div>
          <div>
            <button class="primary" id="load">Load</button>
            <button id="save">Save Token</button>
            <button class="danger" id="clear">Clear Token</button>
          </div>
          <div style="flex:1"></div>
          <div class="sub" id="status">Idle.</div>
        </div>
      </div>

      <div class="grid" id="cards">
        <div class="card"><div class="k">Realized (24h)</div><div class="v" id="c_day">—</div></div>
        <div class="card"><div class="k">Realized (7d)</div><div class="v" id="c_week">—</div></div>
        <div class="card"><div class="k">Realized (total)</div><div class="v" id="c_total">—</div></div>
        <div class="card"><div class="k">Unrealized (mark)</div><div class="v" id="c_unr">—</div></div>
        <div class="card"><div class="k">Bots / Trades</div><div class="v" id="c_meta">—</div></div>
      </div>

      <div class="card">
        <div class="row" style="align-items:center; justify-content:space-between">
          <div style="font-weight:700">Bots</div>
          <div class="sub" id="updated">Not loaded.</div>
        </div>
        <div class="tableWrap">
          <table>
            <thead>
              <tr>
                <th>bot_id</th>
                <th>realized_day</th>
                <th>realized_week</th>
                <th>realized_total</th>
                <th>unrealized</th>
                <th>trades</th>
                <th>open_positions</th>
              </tr>
            </thead>
            <tbody id="tbody">
              <tr><td colspan="7" class="sub">Click “Load” to fetch data.</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
  <div class="row" style="align-items:center; justify-content:space-between">
    <div>
      <div class="k">Recent trades (realized)</div>
      <div class="sub">Shows the latest closes recorded by the bot (strategy exits, TP/SL order deltas).</div>
    </div>
    <div class="pill">/api/trades</div>
  </div>
  <div class="tableWrap" style="margin-top:12px;">
    <table style="min-width:860px;">
      <thead>
        <tr>
          <th>time</th>
          <th>bot</th>
          <th>symbol</th>
          <th>dir</th>
          <th>qty</th>
          <th>entry</th>
          <th>exit</th>
          <th>pnl</th>
          <th>reason</th>
        </tr>
      </thead>
      <tbody id="tradesBody">
        <tr><td colspan="9" class="sub">Load will also fetch recent trades.</td></tr>
      </tbody>
    </table>
  </div>
</div>

<div class="card">
        <details>
          <summary>Quick links</summary>
          <div class="sub" style="margin-top:10px; line-height:1.6">
            API (all): <a href="/api/pnl" target="_blank">/api/pnl</a><br />
            Trades: <a href="/api/trades" target="_blank">/api/trades</a><br />
            API (single bot): <span class="pill">/api/pnl?bot_id=BOT_1</span><br />
            If token is enabled: add <span class="pill">&amp;token=YOUR_TOKEN</span>
          </div>
        </details>
      </div>

      <div id="msg" class="err" style="display:none"></div>
    </div>
  </div>

  <script>
    const el = (id) => document.getElementById(id);
    const tokenEl = el('token');
    const botEl = el('bot');
    const refreshEl = el('refresh');
    const statusEl = el('status');
    const msgEl = el('msg');

    const fmt = (x) => {
      if (x === null || x === undefined) return '0';
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toFixed(4);
    };
    const clsPNL = (x) => {
      const n = Number(x);
      if (Number.isNaN(n)) return '';
      if (n > 0) return 'good';
      if (n < 0) return 'bad';
      return '';
    };

    function setStatus(s, ok=false) {
      statusEl.textContent = s;
      statusEl.className = ok ? 'ok' : 'sub';
    }

    function setErr(s) {
      if (!s) { msgEl.style.display='none'; msgEl.textContent=''; return; }
      msgEl.style.display='block';
      msgEl.textContent = s;
    }

    function getSavedToken() {
      try { return localStorage.getItem('apex_pnl_token') || ''; } catch(e) { return ''; }
    }
    function saveToken(t) {
      try { localStorage.setItem('apex_pnl_token', t || ''); } catch(e) {}
    }
    function clearToken() {
      try { localStorage.removeItem('apex_pnl_token'); } catch(e) {}
    }

    // preload token from URL or localStorage
    const qs = new URLSearchParams(location.search);
    const qsToken = qs.get('token') || '';
    const qsBot = qs.get('bot_id') || qs.get('bot') || '';
    tokenEl.value = qsToken || getSavedToken();
    botEl.value = qsBot;
    if (qsToken) saveToken(qsToken);

    let timer = null;

    async function fetchPnL() {
      setErr('');
      const token = (tokenEl.value || '').trim();
      const bot = (botEl.value || '').trim();
      const url = new URL('/api/pnl', location.origin);
      if (bot) url.searchParams.set('bot_id', bot);
      if (token) url.searchParams.set('token', token);

      setStatus('Loading...');
      const t0 = Date.now();
      let res;
      try {
        res = await fetch(url.toString(), { method: 'GET' });
      } catch (e) {
        setStatus('Network error');
        setErr(String(e));
        return;
      }
      const dt = Date.now() - t0;

      let data;
      try { data = await res.json(); } catch (e) { data = null; }

      if (!res.ok) {
        setStatus('Failed');
        const hint = (res.status === 403)
          ? '403 Forbidden. If you set DASHBOARD_TOKEN, paste it above (or open /dashboard?token=...).'
          : `HTTP ${res.status}`;
        setErr(hint + (data ? ('\n' + JSON.stringify(data, null, 2)) : ''));
        return;
      }

      const bots = (data && data.bots) ? data.bots : [];

      // totals
      let day=0, week=0, total=0, unr=0, trades=0;
      for (const b of bots) {
        day += Number(b.realized_day || 0);
        week += Number(b.realized_week || 0);
        total += Number(b.realized_total || 0);
        unr += Number(b.unrealized || 0);
        trades += Number(b.trades_count || 0);
      }

      el('c_day').textContent = fmt(day); el('c_day').className = 'v ' + clsPNL(day);
      el('c_week').textContent = fmt(week); el('c_week').className = 'v ' + clsPNL(week);
      el('c_total').textContent = fmt(total); el('c_total').className = 'v ' + clsPNL(total);
      el('c_unr').textContent = fmt(unr); el('c_unr').className = 'v ' + clsPNL(unr);
      el('c_meta').textContent = `${bots.length} / ${trades}`;

      const ts = data && data.ts ? Number(data.ts) : null;
      el('updated').textContent = ts ? ('Updated: ' + new Date(ts*1000).toLocaleString()) : 'Updated.';
      setStatus(`Loaded in ${dt}ms`, true);

      const tbody = el('tbody');
      tbody.innerHTML = '';
      if (!bots.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td colspan="7" class="sub">No bots with activity yet (no lots/exits recorded).</td>`;
        tbody.appendChild(tr);
        return;
      }

      for (const b of bots) {
        const opens = Array.isArray(b.open_positions) ? b.open_positions : [];
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><a href="/dashboard?bot_id=${encodeURIComponent(b.bot_id || '')}${token ? ('&token=' + encodeURIComponent(token)) : ''}">${b.bot_id || ''}</a></td>
          <td class="${clsPNL(b.realized_day)}">${fmt(b.realized_day)}</td>
          <td class="${clsPNL(b.realized_week)}">${fmt(b.realized_week)}</td>
          <td class="${clsPNL(b.realized_total)}">${fmt(b.realized_total)}</td>
          <td class="${clsPNL(b.unrealized)}">${fmt(b.unrealized)}</td>
          <td>${b.trades_count ?? 0}</td>
          <td>${opens.length}</td>
        `;
        tbody.appendChild(tr);

        if (opens.length) {
          const tr2 = document.createElement('tr');
          const rows = opens.map(p => {
  const dir = String(p.direction || '');
  const profit = p.profit_pct || '';
  const sl = p.sl_trigger || '';
  const nextSl = p.next_sl_trigger || '';
  const nextAt = p.next_profit_threshold_pct || '';
  return `<tr>
    <td>${p.symbol || ''}</td>
    <td>${dir}</td>
    <td>${p.qty || ''}</td>
    <td>${p.weighted_entry || ''}</td>
    <td>${p.mark_price || ''}</td>
    <td>${profit}</td>
    <td>${sl}</td>
    <td>${nextSl}</td>
    <td>${nextAt}</td>
  </tr>`;
}).join('');

          tr2.innerHTML = `
            <td colspan="7">
              <details>
                <summary>Open positions for ${b.bot_id} (${opens.length})</summary>
                <div class="tableWrap" style="margin-top:10px;">
                  <table style="min-width:620px;">
                    <thead><tr><th>symbol</th><th>dir</th><th>qty</th><th>entry</th><th>mark</th><th>pnl%</th><th>SL</th><th>next SL</th><th>next @pnl%</th></tr></thead>
                    <tbody>${rows}</tbody>
                  </table>
                </div>
              </details>
            </td>
          `;
          tbody.appendChild(tr2);
        }
      }
    await fetchTrades();
}

async function fetchTrades() {
  const token = tokenEl.value.trim() || getSavedToken();
  if (!token) return;

  const bot = (botEl.value || '').trim();
  let url = '/api/trades?limit=200';
  if (bot) url += `&bot_id=${encodeURIComponent(bot)}`;

  let res;
  try {
    res = await fetch(url, { headers: { 'X-Dashboard-Token': token } });
  } catch (e) {
    return;
  }
  if (!res.ok) return;

  let data;
  try { data = await res.json(); } catch (e) { data = null; }
  const trades = (data && data.trades) ? data.trades : [];

  const tb = el('tradesBody');
  if (!tb) return;
  if (!trades.length) {
    tb.innerHTML = '<tr><td colspan="9" class="sub">No trades yet.</td></tr>';
    return;
  }

  tb.innerHTML = '';
  for (const t of trades) {
    const tr = document.createElement('tr');
    const ts = Number(t.ts || 0);
    const dt = ts ? new Date(ts * 1000).toLocaleString() : '';
    const pnl = Number(t.realized_pnl || 0);
    tr.innerHTML = `
      <td class="sub">${dt}</td>
      <td>${t.bot_id || ''}</td>
      <td>${t.symbol || ''}</td>
      <td>${t.direction || ''}</td>
      <td>${t.exit_qty || ''}</td>
      <td>${t.entry_price || ''}</td>
      <td>${t.exit_price || ''}</td>
      <td class="${clsPNL(pnl)}">${fmt(pnl)}</td>
      <td class="sub">${t.reason || ''}</td>
    `;
    tb.appendChild(tr);
  }
}

function setAutoRefresh(
) {
      if (timer) { clearInterval(timer); timer = null; }
      const sec = Number(refreshEl.value || 0);
      if (sec > 0) timer = setInterval(fetchPnL, sec * 1000);
    }

    el('load').addEventListener('click', () => { fetchPnL(); });
    el('save').addEventListener('click', () => { saveToken((tokenEl.value||'').trim()); setStatus('Token saved.', true); });
    el('clear').addEventListener('click', () => { tokenEl.value=''; clearToken(); setStatus('Token cleared.', true); });
    refreshEl.addEventListener('change', setAutoRefresh);

    setAutoRefresh();
    // auto-load once on open
    fetchPnL();
  </script>
</body>
</html>"""

    return Response(html, mimetype="text/html")


@app.route("/webhook", methods=["POST"])
def tv_webhook():
    _ensure_monitor_thread()

    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        print("[WEBHOOK] invalid json:", e)
        return "invalid json", 400

    print("[WEBHOOK] raw body:", body)

    if not isinstance(body, dict):
        return "bad payload", 400

    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        print("[WEBHOOK] invalid secret")
        return "forbidden", 403

    symbol = body.get("symbol")
    if not symbol:
        return "missing symbol", 400
    symbol = str(symbol).upper().strip()

    bot_id = _canon_bot_id(body.get("bot_id", "BOT_1"))
    side_raw = str(body.get("side", "")).upper().strip()
    signal_type_raw = str(body.get("signal_type", "")).lower().strip()
    action_raw = str(body.get("action", "")).lower().strip()
    tv_client_id = body.get("client_id")

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

    sig_id = _get_signal_id(body, mode, bot_id, symbol)
    if is_signal_processed(bot_id, sig_id):
        print(f"[WEBHOOK] dedup: bot={bot_id} symbol={symbol} mode={mode} sig={sig_id}")
        return jsonify({"status": "dedup", "mode": mode, "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}), 200
    mark_signal_processed(bot_id, sig_id, kind=f"webhook_{mode}")

    # -------------------------
    # ENTRY
    # -------------------------
    if mode == "entry":
        expected = _bot_expected_entry_side(bot_id)
        if expected is not None:
            if side_raw and side_raw != expected:
                return jsonify({
                    "status": "rejected_wrong_direction",
                    "bot_id": bot_id,
                    "symbol": symbol,
                    "expected_side": expected,
                    "got_side": side_raw,
                    "signal_id": sig_id,
                }), 200
            side_raw = expected

        if side_raw not in ("BUY", "SELL"):
            return "missing or invalid side", 400

        # ✅ Constraint A: global same-symbol same-direction (across all bots)
        desired_dir = "LONG" if side_raw == "BUY" else "SHORT"
        open_dirs = get_symbol_open_directions(symbol)
        if len(open_dirs) > 1:
            # This should not happen; fail-closed to avoid netting issues.
            return jsonify({
                "status": "reject_direction_conflict",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "desired_direction": desired_dir,
                "open_directions": sorted(list(open_dirs)),
                "signal_id": sig_id,
            }), 200
        if len(open_dirs) == 1 and desired_dir not in open_dirs:
            return jsonify({
                "status": "reject_opposite_direction_locked",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "desired_direction": desired_dir,
                "locked_direction": list(open_dirs)[0],
                "signal_id": sig_id,
            }), 200

        try:
            budget = _extract_budget_usdt(body)
        except Exception as e:
            print("[ENTRY] budget error:", e)
            return str(e), 400

        try:
            snapped_qty = _compute_entry_qty(symbol, side_raw, budget)
        except Exception as e:
            print("[ENTRY] qty compute error:", e)
            return "qty compute error", 500

        size_str = str(snapped_qty)
        print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} budget={budget} -> qty={size_str} sig={sig_id}")

        # Deterministic numeric clientId for attribution: BBB + ts + 01 (ENTRY)
        bnum = _bot_num(bot_id)
        ts_now = int(time.time())
        entry_client_id = f"{bnum:03d}{ts_now}01"


        # Snapshot current position size BEFORE placing the order.
        # This makes position-fallback safer when multiple bots can trade the same symbol.
        pre_pos_size = Decimal("0")
        try:
            pre = get_open_position_for_symbol(symbol)
            if isinstance(pre, dict) and pre.get("size") is not None:
                if side_raw == "BUY" and str(pre.get("side", "")).upper() == "LONG":
                    pre_pos_size = Decimal(str(pre["size"]))
                elif side_raw == "SELL" and str(pre.get("side", "")).upper() == "SHORT":
                    pre_pos_size = Decimal(str(pre["size"]))
        except Exception:
            pre_pos_size = Decimal("0")


        try:
            order = create_market_order(
                symbol=symbol,
                side=side_raw,
                size=size_str,
                reduce_only=False,
                client_id=entry_client_id,
            )
        except Exception as e:
            print("[ENTRY] create_market_order error:", e)
            return "order error", 500

        status, cancel_reason = _order_status_and_reason(order)
        print(f"[ENTRY] order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return jsonify({
                "status": "order_rejected",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "request_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "signal_id": sig_id,
            }), 200

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        entry_price_dec: Optional[Decimal] = None
        final_qty = snapped_qty

        # ✅ Primary: order-based fill summary (from WS order channel)
        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
                max_wait_sec=float(os.getenv("FILL_MAX_WAIT_SEC", "20.0")),
                poll_interval=float(os.getenv("FILL_POLL_INTERVAL", "0.25")),
            )
            entry_price_dec = Decimal(str(fill["avg_fill_price"]))
            final_qty = Decimal(str(fill["filled_qty"]))
            print(f"[ENTRY] fill ok bot={bot_id} symbol={symbol} filled_qty={final_qty} avg_fill={entry_price_dec}")
        except Exception as e:
            print(f"[ENTRY] fill fallback bot={bot_id} symbol={symbol} err:", e)

            # Fallback: use position entryPrice snapshot if order detail is delayed.
            try:
                pos_wait = float(os.getenv("POSITION_FALLBACK_WAIT_SEC", "8.0"))
                pos_poll = float(os.getenv("POSITION_FALLBACK_POLL_INTERVAL", "0.5"))
                deadline = time.time() + pos_wait

                while time.time() < deadline and entry_price_dec is None:
                    pos = get_open_position_for_symbol(symbol)
                    if isinstance(pos, dict):
                        sz = pos.get("size")
                        ep = pos.get("entryPrice") or pos.get("entry_price") or pos.get("avgEntryPrice")
                        try:
                            dsz = Decimal(str(sz)) if sz is not None else Decimal("0")
                        except Exception:
                            dsz = Decimal("0")

                        if dsz > 0 and ep not in (None, "", "0", "0.0"):
                            entry_price_dec = Decimal(str(ep))
                            # Use delta vs pre-order position size, capped at requested qty.
                            try:
                                delta_sz = (dsz - pre_pos_size)
                            except Exception:
                                delta_sz = dsz
                            if delta_sz <= 0:
                                final_qty = snapped_qty
                            else:
                                final_qty = min(snapped_qty, delta_sz)

                            print(f"[ENTRY] position fallback bot={bot_id} symbol={symbol} posSize={dsz} prePos={pre_pos_size} delta={delta_sz} final_qty={final_qty} entryPrice={entry_price_dec}")
                            break

                    time.sleep(pos_poll)

            except Exception as e2:
                print(f"[ENTRY] position fallback failed bot={bot_id} symbol={symbol} err:", e2)

        key = (bot_id, symbol)
        BOT_POSITIONS[key] = {"side": side_raw, "qty": final_qty, "entry_price": entry_price_dec}

        if entry_price_dec is not None and final_qty > 0:
            try:
                record_entry(
                    bot_id=bot_id,
                    symbol=symbol,
                    side=side_raw,
                    qty=final_qty,
                    price=entry_price_dec,
                    reason="strategy_entry_fill",
                )
            except Exception as e:
                print("[PNL] record_entry error:", e)

        # ✅ BOT_1-20：挂交易所 TP/SL（reduceOnly=True）
        if _bot_has_exchange_brackets(bot_id) and entry_price_dec is not None and final_qty > 0:
            direction = "LONG" if side_raw == "BUY" else "SHORT"
            try:
                _place_fixed_brackets(bot_id, symbol, direction, final_qty, entry_price_dec)
            except Exception as e:
                print("[BRACKET] place error:", e)

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(final_qty),
            "entry_price": str(entry_price_dec) if entry_price_dec else None,
            "requested_qty": size_str,
            "order_status": status,
            "cancel_reason": cancel_reason,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "signal_id": sig_id,
        }), 200


    # -------------------------
    # EXIT（策略出场 / TV 出场）
    # -------------------------
    if mode == "exit":
        if not _exit_guard_allow(bot_id, symbol):
            print(f"[EXIT] SKIP (cooldown) bot={bot_id} symbol={symbol} sig={sig_id}")
            return jsonify({"status": "cooldown_skip", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}), 200

        opens = get_bot_open_positions(bot_id)
        long_key = (symbol, "LONG")
        short_key = (symbol, "SHORT")

        long_qty = opens.get(long_key, {}).get("qty", Decimal("0"))
        short_qty = opens.get(short_key, {}).get("qty", Decimal("0"))

        key_local = (bot_id, symbol)
        local = BOT_POSITIONS.get(key_local)
        preferred = "LONG"
        if local and str(local.get("side", "")).upper() == "SELL":
            preferred = "SHORT"

        direction_to_close = None
        qty_to_close = Decimal("0")

        if preferred == "LONG" and long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif preferred == "SHORT" and short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty
        elif long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty

        if (not direction_to_close or qty_to_close <= 0):
            if symbol in REMOTE_FALLBACK_SYMBOLS:
                remote = get_open_position_for_symbol(symbol)
                if remote and remote["size"] > 0:
                    direction_to_close = remote["side"]
                    qty_to_close = remote["size"]
                else:
                    return jsonify({"status": "no_position"}), 200
            else:
                return jsonify({"status": "no_position"}), 200

        # ✅ 手动/TV exit：先取消交易所挂的 TP/SL
        try:
            _cancel_existing_brackets(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        entry_side = "BUY" if direction_to_close == "LONG" else "SELL"
        exit_side = "SELL" if direction_to_close == "LONG" else "BUY"

        # Deterministic numeric clientId for attribution: BBB + ts + 02 (EXIT)
        bnum = _bot_num(bot_id)
        ts_now = int(time.time())
        exit_client_id = f"{bnum:03d}{ts_now}02"

        try:
            order = create_market_order(
                symbol=symbol,
                side=exit_side,
                size=str(qty_to_close),
                reduce_only=True,
                client_id=exit_client_id,
            )
        except Exception as e:
            print("[EXIT] create_market_order error:", e)
            return "order error", 500

        status, cancel_reason = _order_status_and_reason(order)
        print(f"[EXIT] order status={status} cancelReason={cancel_reason!r}")

        if status in ("CANCELED", "REJECTED"):
            return jsonify({
                "status": "exit_rejected",
                "mode": "exit",
                "bot_id": bot_id,
                "symbol": symbol,
                "exit_side": exit_side,
                "requested_qty": str(qty_to_close),
                "order_status": status,
                "cancel_reason": cancel_reason,
                "signal_id": sig_id,
            }), 200

        exit_price = None
        filled_qty = qty_to_close

        try:
            fill = get_fill_summary(
                symbol=symbol,
                order_id=order.get("order_id"),
                client_order_id=order.get("client_order_id"),
                max_wait_sec=float(os.getenv("FILL_MAX_WAIT_SEC", "20.0")),
                poll_interval=float(os.getenv("FILL_POLL_INTERVAL", "0.25")),
            )
            exit_price = Decimal(str(fill["avg_fill_price"]))
            filled_qty = Decimal(str(fill["filled_qty"]))
            print(f"[EXIT] fill ok bot={bot_id} symbol={symbol} filled_qty={filled_qty} avg_fill={exit_price}")
        except Exception as e:
            print(f"[EXIT] fill wait failed bot={bot_id} symbol={symbol} err:", e)
            # Last resort: mark price for bookkeeping ONLY if absolutely necessary
            try:
                rules = _get_symbol_rules(symbol)
                min_qty = rules["min_qty"]
                px_str = get_market_price(symbol, exit_side, str(min_qty))
                exit_price = Decimal(str(px_str))
            except Exception:
                exit_price = Decimal("0.00000001")

        try:
            record_exit_fifo(
                bot_id=bot_id,
                symbol=symbol,
                entry_side=entry_side,
                exit_qty=filled_qty,
                exit_price=exit_price,
                reason="strategy_exit",
            )
        except Exception as e:
            print("[PNL] record_exit_fifo error (strategy):", e)

        try:
            clear_lock_level_pct(bot_id, symbol, direction_to_close)
        except Exception:
            pass

        # Refresh local cache based on DB remaining
        try:
            opens2 = get_bot_open_positions(bot_id)
            rem = opens2.get((symbol, direction_to_close), {}).get("qty", Decimal("0"))
        except Exception:
            rem = Decimal("0")

        if key_local in BOT_POSITIONS:
            BOT_POSITIONS[key_local]["qty"] = rem
            if rem <= 0:
                BOT_POSITIONS[key_local]["entry_price"] = None

        return jsonify({
            "status": "ok",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "closed_qty": str(qty_to_close),
            "order_status": status,
            "cancel_reason": cancel_reason,
            "signal_id": sig_id,
        }), 200

    return "unsupported mode", 400


if __name__ == "__main__":
    _ensure_monitor_thread()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
