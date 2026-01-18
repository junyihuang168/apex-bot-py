import os
import time
import threading
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Dict, Tuple, Optional, Set, Any, List

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    create_trigger_order,
    get_market_price,
    get_reference_price,
    get_l1_bid_ask,
    ensure_public_depth_subscription,
    start_public_ws,
    get_fill_summary,
    snap_price,
    cancel_order_by_client_id,
    cancel_order,
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
    get_protective_orders,
    set_protective_orders,
    clear_protective_orders,
    list_active_protective_orders,
)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ✅ 只让 worker 进程启用 WS/Fills（supervisord.conf 里给 worker 设置 ENABLE_WS="1"，web 设置 "0"）
ENABLE_WS = str(os.getenv("ENABLE_WS", "0")).strip() == "1"
ENABLE_RISK_LOOP = str(os.getenv("ENABLE_RISK_LOOP", "0")).strip() == "1"  # run ladder risk loop in this process

ENABLE_EXCHANGE_STOP = str(os.getenv("ENABLE_EXCHANGE_STOP", "0")).strip() == "1"  # use exchange-native STOP_MARKET trigger
RISK_PRICE_SOURCE = str(os.getenv("RISK_PRICE_SOURCE", "L1_BIDASK")).strip().upper()  # L1_BIDASK / LAST / MARK

# 退出互斥窗口（秒）：防止重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}

# ✅ 说明：本版本支持“交易所原生止损单(Trigger STOP_MARKET)”用于移动止损；
#    - 由 worker 后台循环根据锁盈阶梯更新 stop triggerPrice
#    - 触发后由 worker 兜底对账并写入 PnL (无需 TradingView 额外发 exit 警报)

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
    if isinstance(x, str) and x.strip() == "":
        return default
    try:
        return Decimal(str(x))
    except Exception:
        return default


#+#+#+#+########################################
# BOT GROUPS
#
# Per your requirement:
# - Keep bot-side SL/TP ONLY for: BOT_1–5 (LONG) and BOT_11–15 (SHORT)
# - Remove bot-side SL/TP for: BOT_6–10 and BOT_16–20 (they remain signal-driven only)
#
# Note: In this repo, the only bot-side SL/TP implementation is the ladder-stop loop below.
#+#+#+#+########################################

# ----------------------------
# ✅ BOT 分组（按你要求）
# ----------------------------

_ALLOWED_LONG_TPSL = {f"BOT_{i}" for i in range(1, 6)}
_ALLOWED_SHORT_TPSL = {f"BOT_{i}" for i in range(11, 16)}

# “TPSL bots” here means: bots that are allowed to have bot-side SL/TP logic enabled.
# (In this repo, the only bot-side SL/TP is the ladder stop loop below.)
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

LADDER_LONG_BOTS  = _parse_bot_list(os.getenv("LADDER_LONG_BOTS",  ",".join(sorted(_ALLOWED_LONG_TPSL)))) & _ALLOWED_LONG_TPSL
LADDER_SHORT_BOTS = _parse_bot_list(os.getenv("LADDER_SHORT_BOTS", ",".join(sorted(_ALLOWED_SHORT_TPSL)))) & _ALLOWED_SHORT_TPSL

# Base stop-loss (percent). Example 0.5 -> -0.5% initial lock.
LADDER_BASE_SL_PCT = Decimal(os.getenv("LADDER_BASE_SL_PCT", "0.5"))

# Profit% : lock% mapping (both in percent, not decimals)
# Default (your old ladder):
# 0.15 -> +0.125
# 0.35 -> +0.15
# 0.55 -> +0.35
# 0.75 -> +0.55
# 0.95 -> +0.75
_DEFAULT_LADDER_LEVELS = "0.15:0.125,0.35:0.15,0.55:0.35,0.75:0.55,0.95:0.75"
LADDER_LEVELS_RAW = os.getenv("LADDER_LEVELS", _DEFAULT_LADDER_LEVELS)

# Risk poll interval (seconds)
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.0"))
LADDER_DEBUG = str(os.getenv("LADDER_DEBUG", "0")).strip() == "1"
LADDER_DEBUG_EVERY_SEC = float(os.getenv("LADDER_DEBUG_EVERY_SEC", "10.0"))
_LADDER_STATUS_TS = {}  # key -> last print ts

# L1 (best bid/ask) settings for ladder risk checks
L1_STALE_SEC = float(os.getenv("L1_STALE_SEC", "2.0"))
L1_FALLBACK_TO_MARK = str(os.getenv("L1_FALLBACK_TO_MARK", "1")).strip() == "1"

# Entry sizing (no leverage by default):
# - Orders are placed by quantity. Quantity must satisfy stepSize/minQty.
# - For low-price symbols with large minQty, a small notional may be infeasible.
#   If ENTRY_AUTO_UPSIZE_TO_MIN_QTY=1, we will upsize to the minimum tradable qty.
ENTRY_AUTO_UPSIZE_TO_MIN_QTY = str(os.getenv("ENTRY_AUTO_UPSIZE_TO_MIN_QTY", "1")).strip() == "1"
# Hard cap for upsized notional (USDT). 0 = no cap.
ENTRY_MAX_NOTIONAL_USDT = Decimal(os.getenv("ENTRY_MAX_NOTIONAL_USDT", "0"))
# Safety margin used when reporting minimum required notional.
ENTRY_MIN_NOTIONAL_MARGIN_PCT = Decimal(os.getenv("ENTRY_MIN_NOTIONAL_MARGIN_PCT", "0.03"))


# Ticker fallback cache for risk checks (prevents 'NO_PRICE' when public WS is unstable)
RISK_TICKER_REFRESH_SEC = float(os.getenv("RISK_TICKER_REFRESH_SEC", "0.75"))
RISK_TICKER_STALE_SEC = float(os.getenv("RISK_TICKER_STALE_SEC", "30.0"))
_RISK_TICKER_CACHE: Dict[str, Tuple[Decimal, float]] = {}  # symbol -> (price, ts)
_RISK_TICKER_LOCK = threading.Lock()



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
    """Best-effort mark/index/oracle/last price for risk checks.

    Notes:
    - Different apexomni builds return different field names in the position payload.
    - For ladder SL/TS we only need a reasonable realtime reference price.
    - If the private position payload doesn't include a mark-like price, we fall back to
      the public ticker reference price.
    """
    try:
        pos = get_open_position_for_symbol(symbol)
        if isinstance(pos, dict):
            # Common variants seen across builds
            for k in (
                "markPrice",
                "mark_price",
                "oraclePrice",
                "oracle_price",
                "indexPrice",
                "index_price",
                "lastPrice",
                "last_price",
                "price",
            ):
                v = pos.get(k)
                if v is None or v == "":
                    continue
                try:
                    dv = Decimal(str(v))
                    if dv > 0:
                        return dv
                except Exception:
                    continue
    except Exception:
        # We'll still try public fallback below.
        pass

    # Public fallback (no auth): index/mark/last from ticker.
    try:
        ref = get_reference_price(symbol)
        return ref if ref > 0 else None
    except Exception:
        return None


def _get_l1_risk_price(symbol: str, direction: str) -> Tuple[Optional[Decimal], str, Optional[Decimal], Optional[Decimal]]:
    """Return (ref_price, source, best_bid, best_ask).

    Ladder risk checks use:
      - LONG  -> best_bid
      - SHORT -> best_ask

    Priority order:
      1) Fresh L1 (public WS)
      2) Cached ticker/mark (fast, avoids NO_PRICE)
      3) Live ticker (REST)
      4) Mark/position-derived (private REST)
      5) Last cached price within RISK_TICKER_STALE_SEC

    This makes risk management resilient when the public WS connection is flaky.
    """
    sym = str(symbol).upper().strip()
    dir_u = str(direction or "").upper().strip()

    # Ensure subscription exists (idempotent). Public WS is started in worker.
    try:
        ensure_public_depth_subscription(sym, limit=25, speed="H")
    except Exception:
        pass

    bid, ask, ts = None, None, 0.0
    try:
        bid, ask, ts = get_l1_bid_ask(sym)
    except Exception:
        bid, ask, ts = None, None, 0.0

    now = time.time()
    if bid is not None and ask is not None and bid > 0 and ask > 0 and (now - float(ts)) <= L1_STALE_SEC:
        if dir_u == "LONG":
            return bid, "L1_BID", bid, ask
        return ask, "L1_ASK", bid, ask

    # 2) Use cached ticker if still fresh enough (prevents blocking on REST during spikes)
    with _RISK_TICKER_LOCK:
        cached = _RISK_TICKER_CACHE.get(sym)
    if cached:
        cpx, cts = cached
        if cpx is not None and cpx > 0 and (now - float(cts)) <= RISK_TICKER_REFRESH_SEC:
            if dir_u == "LONG":
                return cpx, "TICKER_CACHE", bid, ask
            return cpx, "TICKER_CACHE", bid, ask

    # 3) Live ticker (REST)
    try:
        ref = get_reference_price(sym)
        if ref is not None and ref > 0:
            with _RISK_TICKER_LOCK:
                _RISK_TICKER_CACHE[sym] = (ref, now)
            return ref, "TICKER_REST", bid, ask
    except Exception:
        pass

    # 4) Mark/position-derived (private REST), optional
    if L1_FALLBACK_TO_MARK:
        try:
            m = _get_mark_price(sym)
            if m is not None and m > 0:
                with _RISK_TICKER_LOCK:
                    _RISK_TICKER_CACHE[sym] = (m, now)
                return m, "MARK_FALLBACK", bid, ask
        except Exception:
            pass

    # 5) Last cached (stale) within tolerance
    if cached:
        cpx, cts = cached
        if cpx is not None and cpx > 0 and (now - float(cts)) <= RISK_TICKER_STALE_SEC:
            return cpx, "TICKER_CACHE_STALE", bid, ask

    return None, "NO_PRICE", bid, ask


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



def _bot_expected_entry_side(bot_id: str) -> Optional[str]:
    b = _canon_bot_id(bot_id)
    if b in LONG_TPSL_BOTS or b in LONG_PNL_ONLY_BOTS:
        return "BUY"
    if b in SHORT_TPSL_BOTS or b in SHORT_PNL_ONLY_BOTS:
        return "SELL"
    return None



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
    """Extract requested USDT size from webhook body.

    Backward/forward compatible with multiple field names seen in different
    TradingView/ApeX webhook formats.
    """
    size_field = (
        body.get("position_size_usdt")
        or body.get("size_usdt")
        or body.get("qty_usdt")          # your current payload
        or body.get("budget_usdt")
        or body.get("usdt")
        or body.get("size")
        or body.get("qty")               # last resort (some send qty as USDT by mistake)
    )
    if size_field is None:
        raise ValueError("missing position_size_usdt / size_usdt / qty_usdt / budget_usdt / size")

    budget = Decimal(str(size_field))
    if budget <= 0:
        raise ValueError("size_usdt must be > 0")
    return budget



# ----------------------------
# 预算 -> snapped qty
# ----------------------------
def _compute_entry_qty(symbol: str, side: str, size_usdt: Any) -> Decimal:
    """Compute order size (contract qty) from a USDT notional.

    Notes:
      - ApeX places orders by *quantity*. Quantity must satisfy stepSize/minQty.
      - For very low-price symbols, minQty can be large. If the requested notional
        is too small, we can optionally upsize to the minimum tradable quantity.

    Reference price source:
      - `get_market_price()` (prefers L1 when available; otherwise mark/ticker fallback)
    """
    sym = str(symbol).upper().strip()
    side_u = str(side).upper().strip()

    rules = _get_symbol_rules(sym)
    step = Decimal(str(rules.get("step_size") or "1"))
    min_qty = Decimal(str(rules.get("min_qty") or "0"))

    budget_usdt = _to_decimal(size_usdt, default=Decimal("0"))
    if budget_usdt <= 0:
        raise ValueError(f"invalid size_usdt: {size_usdt}")

    # Reference price (bid/ask preferred when L1 is available inside get_market_price)
    ref_price_str = get_market_price(sym, side_u, str(min_qty))
    ref_price_dec = _to_decimal(ref_price_str, default=Decimal("0"))
    if ref_price_dec <= 0:
        raise RuntimeError(f"invalid reference price for qty compute: {ref_price_str}")

    # Floor to step (exchange-style)
    qty_raw = budget_usdt / ref_price_dec
    qty_floor = (qty_raw // step) * step
    qty_floor = qty_floor.quantize(step, rounding=ROUND_DOWN)

    if qty_floor >= min_qty and qty_floor > 0:
        return qty_floor

    # Too small: compute minimum tradable qty (ceil minQty to step)
    if min_qty <= 0:
        raise ValueError(f"symbol rules invalid: min_qty={min_qty} step={step}")

    min_steps = (min_qty / step).to_integral_value(rounding=ROUND_UP)
    qty_min = (min_steps * step).quantize(step, rounding=ROUND_DOWN)

    est_notional = (qty_min * ref_price_dec).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    min_needed = (est_notional * (Decimal("1") + ENTRY_MIN_NOTIONAL_MARGIN_PCT)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)

    if not ENTRY_AUTO_UPSIZE_TO_MIN_QTY:
        raise ValueError(
            f"budget too small for {sym}: budget={budget_usdt} < min_needed~{min_needed} "
            f"(minQty={min_qty}, step={step}, ref={ref_price_dec})"
        )

    if ENTRY_MAX_NOTIONAL_USDT > 0 and est_notional > ENTRY_MAX_NOTIONAL_USDT:
        raise ValueError(
            f"budget too small for {sym}: min_notional~{min_needed} exceeds cap "
            f"ENTRY_MAX_NOTIONAL_USDT={ENTRY_MAX_NOTIONAL_USDT}"
        )

    print(
        f"[ENTRY] upsize_to_minQty symbol={sym} side={side_u} "
        f"budget={budget_usdt} -> qty={qty_min} est_notional={est_notional} "
        f"(minQty={min_qty} step={step} ref={ref_price_dec})"
    )
    return qty_min


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


# ----------------------------
# ✅ Ladder risk loop (bot-side close via reduceOnly market)
# ----------------------------
_RISK_THREAD_STARTED = False
_RISK_LOCK = threading.Lock()


def _ladder_desired_lock_pct(profit_pct: Decimal) -> Optional[Decimal]:
    """Return the desired lock pct given current profit pct."""
    desired = None
    for p_th, lock in LADDER_LEVELS:
        if profit_pct >= p_th:
            desired = lock
        else:
            break
    return desired


def _maybe_raise_lock(bot_id: str, symbol: str, direction: str, profit_pct: Decimal):
    # current lock
    try:
        cur = get_lock_level_pct(bot_id, symbol, direction)
    except Exception:
        cur = None

    if cur is None:
        cur = -LADDER_BASE_SL_PCT
        try:
            set_lock_level_pct(bot_id, symbol, direction, cur)
        except Exception:
            return cur

    desired = _ladder_desired_lock_pct(profit_pct)
    if desired is not None and desired > cur:
        try:
            set_lock_level_pct(bot_id, symbol, direction, desired)
            print(
                f"[LADDER] RAISE_LOCK bot={bot_id} {direction} {symbol} profit%={profit_pct:.4f} lock% {cur} -> {desired}"
            )
            return desired
        except Exception:
            return cur
    return cur


def _ladder_should_stop(direction: str, mark: Decimal, stop_price: Decimal) -> bool:
    if mark <= 0 or stop_price <= 0:
        return False
    d = str(direction).upper()
    if d == "LONG":
        return mark <= stop_price
    else:
        return mark >= stop_price


def _ladder_close_position(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    if qty <= 0:
        return

    entry_side = "BUY" if direction == "LONG" else "SELL"
    exit_side = "SELL" if direction == "LONG" else "BUY"

    # Deterministic numeric clientId: BBB + ts + 90 (ladder)
    bnum = _bot_num(bot_id)
    ts_now = int(time.time())
    exit_client_id = f"{bnum:03d}{ts_now}90"

    try:
        order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=exit_client_id,
        )
    except Exception as e:
        print(f"[LADDER] close order error bot={bot_id} {direction} {symbol} qty={qty}: {e}")
        return

    try:
        fill = get_fill_summary(
            symbol=symbol,
            order_id=order.get("order_id"),
            client_order_id=order.get("client_order_id"),
            max_wait_sec=float(os.getenv("FILL_MAX_WAIT_SEC", "25.0")),
            poll_interval=float(os.getenv("FILL_POLL_INTERVAL", "0.25")),
        )
        exit_price = Decimal(str(fill["avg_fill_price"]))
        filled_qty = Decimal(str(fill["filled_qty"]))
    except Exception as e:
        print(f"[LADDER] fill unavailable bot={bot_id} {direction} {symbol}: {e}")
        # Fail-closed: do not write a fake price. We simply do not record.
        return

    try:
        record_exit_fifo(
            bot_id=bot_id,
            symbol=symbol,
            entry_side=entry_side,
            exit_qty=filled_qty,
            exit_price=exit_price,
            reason=reason,
        )
    except Exception as e:
        print("[LADDER] record_exit_fifo error:", e)

    try:
        opens2 = get_bot_open_positions(bot_id)
        rem = opens2.get((symbol, direction), {}).get("qty", Decimal("0"))
        if rem <= 0:
            clear_lock_level_pct(bot_id, symbol, direction)
    except Exception:
        pass

    # local cache
    try:
        key_local = (bot_id, symbol)
        if key_local in BOT_POSITIONS:
            BOT_POSITIONS[key_local]["qty"] = Decimal("0")
            BOT_POSITIONS[key_local]["entry_price"] = None
    except Exception:
        pass


# --- Exchange-native stop helpers ---
# We manage ONE reduce-only STOP_MARKET per (bot, symbol, direction).
# The stop trigger is updated as lock level increases.

_EX_STOP_MIN_MOVE_TICKS = int(os.getenv("EX_STOP_MIN_MOVE_TICKS", "1"))


def _ex_stop_client_id(bot_id: str, symbol: str, direction: str) -> str:
    b = _canon_bot_id(bot_id)
    s = str(symbol).upper().replace("-", "")
    d = str(direction).upper()
    return f"sl-{b}-{s}-{d}"


def _should_update_stop(old_price: Optional[Decimal], new_price: Decimal, tick: Decimal) -> bool:
    if old_price is None or old_price <= 0:
        return True
    try:
        diff = abs(new_price - old_price)
        return diff >= (tick * Decimal(str(max(1, _EX_STOP_MIN_MOVE_TICKS))))
    except Exception:
        return True


def _ensure_exchange_stop_order(bot_id: str, symbol: str, direction: str, qty: Decimal, stop_price: Decimal) -> None:
    """Cancel+replace reduce-only STOP_MARKET trigger order using LAST price trigger."""
    if qty <= 0 or stop_price <= 0:
        return

    rules = _get_symbol_rules(symbol)
    tick = Decimal(str(rules.get("tick_size") or "0.01"))
    stop_price = snap_price(symbol, stop_price)

    # Load current stored protective state
    po = get_protective_orders(bot_id, symbol, direction) or {}
    old_sl_id = str(po.get("sl_order_id") or "").strip()
    old_sl_price = po.get("sl_price")
    try:
        old_sl_price_dec = Decimal(str(old_sl_price)) if old_sl_price not in (None, "") else None
    except Exception:
        old_sl_price_dec = None

    if not _should_update_stop(old_sl_price_dec, stop_price, tick):
        return

    # Cancel old stop order if present
    if old_sl_id:
        try:
            cancel_order(old_sl_id)
        except Exception:
            pass

    # Place new STOP_MARKET trigger
    close_side = "SELL" if direction.upper() == "LONG" else "BUY"
    client_id = _ex_stop_client_id(bot_id, symbol, direction)

    try:
        res = create_trigger_order(
            symbol=symbol,
            side=close_side,
            qty=str(qty),
            trigger_price=str(stop_price),
            reduce_only=True,
            client_order_id=client_id,
            trigger_price_type="LAST",
        )
    except Exception as e:
        print(f"[EXSTOP] create_trigger_order failed bot={bot_id} {direction} {symbol} stop={stop_price}: {e}")
        return

    # Try extract order id
    order_id = ""
    try:
        data = res.get("data") if isinstance(res, dict) else None
        if isinstance(data, dict):
            order_id = str(data.get("orderId") or data.get("id") or "")
        if not order_id:
            order_id = str(res.get("order_id") or res.get("orderId") or "")
    except Exception:
        order_id = ""

    set_protective_orders(
        bot_id=bot_id,
        symbol=symbol,
        direction=direction,
        sl_order_id=order_id,
        tp_order_id=str(po.get("tp_order_id") or ""),
        sl_client_id=client_id,
        tp_client_id=str(po.get("tp_client_id") or ""),
        sl_price=str(stop_price),
        tp_price=str(po.get("tp_price") or ""),
        is_active=1,
    )

    if LADDER_DEBUG:
        print(f"[EXSTOP] UPDATED bot={bot_id} {direction} {symbol} qty={qty} stop={stop_price} sl_id={order_id}")


def _reconcile_exchange_stop_fills() -> None:
    """If exchange-native stop filled (position closed), fetch fills and record exit."""
    try:
        rows = list_active_protective_orders(limit=200)
    except Exception:
        return

    for r in rows:
        try:
            bot_id = _canon_bot_id(r.get("bot_id"))
            symbol = str(r.get("symbol") or "").upper().strip()
            direction = str(r.get("direction") or "").upper().strip()
            sl_order_id = str(r.get("sl_order_id") or "").strip()
            if not bot_id or not symbol or not direction or not sl_order_id:
                continue

            # Check exchange position
            pos = None
            try:
                pos = get_open_position_for_symbol(symbol)
            except Exception:
                pos = None

            # If position still open, nothing to do
            if pos and isinstance(pos, dict):
                size = _to_decimal(pos.get("size") or pos.get("positionSize") or pos.get("qty"), Decimal("0"))
                if abs(size) > Decimal("0"):
                    continue

            # Attempt fetch fill summary for the stop order
            try:
                fill = get_fill_summary(symbol=symbol, order_id=sl_order_id, client_order_id=str(r.get("sl_client_id") or ""), max_wait_sec=0.5, poll_interval=0.2)
                filled_qty = _to_decimal(fill.get("filled_qty"), Decimal("0"))
                exit_price = _to_decimal(fill.get("avg_fill_price"), Decimal("0"))
            except Exception:
                # If fill not yet available, keep for next round
                continue

            if filled_qty <= 0 or exit_price <= 0:
                continue

            entry_side = "BUY" if direction.upper() == "LONG" else "SELL"

            try:
                record_exit_fifo(
                    bot_id=bot_id,
                    symbol=symbol,
                    entry_side=entry_side,
                    exit_qty=filled_qty,
                    exit_price=exit_price,
                    reason="exchange_stop",
                )
            except Exception as e:
                # Likely already recorded; still clear
                print("[EXSTOP] record_exit_fifo error:", e)

            try:
                clear_lock_level_pct(bot_id, symbol, direction)
            except Exception:
                pass

            try:
                clear_protective_orders(bot_id, symbol, direction)
            except Exception:
                pass

            # local cache
            try:
                key_local = (bot_id, symbol)
                if key_local in BOT_POSITIONS:
                    BOT_POSITIONS[key_local]["qty"] = Decimal("0")
                    BOT_POSITIONS[key_local]["entry_price"] = None
            except Exception:
                pass

            print(f"[EXSTOP] EXIT RECORDED bot={bot_id} {direction} {symbol} qty={filled_qty} @ {exit_price}")

        except Exception as e:
            print("[EXSTOP] reconcile error:", e)



def _risk_loop():
    print(f"[LADDER] risk loop started (interval={RISK_POLL_INTERVAL}s)")
    while True:
        try:
            # bots that have ladder enabled
            bots = sorted({*_parse_bot_list(','.join(LADDER_LONG_BOTS)), *_parse_bot_list(','.join(LADDER_SHORT_BOTS))})
        except Exception:
            bots = list(LADDER_LONG_BOTS | LADDER_SHORT_BOTS)

        for bot_id in bots:
            try:
                bot_id = _canon_bot_id(bot_id)
                opens = get_bot_open_positions(bot_id)
            except Exception:
                continue

            for (symbol, direction), v in list(opens.items()):
                try:
                    symbol = str(symbol).upper().strip()
                    direction = str(direction).upper().strip()
                    if not _bot_uses_ladder(bot_id, direction):
                        continue

                    qty = v.get("qty", Decimal("0"))
                    entry = v.get("weighted_entry")
                    if qty is None or entry is None:
                        continue
                    qty = Decimal(str(qty))
                    entry = Decimal(str(entry))
                    if qty <= 0 or entry <= 0:
                        continue

                    ref_price, ref_src, best_bid, best_ask = _get_l1_risk_price(symbol, direction)
                    if ref_price is None or ref_price <= 0:
                        continue

                    profit_pct = _compute_profit_pct(direction, entry, ref_price)
                    lock_pct = _maybe_raise_lock(bot_id, symbol, direction, profit_pct)
                    stop_price = _compute_stop_price(direction, entry, Decimal(str(lock_pct)))

                    if LADDER_DEBUG:
                        try:
                            key = f"{bot_id}:{direction}:{symbol}"
                            now = time.time()
                            last = _LADDER_STATUS_TS.get(key, 0.0)
                            if now - last >= LADDER_DEBUG_EVERY_SEC:
                                _LADDER_STATUS_TS[key] = now
                                print(
                                    f"[LADDER] STATUS bot={bot_id} {direction} {symbol} qty={qty} "
                                    f"entry={entry} ref={ref_price} src={ref_src} "
                                    f"bid={best_bid} ask={best_ask} profit%={profit_pct:.4f} lock%={lock_pct} stop={stop_price}"
                                )
                        except Exception:
                            pass

                    if ENABLE_EXCHANGE_STOP:
                        # Keep an exchange-native STOP_MARKET trigger resting on book.
                        # The trigger reference price is controlled by STOP_TRIGGER_PRICE_TYPE (default LAST).
                        _ensure_exchange_stop_order(bot_id, symbol, direction, qty, stop_price)
                    else:
                        # Bot-side emergency close (legacy)
                        if _ladder_should_stop(direction, ref_price, stop_price):
                            if not _exit_guard_allow(bot_id, symbol):
                                continue
                            print(
                                f"[LADDER] STOP bot={bot_id} {direction} {symbol} qty={qty} "
                                f"entry={entry} ref={ref_price} src={ref_src} "
                                f"bid={best_bid} ask={best_ask} profit%={profit_pct:.4f} lock%={lock_pct} stop={stop_price}"
                            )
                            _ladder_close_position(bot_id, symbol, direction, qty, reason="ladder_stop")

                except Exception as e:
                    print("[LADDER] loop error:", e)
        # Exchange stop reconciliation (fills/close detection)
        if ENABLE_EXCHANGE_STOP:
            try:
                _reconcile_exchange_stop_fills()
            except Exception as e:
                print('[STOP] reconcile error:', e)


        time.sleep(RISK_POLL_INTERVAL)


def _ensure_risk_thread():
    global _RISK_THREAD_STARTED
    with _RISK_LOCK:
        if _RISK_THREAD_STARTED:
            return
        t = threading.Thread(target=_risk_loop, daemon=True, name="apex-ladder-risk")
        t.start()
        _RISK_THREAD_STARTED = True
        print("[LADDER] risk thread created")

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

        # Ladder risk loop (bot-side trailing stop) is controlled independently of private WS.
        if ENABLE_RISK_LOOP:
            # Public WS for L1 bid/ask (orderBook25.H.*) used by ladder risk checks.
            # Safe to call multiple times.
            try:
                start_public_ws()
            except Exception as e:
                print("[SYSTEM] public WS failed to start:", e)
            _ensure_risk_thread()
            print("[SYSTEM] ladder risk enabled in this process (ENABLE_RISK_LOOP=1)")
        else:
            print("[SYSTEM] ladder risk disabled in this process (ENABLE_RISK_LOOP=0)")

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
                px = _get_mark_price(symbol)
            except Exception:
                continue
            if px is None:
                continue

            if direction.upper() == "LONG":
                unrealized += (px - wentry) * qty
            else:
                unrealized += (wentry - px) * qty

            open_rows.append({
                "symbol": str(symbol).upper(),
                "direction": direction.upper(),
                "qty": str(qty),
                "weighted_entry": str(wentry),
                "mark_price": str(px),
                "l1_bid": str(get_l1_bid_ask(symbol)[0]) if get_l1_bid_ask(symbol)[0] is not None else None,
                "l1_ask": str(get_l1_bid_ask(symbol)[1]) if get_l1_bid_ask(symbol)[1] is not None else None,
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
        <details>
          <summary>Quick links</summary>
          <div class="sub" style="margin-top:10px; line-height:1.6">
            API (all): <a href="/api/pnl" target="_blank">/api/pnl</a><br />
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
            return `<tr>
              <td>${p.symbol || ''}</td>
              <td>${dir}</td>
              <td>${p.qty || ''}</td>
              <td>${p.weighted_entry || ''}</td>
              <td>${p.mark_price || ''}</td>
            </tr>`;
          }).join('');
          tr2.innerHTML = `
            <td colspan="7">
              <details>
                <summary>Open positions for ${b.bot_id} (${opens.length})</summary>
                <div class="tableWrap" style="margin-top:10px;">
                  <table style="min-width:620px;">
                    <thead><tr><th>symbol</th><th>direction</th><th>qty</th><th>weighted_entry</th><th>mark_price</th></tr></thead>
                    <tbody>${rows}</tbody>
                  </table>
                </div>
              </details>
            </td>
          `;
          tbody.appendChild(tr2);
        }
      }
    }

    function setAutoRefresh() {
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

        # Optional leverage support (default 1x). If you later decide to use leverage,
        # treat `position_size_usdt` as *margin*, so effective notional = margin * leverage.
        try:
            leverage_raw = body.get("leverage", 1)
            leverage = int(float(str(leverage_raw)))
        except Exception:
            leverage = 1
        if leverage <= 0:
            leverage = 1

        effective_notional = budget * Decimal(str(leverage))

        try:
            snapped_qty = _compute_entry_qty(symbol, side_raw, effective_notional)
        except Exception as e:
            print("[ENTRY] qty compute error:", e)
            return "qty compute error", 500

        size_str = str(snapped_qty)
        print(f"[ENTRY] bot={bot_id} symbol={symbol} side={side_raw} margin={budget} lev={leverage} notional={effective_notional} -> qty={size_str} sig={sig_id}")

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

        # Ladder SL/lock state (bot-side only; no exchange protective orders)
        direction = "LONG" if side_raw == "BUY" else "SHORT"
        if _bot_uses_ladder(bot_id, direction) and entry_price_dec is not None and final_qty > 0:
            try:
                set_lock_level_pct(bot_id, symbol, direction, -LADDER_BASE_SL_PCT)
            except Exception as e:
                print("[LADDER] set base lock failed:", e)

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
            # Fail-closed: if fills are unavailable we do NOT write a fake price.
            return jsonify({"status": "fill_unavailable", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}), 200

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
