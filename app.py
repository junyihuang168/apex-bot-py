# app.py
import os
import time
import threading
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Dict, Tuple, Optional, Set, Any, List

from flask import Flask, request, jsonify, Response

from apex_client import (
    create_market_order,
    get_market_price,
    get_reference_price,
    get_l1_bid_ask,
    ensure_public_depth_subscription,
    start_public_ws,
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
    record_trade_event,
    list_trade_events,
    realized_pnl_by_window,

)

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "")

# ✅ 只让 worker 进程启用 WS/Fills（supervisord.conf 里给 worker 设置 ENABLE_WS="1"，web 设置 "0"）
ENABLE_WS = str(os.getenv("ENABLE_WS", "0")).strip() == "1"
ENABLE_RISK_LOOP = str(os.getenv("ENABLE_RISK_LOOP", "0")).strip() == "1"  # run ladder risk loop in this process
RISK_PRICE_SOURCE = str(os.getenv("RISK_PRICE_SOURCE", "MARK")).upper().strip()  # MARK | LAST | INDEX | L1

# 退出互斥窗口（秒）：防止重复平仓
EXIT_COOLDOWN_SEC = float(os.getenv("EXIT_COOLDOWN_SEC", "2.0"))

# Entry-after-exit guard:
# - When TradingView emits CLOSE then OPEN almost at the same time (same bar / same timestamp),
#   we can block the OPEN to prevent immediate re-entry / flip.
# - ENTRY_COOLDOWN_AFTER_EXIT_SEC: time-based cooldown (per symbol)
# - ENTRY_BLOCK_SAME_TV_CLIENT_ID: block ENTRY if its TV client_id equals the most recent EXIT client_id (per symbol)
ENTRY_COOLDOWN_AFTER_EXIT_SEC = float(os.getenv("ENTRY_COOLDOWN_AFTER_EXIT_SEC", "4.0"))
ENTRY_BLOCK_SAME_TV_CLIENT_ID = str(os.getenv("ENTRY_BLOCK_SAME_TV_CLIENT_ID", "1")).strip() == "1"


# 远程兜底白名单：只有本地无 lots 且 symbol 在白名单，才允许 remote fallback
REMOTE_FALLBACK_SYMBOLS = {
    s.strip().upper() for s in os.getenv("REMOTE_FALLBACK_SYMBOLS", "").split(",") if s.strip()
}


# ----------------------------
# ✅ Mirror bots (SMC long/short pairs) — ensure EXIT happens before ENTRY when flipping
# Default pairs: BOT_1<->BOT_11 ... BOT_5<->BOT_15 (override via MIRROR_BOT_PAIRS)
# Example: MIRROR_BOT_PAIRS="BOT_1:BOT_11,BOT_2:BOT_12,BOT_3:BOT_13"
# ----------------------------
MIRROR_BOT_PAIRS_RAW = str(os.getenv("MIRROR_BOT_PAIRS", "")).strip()
MIRROR_WAIT_SEC = float(os.getenv("MIRROR_WAIT_SEC", "4.0"))
MIRROR_FORCE_EXIT = str(os.getenv("MIRROR_FORCE_EXIT", "1")).strip() == "1"

# ✅ Global single-position per symbol (across ALL bots):
# If ANY bot currently holds a position on a symbol, and ANOTHER bot sends an ENTRY for the same symbol,
# we will force-close the existing holder(s) first, then allow the new ENTRY.
GLOBAL_SYMBOL_SINGLE_POSITION = str(os.getenv("GLOBAL_SYMBOL_SINGLE_POSITION", "1")).strip() == "1"
GLOBAL_FLIP_WAIT_SEC = float(os.getenv("GLOBAL_FLIP_WAIT_SEC", "6.0"))

_MIRROR_LOCKS: dict = {}  # symbol -> threading.Lock
_MIRROR_LOCKS_GUARD = threading.Lock()

# ✅ 说明：本版本不在交易所挂真实 TP/SL 单；仅使用真实 fills 进行记账，并由机器人在后台按规则触发平仓。

# 本地 cache（仅辅助）
BOT_POSITIONS: Dict[Tuple[str, str], dict] = {}

_MONITOR_THREAD_STARTED = False
_MONITOR_LOCK = threading.Lock()

# ✅ 退出互斥：bot+symbol 粒度 cooldown
_EXIT_LOCK = threading.Lock()
_LAST_EXIT_TS: Dict[Tuple[str, str], float] = {}

# ✅ Entry guard: symbol-level (prevents CLOSE->OPEN immediate re-entry)
_ENTRY_GUARD_LOCK = threading.Lock()
_LAST_EXIT_SYMBOL_TS: Dict[str, float] = {}         # symbol -> last exit ts
_LAST_EXIT_TV_CLIENT_ID: Dict[str, str] = {}        # symbol -> last exit tv client_id (raw string)



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


########################################################################
# BOT GROUPS
#
# ✅ 移动止损（Ladder Stop）适用范围（按你最新需求）
# - BOT_1–10：做多（LONG）移动止损
# - BOT_11–20：做空（SHORT）移动止损
#
# 说明：
# - 本版本的“移动止损”是机器人侧（bot-side）reduceOnly 市价平仓，不在交易所挂保护单。
# - 具体参数见：LADDER_CONFIGS（按 bot 分段配置 Base SL + Ladder Levels）
########################################################################

# ----------------------------
# ✅ BOT 分组（按你最新需求）
#
# LONG bots:
# - BOT_1~BOT_10
# - BOT_21~BOT_25
# SHORT bots:
# - BOT_11~BOT_20
# - BOT_31~BOT_35
# ----------------------------

_ALLOWED_LONG_TPSL = {f"BOT_{i}" for i in range(1, 11)} | {f"BOT_{i}" for i in range(21, 26)}
_ALLOWED_SHORT_TPSL = {f"BOT_{i}" for i in range(11, 21)} | {f"BOT_{i}" for i in range(31, 36)}

# “TPSL bots” here means: bots that are allowed to have bot-side Ladder Stop enabled.
# You can override via env LONG_TPSL_BOTS / SHORT_TPSL_BOTS, but we always intersect with the allowed sets.
LONG_TPSL_BOTS = _parse_bot_list(os.getenv("LONG_TPSL_BOTS", ",".join(sorted(_ALLOWED_LONG_TPSL)))) & _ALLOWED_LONG_TPSL
SHORT_TPSL_BOTS = _parse_bot_list(os.getenv("SHORT_TPSL_BOTS", ",".join(sorted(_ALLOWED_SHORT_TPSL)))) & _ALLOWED_SHORT_TPSL

# “PNL only” means: no bot-side Ladder Stop; only record real fills and follow strategy exit signals.
# (Keep future expansion ranges here.)
LONG_PNL_ONLY_BOTS = _parse_bot_list(
    os.getenv(
        "LONG_PNL_ONLY_BOTS",
        ",".join([*(f"BOT_{i}" for i in range(21, 31))]),
    )
) - _ALLOWED_LONG_TPSL

SHORT_PNL_ONLY_BOTS = _parse_bot_list(
    os.getenv(
        "SHORT_PNL_ONLY_BOTS",
        ",".join([*(f"BOT_{i}" for i in range(31, 41))]),
    )
) - _ALLOWED_SHORT_TPSL


# ----------------------------
# ✅ Ladder Stop (bot-side only; no exchange protective orders)
#
# 说明：
# - 本版本的止损/移动止损都是机器人侧（bot-side）reduceOnly 市价平仓，不在交易所挂保护单。
# - “初始止损”通过 lock% 初始化为 -base_sl_pct 实现。
# - “无限延伸”通过最后一档 gap = (last_profit - last_lock) 继续跟随：lock = profit - gap。
#
# 当前已启用的方案（按 bot 段部署）：
#
# 方案 A（阶梯 + 无限）：Initial SL -1.0%
#   1.0→0.0, 2.0→1.0, 3.0→2.0, 5.0→3.5
#   - LONG:  BOT_1~BOT_5
#   - SHORT: BOT_11~BOT_15
#
# 方案 B（已删除阶梯；仅保留硬止损）：Fixed SL -1.0%（无阶梯/无锁盈）
#   - LONG:  BOT_6~BOT_10
#   - SHORT: BOT_16~BOT_20
#
# 方案 C（cycle23 无限循环）：Initial SL -0.8%
#   Stage1: profit>=0.8% -> lock=0.0%
#   Then repeat forever (k=0,1,2...):
#     Stage2: profit>=(2.0+4.0k)% -> lock=0.60*(2.0+4.0k)%
#     Stage3: profit>=(4.0+4.0k)% -> lock=(4.0+4.0k)-0.5
#   - LONG:  BOT_21~BOT_25
#   - SHORT: BOT_31~BOT_35
#
# 方案 D（阶梯 + 无限）：Initial SL -0.85%
#   2.0→0.4, 2.4→0.8，然后按最后 gap=1.6 无限上调（lock=profit-1.6）
#   - LONG:  BOT_26~BOT_30
#   - SHORT: BOT_36~BOT_40
# ----------------------------


def _ladder_levels(*pairs: Tuple[str, str]) -> List[Tuple[Decimal, Decimal]]:
    out: List[Tuple[Decimal, Decimal]] = []
    for a, b in pairs:
        out.append((Decimal(str(a)), Decimal(str(b))))
    out.sort(key=lambda x: x[0])
    return out


_LADDER_CFG_A = {
    "name": "A",
    # ✅ BOT_1-5 (LONG) & BOT_11-15 (SHORT): Initial SL = -1.0% (bot-side)
    "base_sl_pct": Decimal("1.0"),
    "levels": _ladder_levels(
        ("1.0", "0.0"),
        ("2.0", "1.0"),
        ("3.0", "2.0"),
        ("5.0", "3.5"),
    ),
    # Infinite tail: gap = 5.0 - 3.5 = 1.5  => lock = profit - 1.5 (after profit >= 5.0)
    "long_bots": {f"BOT_{i}" for i in range(1, 6)},
    "short_bots": {f"BOT_{i}" for i in range(11, 16)},
}


_LADDER_CFG_B_FIXED = {
    "name": "B_FIXED_1PCT",  # ✅ Scheme B removed: keep ONLY a fixed -1.0% stop (no ladder)
    # BOT_6-10 (LONG) & BOT_16-20 (SHORT): Fixed SL = -1.0% (bot-side)
    "base_sl_pct": Decimal("1.0"),
    # No ladder levels: lock% will stay at -1.0% forever (pure hard stop).
    "levels": _ladder_levels(),
    "long_bots": {f"BOT_{i}" for i in range(6, 11)},
    "short_bots": {f"BOT_{i}" for i in range(16, 21)},
}

_LADDER_CFG_C = {
    "name": "C",
    "mode": "cycle23",  # stage1 once, then stage2+stage3 repeat to infinity (monotonic SL only)
    # ✅ BOT_21-25 (LONG) & BOT_31-35 (SHORT): Initial SL = -0.8%
    "base_sl_pct": Decimal("0.8"),
    # Stage 1 (one-time): Profit >= 0.8% -> lock to 0.0% (breakeven)
    "stage1_profit": Decimal("0.8"),
    "stage1_lock": Decimal("0.0"),
    # Cycle step: every 4.0% profit we repeat stage2 then stage3
    # Stage 2 trigger: Profit >= (2.0% + 4.0% * k) -> lock = 60% * threshold
    "cycle_stage2_start": Decimal("2.0"),
    "cycle_stage3_start": Decimal("4.0"),
    "cycle_step": Decimal("4.0"),
    "stage2_lock_ratio": Decimal("0.60"),
    # Stage 3 trigger: Profit >= (4.0% + 4.0% * k) -> lock = threshold - 0.5%
    "stage3_trail_gap": Decimal("0.5"),
    # Dummy levels retained for compatibility (not used in cycle23 mode)
    "levels": _ladder_levels(
        ("0.8", "0.0"),
        ("2.0", "1.2"),
        ("4.0", "3.5"),
    ),
    "long_bots": {f"BOT_{i}" for i in range(21, 26)},
    "short_bots": {f"BOT_{i}" for i in range(31, 36)},
}


_LADDER_CFG_D = {
    "name": "D",
    # ✅ BOT_26-30 (LONG) & BOT_36-40 (SHORT): Initial SL = -0.85%
    "base_sl_pct": Decimal("0.85"),
    # Ladder:
    #   Profit >= 2.0% -> lock 0.4%
    #   Profit >= 2.4% -> lock 0.8%
    # Infinite tail uses last gap: 2.4 - 0.8 = 1.6  => lock = profit - 1.6 (for profit >= 2.4)
    "levels": _ladder_levels(
        ("2.0", "0.4"),
        ("2.4", "0.8"),
    ),
    "long_bots": {f"BOT_{i}" for i in range(26, 31)},
    "short_bots": {f"BOT_{i}" for i in range(36, 41)},
}


LADDER_CONFIGS = [_LADDER_CFG_A, _LADDER_CFG_B_FIXED, _LADDER_CFG_C, _LADDER_CFG_D]


def _ladder_trailing_gap_pct(levels: List[Tuple[Decimal, Decimal]]) -> Optional[Decimal]:
    """After the last level, we keep trailing by the last gap: gap = last_profit - last_lock."""
    if not levels:
        return None
    last_profit, last_lock = levels[-1]
    try:
        gap = last_profit - last_lock
    except Exception:
        return None
    if gap <= 0:
        return None
    return gap


def _get_ladder_cfg(bot_id: str, direction: str) -> Optional[dict]:
    b = _canon_bot_id(bot_id)
    d = str(direction or "").upper().strip()
    for cfg in LADDER_CONFIGS:
        if d == "LONG" and b in cfg["long_bots"]:
            return cfg
        if d == "SHORT" and b in cfg["short_bots"]:
            return cfg
    return None


def _serialize_ladder_cfg(cfg: Optional[dict]) -> Optional[dict]:
    if not cfg:
        return None
    out = {
        "name": cfg.get("name"),
        "mode": str(cfg.get("mode", "ladder")),
        "base_sl_pct": str(cfg.get("base_sl_pct")) if cfg.get("base_sl_pct") is not None else None,
    }
    levels = cfg.get("levels") or []
    out["levels"] = [[str(p), str(l)] for (p, l) in levels]
    # cycle23 extras (optional)
    for k in (
        "stage1_profit", "stage1_lock", "cycle_stage2_start", "cycle_stage3_start", "cycle_step",
        "stage2_lock_ratio", "stage3_trail_gap",
    ):
        if k in cfg:
            try:
                out[k] = str(cfg.get(k))
            except Exception:
                out[k] = cfg.get(k)
    return out


def _all_ladder_bots() -> Set[str]:
    s: Set[str] = set()
    for cfg in LADDER_CONFIGS:
        s |= set(cfg.get("long_bots", set()))
        s |= set(cfg.get("short_bots", set()))
    return s

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
def _bot_uses_ladder(bot_id: str, direction: str) -> bool:
    return _get_ladder_cfg(bot_id, direction) is not None


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




def _get_risk_price(symbol: str, direction: str):
    """Return (price, source, bid, ask). Source is one of MARK/LAST/INDEX/L1/REF."""
    src = (RISK_PRICE_SOURCE or 'MARK').upper()

    # Prefer exchange-native reference prices when available.
    if src == 'MARK':
        mp = _get_mark_price(symbol)
        if mp:
            return mp, 'MARK', None, None
        rp = get_reference_price(symbol)
        return rp, 'REF', None, None

    if src == 'LAST':
        rp = get_reference_price(symbol)
        return rp, 'LAST', None, None

    if src == 'INDEX':
        # If Omni SDK does not expose a dedicated index endpoint here, fall back.
        rp = get_reference_price(symbol)
        return rp, 'INDEX', None, None

    # L1 / bid-ask path
    return _get_l1_risk_price(symbol, direction)
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


def _mark_symbol_exit(symbol: str, tv_client_id: str = "") -> None:
    """Mark that this symbol has just received an EXIT intent.

    Used to block immediate CLOSE->OPEN (same bar / same timestamp) scenarios.
    This marker is intentionally written on EXIT webhook receipt (not on fill confirmation),
    because the user requirement is to block OPEN when CLOSE arrives together.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return
    now = time.time()
    with _ENTRY_GUARD_LOCK:
        _LAST_EXIT_SYMBOL_TS[sym] = now
        if tv_client_id:
            _LAST_EXIT_TV_CLIENT_ID[sym] = str(tv_client_id)


def _entry_guard_reject(symbol: str, tv_client_id: str = "") -> Optional[dict]:
    """Return a rejection payload if ENTRY is not allowed right after EXIT for this symbol."""
    sym = str(symbol or "").upper().strip()
    now = time.time()

    with _ENTRY_GUARD_LOCK:
        last_ts = _LAST_EXIT_SYMBOL_TS.get(sym, 0.0)
        last_tv = _LAST_EXIT_TV_CLIENT_ID.get(sym, "")

    # 1) Same TV client_id (usually includes bar timestamp) => block
    if ENTRY_BLOCK_SAME_TV_CLIENT_ID and tv_client_id and last_tv and str(tv_client_id) == str(last_tv):
        print(f"[ENTRY_GUARD] reject_entry_same_bar_as_exit symbol={sym} tv_client_id={tv_client_id} last_exit_tv={last_tv}")
        return {
            "status": "reject_entry_same_bar_as_exit",
            "symbol": sym,
            "tv_client_id": str(tv_client_id),
            "last_exit_tv_client_id": str(last_tv),
        }

    # 2) Time-based cooldown per symbol
    if ENTRY_COOLDOWN_AFTER_EXIT_SEC and ENTRY_COOLDOWN_AFTER_EXIT_SEC > 0:
        # If we've never seen an EXIT marker for this symbol, allow ENTRY.
        if last_ts and float(last_ts) > 0:
            dt = now - float(last_ts)
            if dt < ENTRY_COOLDOWN_AFTER_EXIT_SEC:
                print(f"[ENTRY_GUARD] reject_entry_after_exit_cooldown symbol={sym} dt={dt:.3f}s cooldown={ENTRY_COOLDOWN_AFTER_EXIT_SEC}s")
                return {
                    "status": "reject_entry_after_exit_cooldown",
                    "symbol": sym,
                    "cooldown_sec": ENTRY_COOLDOWN_AFTER_EXIT_SEC,
                    "since_exit_sec": round(dt, 3),
                }

    return None


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

    Why this exists:
      - ApeX places orders by *quantity*. Quantity must satisfy stepSize/minQty.
      - For very low-price symbols, minQty can be large. If the requested notional
        is too small, we can optionally upsize to the minimum tradable quantity.

    Robust reference price selection (fixes 'reference price = 0.00' after WS staleness):
      1) get_reference_price()  -> REST ticker (most stable)
      2) get_market_price()     -> L1 bid/ask when available, but can transiently be 0 if public WS is stale
      3) _get_mark_price()      -> best-effort mark/index/oracle/last fallback
    """
    sym = str(symbol).upper().strip()
    side_u = str(side).upper().strip()

    rules = _get_symbol_rules(sym)
    step = Decimal(str(rules.get("step_size") or "1"))
    min_qty = Decimal(str(rules.get("min_qty") or "0"))

    budget_usdt = _to_decimal(size_usdt, default=Decimal("0"))
    if budget_usdt <= 0:
        raise ValueError(f"invalid size_usdt: {size_usdt}")

    def _pick_ref_price() -> Tuple[Decimal, str]:
        # 1) REST ticker/reference (most stable; does not depend on public WS health)
        try:
            rp = get_reference_price(sym)
            rp_dec = _to_decimal(rp, default=Decimal("0"))
            if rp_dec > 0:
                return rp_dec, "REF_REST"
        except Exception:
            pass

        # 2) Market price helper (prefers L1 when available; can become 0 when WS is stale)
        for q in (str(min_qty), "1"):
            try:
                mp = get_market_price(sym, side_u, q)
                mp_dec = _to_decimal(mp, default=Decimal("0"))
                if mp_dec > 0:
                    return mp_dec, f"MARKET_PRICE(q={q})"
            except Exception:
                continue

        # 3) Mark/index/oracle fallback (best effort)
        try:
            mk = _get_mark_price(sym)
            mk_dec = _to_decimal(mk, default=Decimal("0"))
            if mk_dec > 0:
                return mk_dec, "MARK_FALLBACK"
        except Exception:
            pass

        return Decimal("0"), "NO_PRICE"

    ref_price_dec, ref_src = _pick_ref_price()
    if ref_price_dec <= 0:
        raise RuntimeError(f"invalid reference price for qty compute: {ref_price_dec} src={ref_src}")

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
    min_needed = (est_notional * (Decimal("1") + ENTRY_MIN_NOTIONAL_MARGIN_PCT)).quantize(
        Decimal("0.00000001"), rounding=ROUND_DOWN
    )

    if not ENTRY_AUTO_UPSIZE_TO_MIN_QTY:
        raise ValueError(
            f"budget too small for {sym}: budget={budget_usdt} < min_needed~{min_needed} "
            f"(minQty={min_qty}, step={step}, ref={ref_price_dec}, src={ref_src})"
        )

    if ENTRY_MAX_NOTIONAL_USDT > 0 and est_notional > ENTRY_MAX_NOTIONAL_USDT:
        raise ValueError(
            f"budget too small for {sym}: min_notional~{min_needed} exceeds cap "
            f"ENTRY_MAX_NOTIONAL_USDT={ENTRY_MAX_NOTIONAL_USDT}"
        )

    print(
        f"[ENTRY] upsize_to_minQty symbol={sym} side={side_u} "
        f"budget={budget_usdt} -> qty={qty_min} est_notional={est_notional} "
        f"(minQty={min_qty} step={step} ref={ref_price_dec} src={ref_src})"
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
# ✅ Mirror helpers
# ----------------------------

def _parse_mirror_pairs(raw: str) -> dict:
    """Return mapping bot->mirror_bot.

    Format: "BOT_1:BOT_11,BOT_2:BOT_12" (case/spacing tolerant).
    If empty, defaults to BOT_1<->BOT_11 ... BOT_5<->BOT_15.
    """
    mapping = {}
    raw = (raw or '').strip()

    def _add(a: str, b: str):
        a = _canon_bot_id(a)
        b = _canon_bot_id(b)
        if not a or not b or a == b:
            return
        mapping[a] = b
        mapping[b] = a

    if raw:
        for part in raw.split(','):
            part = part.strip()
            if not part or ':' not in part:
                continue
            a, b = part.split(':', 1)
            _add(a.strip(), b.strip())
        return mapping

    # default: 1-5 paired with 11-15
    for i in range(1, 6):
        _add(f"BOT_{i}", f"BOT_{i+10}")
    return mapping


_MIRROR_PAIRS = _parse_mirror_pairs(MIRROR_BOT_PAIRS_RAW)


def _mirror_partner(bot_id: str) -> str:
    return _MIRROR_PAIRS.get(_canon_bot_id(bot_id), "")


def _is_mirror_bot(bot_id: str) -> bool:
    return bool(_mirror_partner(bot_id))


def _get_mirror_lock(symbol: str) -> threading.Lock:
    sym = str(symbol or '').upper().strip()
    with _MIRROR_LOCKS_GUARD:
        lk = _MIRROR_LOCKS.get(sym)
        if lk is None:
            lk = threading.Lock()
            _MIRROR_LOCKS[sym] = lk
        return lk


def _find_symbol_holders(symbol: str) -> List[Dict[str, Any]]:
    """Return a list of bots currently holding an open position for this symbol.

    Output items: {"bot_id": "BOT_X", "direction": "LONG|SHORT", "qty": Decimal}

    Source of truth is the local PnL DB (lots with remaining_qty).
    This is used to enforce **global single-position per symbol**.
    """
    sym = str(symbol or "").upper().strip()
    if not sym:
        return []

    holders: List[Dict[str, Any]] = []
    bots = list_bots_with_activity()
    for b in bots:
        b2 = _canon_bot_id(b)
        try:
            opens = get_bot_open_positions(b2)
        except Exception:
            continue
        for d in ("LONG", "SHORT"):
            try:
                q = opens.get((sym, d), {}).get("qty", Decimal("0"))
            except Exception:
                q = Decimal("0")
            if q and q > 0:
                holders.append({"bot_id": b2, "direction": d, "qty": q})
    return holders


def _execute_exit_order(
    bot_id: str,
    symbol: str,
    *,
    force_direction: str = "",
    ignore_cooldown: bool = False,
    reason: str = "strategy_exit",
    sig_id: str = "",
) -> dict:
    """Execute a reduce-only market close for bot+symbol and record real fills.

    - force_direction: "LONG" or "SHORT" (optional)
    - ignore_cooldown: bypass EXIT_COOLDOWN_SEC guard (used for mirror flips)

    Returns a payload dict suitable for jsonify().
    """
    bot_id = _canon_bot_id(bot_id)
    symbol = str(symbol or "").upper().strip()

    if (not ignore_cooldown) and (not _exit_guard_allow(bot_id, symbol)):
        print(f"[EXIT] cooldown_skip bot={bot_id} symbol={symbol} cooldown={EXIT_COOLDOWN_SEC}s")
        return {"status": "cooldown_skip", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}

    opens = get_bot_open_positions(bot_id)
    long_key = (symbol, "LONG")
    short_key = (symbol, "SHORT")

    long_qty = opens.get(long_key, {}).get("qty", Decimal("0"))
    short_qty = opens.get(short_key, {}).get("qty", Decimal("0"))

    # Prefer local cache direction unless forced
    key_local = (bot_id, symbol)
    local = BOT_POSITIONS.get(key_local)
    preferred = "LONG"
    if local and str(local.get("side", "")).upper() == "SELL":
        preferred = "SHORT"

    direction_to_close = None
    qty_to_close = Decimal("0")

    fd = str(force_direction or "").upper().strip()
    if fd in ("LONG", "SHORT"):
        if fd == "LONG" and long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif fd == "SHORT" and short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty

    if (not direction_to_close) or qty_to_close <= 0:
        if preferred == "LONG" and long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif preferred == "SHORT" and short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty
        elif long_qty > 0:
            direction_to_close, qty_to_close = "LONG", long_qty
        elif short_qty > 0:
            direction_to_close, qty_to_close = "SHORT", short_qty

    if (not direction_to_close) or qty_to_close <= 0:
        if symbol in REMOTE_FALLBACK_SYMBOLS:
            remote = get_open_position_for_symbol(symbol)
            if remote and remote["size"] > 0:
                direction_to_close = remote["side"]
                qty_to_close = remote["size"]
            else:
                print(f"[EXIT] no_position bot={bot_id} symbol={symbol}")
                return {"status": "no_position", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}
        else:
            print(f"[EXIT] no_position bot={bot_id} symbol={symbol}")
            return {"status": "no_position", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}

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
        return {"status": "order_error", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id, "error": str(e)}

    status, cancel_reason = _order_status_and_reason(order)
    print(f"[EXIT] order status={status} cancelReason={cancel_reason!r} bot={bot_id} symbol={symbol} reason={reason}")

    if status in ("CANCELED", "REJECTED"):
        return {
            "status": "exit_rejected",
            "mode": "exit",
            "bot_id": bot_id,
            "symbol": symbol,
            "exit_side": exit_side,
            "requested_qty": str(qty_to_close),
            "order_status": status,
            "cancel_reason": cancel_reason,
            "signal_id": sig_id,
        }

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
        return {"status": "fill_unavailable", "mode": "exit", "bot_id": bot_id, "symbol": symbol, "signal_id": sig_id}

    try:
        out = record_exit_fifo(
            bot_id=bot_id,
            symbol=symbol,
            entry_side=entry_side,
            exit_qty=filled_qty,
            exit_price=exit_price,
            reason=reason,
        )
        try:
            direction = "LONG" if str(entry_side).upper() == "BUY" else "SHORT"
            realized_sum = Decimal(str((out or {}).get("realized_sum", "0")))
            record_trade_event(
                bot_id=bot_id,
                symbol=symbol,
                direction=direction,
                event_type="EXIT",
                qty=filled_qty,
                exit_price=exit_price,
                realized_pnl=realized_sum,
                reason=reason,
            )
        except Exception as _e:
            print("[DASH] record_trade_event EXIT error:", _e)
    except Exception as e:
        print("[PNL] record_exit_fifo error:", e)

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

    return {
        "status": "ok",
        "mode": "exit",
        "bot_id": bot_id,
        "symbol": symbol,
        "exit_side": exit_side,
        "closed_qty": str(qty_to_close),
        "order_status": status,
        "cancel_reason": cancel_reason,
        "signal_id": sig_id,
    }


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


def _ladder_desired_lock_pct(levels: List[Tuple[Decimal, Decimal]], profit_pct: Decimal) -> Optional[Decimal]:
    """Return the desired lock pct given current profit pct and discrete ladder levels."""
    desired: Optional[Decimal] = None
    for p_th, lock in (levels or []):
        if profit_pct >= p_th:
            desired = lock
        else:
            break
    return desired


def _cycle23_desired_lock_pct(cfg: dict, profit_pct: Decimal) -> Optional[Decimal]:
    """Desired lock% for the CYCLE_23 scheme (stage2/stage3 loop), independent of current lock.

    Returns the maximum candidate lock% implied by the rules, or None if no rule is active yet.
    Caller must enforce monotonicity: new_lock = max(old_lock, desired).
    """
    cycle = cfg.get("cycle") or {}
    try:
        stage1_profit: Decimal = Decimal(str(cycle.get("stage1_profit", "0.8")))
        stage1_lock: Decimal = Decimal(str(cycle.get("stage1_lock", "0.0")))
        cycle_step: Decimal = Decimal(str(cycle.get("cycle_step", "4.0")))
        stage2_start: Decimal = Decimal(str(cycle.get("stage2_start", "2.0")))
        stage2_lock_ratio: Decimal = Decimal(str(cycle.get("stage2_lock_ratio", "0.60")))
        stage3_start: Decimal = Decimal(str(cycle.get("stage3_start", "4.0")))
        stage3_lock_offset: Decimal = Decimal(str(cycle.get("stage3_lock_offset", "0.5")))
    except Exception:
        return None

    desired: Optional[Decimal] = None

    # Stage 1 (one-time floor)
    if profit_pct >= stage1_profit:
        desired = stage1_lock

    # Stage 2/3 loop (stepwise triggers)
    if cycle_step > 0:
        # Stage 2 triggers at: stage2_start + k*cycle_step
        if profit_pct >= stage2_start:
            k2_max = int(((profit_pct - stage2_start) / cycle_step).to_integral_value(rounding=ROUND_DOWN))
            for k in range(0, k2_max + 1):
                trig = stage2_start + (cycle_step * Decimal(k))
                cand = (stage2_lock_ratio * trig)
                desired = cand if desired is None or cand > desired else desired

        # Stage 3 triggers at: stage3_start + k*cycle_step
        if profit_pct >= stage3_start:
            k3_max = int(((profit_pct - stage3_start) / cycle_step).to_integral_value(rounding=ROUND_DOWN))
            for k in range(0, k3_max + 1):
                trig = stage3_start + (cycle_step * Decimal(k))
                cand = trig - stage3_lock_offset
                desired = cand if desired is None or cand > desired else desired

    return desired


def _maybe_raise_lock(bot_id: str, symbol: str, direction: str, profit_pct: Decimal):
    """Update and return current lock% for (bot,symbol,direction).

    Two modes:
    1) Default ladder mode:
       - Initialize lock% to -base_sl_pct.
       - Raise lock% as profit reaches ladder thresholds.
       - After the last level, keep trailing "infinitely" by the last gap (last_profit - last_lock).

    2) cycle23 mode (BOT_21-25 / BOT_31-35):
       - Initial SL = -0.8%
       - Stage 1 (one-time): profit>=0.8% -> lock=0.0%
       - Then repeat forever (k=0,1,2...):
           Stage 2: profit>= (2.0 + 4.0*k)% -> lock = 0.60 * (2.0 + 4.0*k)%
           Stage 3: profit>= (4.0 + 4.0*k)% -> lock = (4.0 + 4.0*k) - 0.5
       - Hard constraint: lock must be monotonic (never lower than previous lock).
    """
    cfg = _get_ladder_cfg(bot_id, direction)
    if not cfg:
        return None

    mode = str(cfg.get("mode", "ladder")).lower().strip()

    # current lock
    try:
        cur = get_lock_level_pct(bot_id, symbol, direction)
    except Exception:
        cur = None

    base_sl_pct: Decimal = Decimal(str(cfg.get("base_sl_pct", "0")))
    if cur is None:
        cur = -base_sl_pct
        try:
            set_lock_level_pct(bot_id, symbol, direction, cur)
        except Exception:
            return cur

    desired: Optional[Decimal] = None

    if mode == "cycle23":
        # Stage 1 (one-time)
        st1_p: Decimal = cfg.get("stage1_profit", Decimal("0"))
        st1_lock: Decimal = cfg.get("stage1_lock", Decimal("0"))
        if profit_pct >= st1_p:
            desired = st1_lock

        # Cycle computations (pick the best candidate <= current profit)
        step: Decimal = cfg.get("cycle_step", Decimal("4.0"))
        s2_start: Decimal = cfg.get("cycle_stage2_start", Decimal("2.0"))
        s3_start: Decimal = cfg.get("cycle_stage3_start", Decimal("4.0"))
        ratio: Decimal = cfg.get("stage2_lock_ratio", Decimal("0.60"))
        gap: Decimal = cfg.get("stage3_trail_gap", Decimal("0.5"))

        # Stage 2 best candidate
        if profit_pct >= s2_start and step > 0:
            k2 = int(((profit_pct - s2_start) // step))
            if k2 < 0:
                k2 = 0
            p2 = s2_start + (step * Decimal(k2))
            cand2 = (p2 * ratio)
            if desired is None or cand2 > desired:
                desired = cand2

        # Stage 3 best candidate
        if profit_pct >= s3_start and step > 0:
            k3 = int(((profit_pct - s3_start) // step))
            if k3 < 0:
                k3 = 0
            p3 = s3_start + (step * Decimal(k3))
            cand3 = p3 - gap
            if desired is None or cand3 > desired:
                desired = cand3

    else:
        # Default ladder mode
        levels: List[Tuple[Decimal, Decimal]] = cfg.get("levels") or []
        tail_gap = _ladder_trailing_gap_pct(levels)
        last_profit = levels[-1][0] if levels else None

        desired = _ladder_desired_lock_pct(levels, profit_pct)

        # Infinite tail: once profit is beyond the last ladder threshold, keep trailing by the last gap.
        if tail_gap is not None and last_profit is not None and profit_pct >= last_profit:
            tail_lock = profit_pct - tail_gap
            if desired is None or tail_lock > desired:
                desired = tail_lock

    # Hard monotonic rule: lock can only move up (never down)
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

    desired: Optional[Decimal] = None

    if mode == "LADDER":
        desired = _ladder_desired_lock_pct(levels, profit_pct)

        # Infinite tail: once profit is beyond the last ladder threshold, keep trailing by the last gap.
        tail_gap = _ladder_trailing_gap_pct(levels)
        last_profit = levels[-1][0] if levels else None
        if tail_gap is not None and last_profit is not None and profit_pct >= last_profit:
            tail_lock = profit_pct - tail_gap
            if desired is None or tail_lock > desired:
                desired = tail_lock

    elif mode == "CYCLE_23":
        desired = _cycle23_desired_lock_pct(cfg, profit_pct)

    else:
        # Unknown mode: behave like LADDER but without tail.
        desired = _ladder_desired_lock_pct(levels, profit_pct)

    # ✅ Hard constraint: stop-lock only moves upward
    if desired is not None and desired > cur:
        try:
            set_lock_level_pct(bot_id, symbol, direction, desired)
            print(
                f"[LADDER] RAISE_LOCK bot={bot_id} {direction} {symbol} profit%={profit_pct:.4f} lock% {cur} -> {desired} mode={mode}"
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
        out = record_exit_fifo(
            bot_id=bot_id,
            symbol=symbol,
            entry_side=entry_side,
            exit_qty=filled_qty,
            exit_price=exit_price,
            reason=reason,
        )
        try:
            realized_sum = Decimal(str((out or {}).get("realized_sum", "0")))
            record_trade_event(
                bot_id=bot_id,
                symbol=symbol,
                direction=direction,
                event_type="EXIT",
                qty=filled_qty,
                exit_price=exit_price,
                realized_pnl=realized_sum,
                reason=reason,
            )
        except Exception as _e:
            print("[DASH] record_trade_event LADDER EXIT error:", _e)
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


def _risk_loop():
    print(f"[LADDER] risk loop started (interval={RISK_POLL_INTERVAL}s)")
    while True:
        # bots that have ladder enabled (across all ladder configs)
        bots = sorted(_all_ladder_bots())

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

                    ref_price, ref_src, best_bid, best_ask = _get_risk_price(symbol, direction)
                    if ref_price is None or ref_price <= 0:
                        continue

                    profit_pct = _compute_profit_pct(direction, entry, ref_price)
                    old_lock = None
                    try:
                        old_lock = get_lock_level_pct(bot_id, symbol, direction)
                    except Exception:
                        old_lock = None

                    lock_pct = _maybe_raise_lock(bot_id, symbol, direction, profit_pct)
                    if lock_pct is None:
                        continue
                    stop_price = _compute_stop_price(direction, entry, Decimal(str(lock_pct)))

                    # Dashboard event on lock change
                    try:
                        new_lock = Decimal(str(lock_pct))
                        old_lock_dec = Decimal(str(old_lock)) if old_lock is not None else None
                        if old_lock_dec is None or new_lock != old_lock_dec:
                            record_trade_event(
                                bot_id=bot_id,
                                symbol=symbol,
                                direction=direction,
                                event_type="STOP_UPDATE",
                                qty=qty,
                                entry_price=entry,
                                stop_price=stop_price,
                                lock_level_pct=new_lock,
                                reason="lock_update",
                            )
                    except Exception as _e:
                        if LADDER_DEBUG:
                            print("[DASH] record_trade_event STOP_UPDATE error:", _e)


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
            # In MARK/LAST/INDEX modes, we do NOT need public orderBook ws.
            # Only start public ws when explicitly using L1 bid/ask as the risk price source.
            if RISK_PRICE_SOURCE in ("L1", "BIDASK"):
                try:
                    start_public_ws()
                except Exception as e:
                    print("[SYSTEM] public WS failed to start:", e)
            else:
                print(f"[SYSTEM] risk price source={RISK_PRICE_SOURCE}; public WS not started")

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

@app.route("/api/live", methods=["GET"])
def api_live():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    bot_id = request.args.get("bot_id")
    limit = int(request.args.get("limit") or 50)
    days = int(request.args.get("days") or 7)

    try:
        events = list_trade_events(
            bot_id=_canon_bot_id(bot_id) if bot_id else None,
            limit=limit,
            days=days,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"ts": int(time.time()), "events": events}), 200


@app.route("/api/pnl_detail", methods=["GET"])
def api_pnl_detail():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    bot_id = request.args.get("bot_id")
    window = str(request.args.get("window") or "7d").lower().strip()
    windows = {
        "24h": 24 * 3600,
        "1d": 24 * 3600,
        "3d": 3 * 24 * 3600,
        "7d": 7 * 24 * 3600,
        "30d": 30 * 24 * 3600,
    }
    sec = windows.get(window, 7 * 24 * 3600)

    try:
        out = realized_pnl_by_window(sec, bot_id=_canon_bot_id(bot_id) if bot_id else None)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"ts": int(time.time()), **out}), 200


@app.route("/api/risk_config", methods=["GET"])
def api_risk_config():
    if not _require_token():
        return jsonify({"error": "forbidden"}), 403

    _ensure_monitor_thread()

    rows = []
    for i in range(1, 41):
        b = f"BOT_{i}"
        long_cfg = _get_ladder_cfg(b, "LONG")
        short_cfg = _get_ladder_cfg(b, "SHORT")
        rows.append({
            "bot_id": b,
            "long": _serialize_ladder_cfg(long_cfg) if long_cfg else None,
            "short": _serialize_ladder_cfg(short_cfg) if short_cfg else None,
        })

    return jsonify({"ts": int(time.time()), "bots": rows}), 200




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

      <div class="grid">
        <div class="card">
          <div class="row" style="align-items:center; justify-content:space-between">
            <div style="font-weight:700">Live feed (last 7d)</div>
            <div class="sub">Shows ENTRY / STOP_UPDATE / EXIT. Newest on top.</div>
          </div>
          <div class="tableWrap" style="margin-top:8px;">
            <table style="min-width:860px;">
              <thead>
                <tr>
                  <th>time (PT)</th>
                  <th>bot</th>
                  <th>symbol</th>
                  <th>dir</th>
                  <th>event</th>
                  <th>entry</th>
                  <th>stop</th>
                  <th>exit</th>
                  <th>realized</th>
                  <th>reason</th>
                </tr>
              </thead>
              <tbody id="live_body">
                <tr><td colspan="10" class="sub">Loading…</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="row" style="align-items:center; justify-content:space-between">
            <div style="font-weight:700">PnL detail</div>
            <div class="row" style="gap:10px; align-items:center">
              <label class="sub" style="margin:0">Window</label>
              <select id="window" class="small" style="width:110px">
                <option value="24h">24h</option>
                <option value="3d">3d</option>
                <option value="7d" selected>7d</option>
                <option value="30d">30d</option>
              </select>
            </div>
          </div>
          <div style="display:flex; gap:14px; margin-top:10px; flex-wrap:wrap">
            <div class="card" style="flex:1; min-width:220px">
              <div class="k">Realized (window)</div>
              <div class="v" id="d_realized">—</div>
            </div>
            <div class="card" style="flex:1; min-width:220px">
              <div class="k">Trades (window)</div>
              <div class="v" id="d_trades">—</div>
            </div>
          </div>

          <div style="height:12px"></div>

          <details>
            <summary>Risk config (which bots have SL / ladder)</summary>
            <div class="tableWrap" style="margin-top:10px;">
              <table style="min-width:860px;">
                <thead>
                  <tr>
                    <th>bot</th>
                    <th>long</th>
                    <th>short</th>
                  </tr>
                </thead>
                <tbody id="risk_body">
                  <tr><td colspan="3" class="sub">Loading…</td></tr>
                </tbody>
              </table>
            </div>
          </details>
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
        setErr(hint + (data ? ('\\n' + JSON.stringify(data, null, 2)) : ''));
        return;
      // update extra panels
      fetchLive();
      fetchDetail();
      fetchRisk();

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

    const ptTime = (ts) => {
      if (!ts) return '';
      try {
        return new Date(Number(ts) * 1000).toLocaleString([], { timeZone: 'America/Los_Angeles' });
      } catch(e) {
        return new Date(Number(ts) * 1000).toLocaleString();
      }
    };

    async function fetchLive() {
      const token = (tokenEl.value || '').trim();
      const bot = (botEl.value || '').trim();
      const url = new URL('/api/live', location.origin);
      url.searchParams.set('days', '7');
      url.searchParams.set('limit', '50');
      if (bot) url.searchParams.set('bot_id', bot);
      if (token) url.searchParams.set('token', token);

      const tbody = el('live_body');
      try {
        const res = await fetch(url.toString(), { method: 'GET' });
        const data = await res.json();
        if (!res.ok) throw new Error(JSON.stringify(data));

        const events = Array.isArray(data.events) ? data.events : [];
        tbody.innerHTML = '';
        if (!events.length) {
          const tr = document.createElement('tr');
          tr.innerHTML = `<td colspan="10" class="sub">No events in range.</td>`;
          tbody.appendChild(tr);
          return;
        }

        for (const ev of events) {
          const tr = document.createElement('tr');
          const realized = ev.realized_pnl === null || ev.realized_pnl === undefined ? '' : fmt(ev.realized_pnl);
          tr.innerHTML = `
            <td>${ptTime(ev.ts)}</td>
            <td>${ev.bot_id || ''}</td>
            <td>${ev.symbol || ''}</td>
            <td>${ev.direction || ''}</td>
            <td>${ev.event_type || ''}</td>
            <td>${ev.entry_price || ''}</td>
            <td>${ev.stop_price || ''}</td>
            <td>${ev.exit_price || ''}</td>
            <td class="${clsPNL(ev.realized_pnl)}">${realized}</td>
            <td>${ev.reason || ''}</td>
          `;
          tbody.appendChild(tr);
        }
      } catch(e) {
        tbody.innerHTML = `<tr><td colspan="10" class="sub">Live feed error: ${String(e)}</td></tr>`;
      }
    }

    async function fetchDetail() {
      const token = (tokenEl.value || '').trim();
      const bot = (botEl.value || '').trim();
      const windowSel = el('window');
      const win = windowSel ? (windowSel.value || '7d') : '7d';

      const url = new URL('/api/pnl_detail', location.origin);
      url.searchParams.set('window', win);
      if (bot) url.searchParams.set('bot_id', bot);
      if (token) url.searchParams.set('token', token);

      try {
        const res = await fetch(url.toString(), { method: 'GET' });
        const data = await res.json();
        if (!res.ok) throw new Error(JSON.stringify(data));

        el('d_realized').textContent = fmt(data.realized || 0);
        el('d_realized').className = 'v ' + clsPNL(data.realized || 0);
        el('d_trades').textContent = String(data.trades ?? 0);
      } catch(e) {
        el('d_realized').textContent = '—';
        el('d_trades').textContent = '—';
      }
    }

    async function fetchRisk() {
      const token = (tokenEl.value || '').trim();
      const url = new URL('/api/risk_config', location.origin);
      if (token) url.searchParams.set('token', token);

      const tbody = el('risk_body');
      try {
        const res = await fetch(url.toString(), { method: 'GET' });
        const data = await res.json();
        if (!res.ok) throw new Error(JSON.stringify(data));

        const bots = Array.isArray(data.bots) ? data.bots : [];
        tbody.innerHTML = '';

        const describe = (cfg) => {
          if (!cfg) return '<span class="sub">none</span>';
          const levels = Array.isArray(cfg.levels) ? cfg.levels.length : 0;
          const base = cfg.base_sl_pct === null || cfg.base_sl_pct === undefined ? '' : `${cfg.base_sl_pct}%`;
          return `<span class="pill">${cfg.name || 'cfg'}</span> <span class="sub">mode=${cfg.mode} baseSL=${base} levels=${levels}</span>`;
        };

        for (const b of bots) {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${b.bot_id || ''}</td>
            <td>${describe(b.long)}</td>
            <td>${describe(b.short)}</td>
          `;
          tbody.appendChild(tr);
        }
      } catch(e) {
        tbody.innerHTML = `<tr><td colspan="3" class="sub">Risk config error: ${String(e)}</td></tr>`;
      }
    }

    const windowEl = el('window');
    if (windowEl) windowEl.addEventListener('change', () => { fetchDetail(); });

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

        desired_dir = "LONG" if side_raw == "BUY" else "SHORT"

        # ✅ Global single-position per symbol (across ALL bots):
        # If another bot is already holding this symbol (either direction),
        # force-close the existing holder(s) first, then allow the new ENTRY.
        forced_flip = False
        flip_summary: Dict[str, Any] = {}

        if GLOBAL_SYMBOL_SINGLE_POSITION:
            with _get_mirror_lock(symbol):
                holders = _find_symbol_holders(symbol)

                # Remote fallback (covers cases where local DB is empty after restart,
                # but an exchange position still exists). Only runs for whitelisted symbols.
                if (not holders) and (symbol in REMOTE_FALLBACK_SYMBOLS):
                    try:
                        remote = get_open_position_for_symbol(symbol)
                    except Exception:
                        remote = None
                    if isinstance(remote, dict) and remote.get("size") is not None:
                        try:
                            rsz = Decimal(str(remote.get("size")))
                        except Exception:
                            rsz = Decimal("0")
                        rdir = str(remote.get("side") or "").upper().strip()
                        if rsz > 0 and rdir in ("LONG", "SHORT"):
                            res = _execute_exit_order(
                                bot_id,
                                symbol,
                                force_direction=rdir,
                                ignore_cooldown=True,
                                reason="global_symbol_flip_exit_remote",
                                sig_id=sig_id,
                            )
                            if res.get("status") not in ("ok", "no_position"):
                                return jsonify({
                                    "status": "global_flip_exit_failed",
                                    "mode": "entry",
                                    "bot_id": bot_id,
                                    "symbol": symbol,
                                    "desired_direction": desired_dir,
                                    "holder": {"bot_id": "REMOTE", "direction": rdir},
                                    "holder_exit_status": res.get("status"),
                                    "signal_id": sig_id,
                                }), 200
                            # Wait briefly for exchange position to clear before continuing.
                            deadline = time.time() + max(0.5, float(GLOBAL_FLIP_WAIT_SEC or 0))
                            while time.time() < deadline:
                                try:
                                    chk = get_open_position_for_symbol(symbol)
                                except Exception:
                                    chk = None
                                if not isinstance(chk, dict) or chk.get("size") in (None, "", "0", "0.0"):
                                    break
                                try:
                                    csz = Decimal(str(chk.get("size")))
                                except Exception:
                                    csz = Decimal("0")
                                if csz <= 0:
                                    break
                                time.sleep(0.15)
                            forced_flip = True

                # If the only holder is THIS bot and direction matches, do nothing.
                same_owner_same_dir = (
                    len(holders) == 1
                    and holders[0].get("bot_id") == bot_id
                    and holders[0].get("direction") == desired_dir
                )

                if holders and (not same_owner_same_dir):
                    results: List[Dict[str, Any]] = []
                    for h in holders:
                        h_bot = _canon_bot_id(h.get("bot_id"))
                        h_dir = str(h.get("direction") or "").upper().strip()
                        if h_dir not in ("LONG", "SHORT"):
                            continue
                        res = _execute_exit_order(
                            h_bot,
                            symbol,
                            force_direction=h_dir,
                            ignore_cooldown=True,
                            reason="global_symbol_flip_exit",
                            sig_id=sig_id,
                        )
                        results.append({
                            "bot_id": h_bot,
                            "direction": h_dir,
                            "requested_qty": str(h.get("qty")) if h.get("qty") is not None else "0",
                            "exit_status": res.get("status"),
                        })

                        # If we could not confirm fills, fail-closed.
                        if res.get("status") not in ("ok", "no_position"):
                            return jsonify({
                                "status": "global_flip_exit_failed",
                                "mode": "entry",
                                "bot_id": bot_id,
                                "symbol": symbol,
                                "desired_direction": desired_dir,
                                "holder": {"bot_id": h_bot, "direction": h_dir},
                                "holder_exit_status": res.get("status"),
                                "signal_id": sig_id,
                            }), 200

                    # Wait for all directions to clear before opening the new position.
                    deadline = time.time() + max(0.5, float(GLOBAL_FLIP_WAIT_SEC or 0))
                    while time.time() < deadline:
                        dirs = get_symbol_open_directions(symbol)
                        if not dirs:
                            break
                        time.sleep(0.15)

                    dirs = get_symbol_open_directions(symbol)
                    if dirs:
                        return jsonify({
                            "status": "global_flip_exit_incomplete",
                            "mode": "entry",
                            "bot_id": bot_id,
                            "symbol": symbol,
                            "desired_direction": desired_dir,
                            "remaining_open_directions": sorted(list(dirs)),
                            "flip_results": results,
                            "signal_id": sig_id,
                        }), 200

                    forced_flip = True
                    flip_summary = {"flip_results": results}


        # ✅ Entry guard: if a CLOSE arrived together with OPEN, block the OPEN.
        # BUT: when we just forced a global flip (close->open), we must allow the OPEN.
        if not forced_flip:
            rej = _entry_guard_reject(symbol, tv_client_id=str(tv_client_id or ""))
            if rej is not None:
                rej.update({"mode": "entry", "bot_id": bot_id, "signal_id": sig_id})
                return jsonify(rej), 200

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
            return jsonify({
                "status": "qty_compute_error",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "error": str(e),
                "signal_id": sig_id,
            }), 200

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
        data_brief = (order or {}).get("data") or {}
        try:
            code = data_brief.get("code")
            msg = data_brief.get("msg")
        except Exception:
            code = None
            msg = None
        print(f"[ENTRY] order status={status} cancelReason={cancel_reason!r} orderId={data_brief.get('orderId')} code={code} msg={msg!r}")

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
                "code": code,
                "msg": msg,
                "signal_id": sig_id,
            }), 200

        order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")

        # If we did not receive an orderId, fail fast instead of waiting for fills.
        # This is the most common case when size step/precision is invalid.
        if not order_id:
            return jsonify({
                "status": "order_rejected",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "request_qty": size_str,
                "order_status": status or "REJECTED",
                "cancel_reason": cancel_reason or (msg or "missing orderId"),
                "code": code,
                "msg": msg,
                "signal_id": sig_id,
            }), 200

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
                pos_wait = float(os.getenv("POS_FALLBACK_MAX_WAIT_SEC", "2.5"))
                deadline = time.time() + max(0.2, pos_wait)
                while time.time() < deadline:
                    pos = get_open_position_for_symbol(symbol)
                    if isinstance(pos, dict) and pos.get("side") and pos.get("size") is not None:
                        try:
                            pside = str(pos.get("side")).upper().strip()
                            psize = Decimal(str(pos.get("size")))
                            epx = pos.get("entryPrice") or pos.get("entry_price") or pos.get("avgEntryPrice")
                            epx_dec = _to_decimal(epx, default=Decimal("0"))
                        except Exception:
                            pside, psize, epx_dec = "", Decimal("0"), Decimal("0")

                        if side_raw == "BUY" and pside == "LONG":
                            if psize > pre_pos_size and epx_dec > 0:
                                entry_price_dec = epx_dec
                                final_qty = (psize - pre_pos_size) if (psize - pre_pos_size) > 0 else snapped_qty
                                break
                        elif side_raw == "SELL" and pside == "SHORT":
                            if psize > pre_pos_size and epx_dec > 0:
                                entry_price_dec = epx_dec
                                final_qty = (psize - pre_pos_size) if (psize - pre_pos_size) > 0 else snapped_qty
                                break

                    time.sleep(0.15)
            except Exception:
                pass

        if entry_price_dec is None or entry_price_dec <= 0:
            # If we still can't confirm, fail-closed (do not record fake entry).
            return jsonify({
                "status": "fill_unavailable",
                "mode": "entry",
                "bot_id": bot_id,
                "symbol": symbol,
                "side": side_raw,
                "request_qty": size_str,
                "order_status": status,
                "cancel_reason": cancel_reason,
                "signal_id": sig_id,
            }), 200

        # Record entry into PnL DB
        try:
            record_entry(
                bot_id=bot_id,
                symbol=symbol,
                side=side_raw,
                qty=final_qty,
                price=entry_price_dec,
            )
        except Exception as e:
            print("[PNL] record_entry error:", e)

        # Record dashboard event (ENTRY)
        try:
            direction = "LONG" if side_raw == "BUY" else "SHORT"
            cfg = _get_ladder_cfg(bot_id, direction)
            lock_pct = None
            stop_price = None
            if cfg and cfg.get("base_sl_pct") is not None:
                base_sl = Decimal(str(cfg.get("base_sl_pct")))
                lock_pct = -base_sl
                stop_price = _compute_stop_price(direction, entry_price_dec, lock_pct)

            record_trade_event(
                bot_id=bot_id,
                symbol=symbol,
                direction=direction,
                event_type="ENTRY",
                qty=final_qty,
                entry_price=entry_price_dec,
                stop_price=stop_price,
                lock_level_pct=lock_pct,
                reason="webhook_entry",
            )
        except Exception as e:
            print("[DASH] record_trade_event ENTRY error:", e)

        # Local cache for speed
        BOT_POSITIONS[(bot_id, symbol)] = {
            "side": side_raw,
            "qty": final_qty,
            "entry_price": entry_price_dec,
        }

        return jsonify({
            "status": "ok",
            "mode": "entry",
            "bot_id": bot_id,
            "symbol": symbol,
            "side": side_raw,
            "qty": str(final_qty),
            "entry_price": str(entry_price_dec),
            "forced_flip": forced_flip,
            **(flip_summary or {}),
            "signal_id": sig_id,
        }), 200

    # -------------------------
    # EXIT
    # -------------------------
    else:
        # Mark this symbol as just-exited (for CLOSE->OPEN guard)
        _mark_symbol_exit(symbol, tv_client_id=str(tv_client_id or ""))

        # Determine direction hint from side if provided
        direction_hint = ""
        if side_raw == "BUY":
            direction_hint = "SHORT"  # buy to close short
        elif side_raw == "SELL":
            direction_hint = "LONG"   # sell to close long

        res = _execute_exit_order(
            bot_id=bot_id,
            symbol=symbol,
            force_direction=direction_hint,
            ignore_cooldown=False,
            reason="strategy_exit",
            sig_id=sig_id,
        )
        res["signal_id"] = sig_id
        return jsonify(res), 200


if __name__ == "__main__":
    # Local dev only; production should use gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
