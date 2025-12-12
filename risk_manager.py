import os
import time
import threading
from decimal import Decimal
from typing import Set, Optional

from apex_client import create_market_order, get_market_price, _get_symbol_rules
from pnl_store import (
    get_bot_open_positions,
    record_exit_fifo,
    get_lock_level_pct,
    set_lock_level_pct,
    clear_lock_level_pct,
)

# ✅ BOT 分组（由 app.py 在启动时注入）
_LONG_RISK_BOTS: Set[str] = set()
_SHORT_RISK_BOTS: Set[str] = set()

# 风控轮询间隔
RISK_POLL_INTERVAL = float(os.getenv("RISK_POLL_INTERVAL", "1.0"))

# 基础止损：-0.5%
BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "0.5"))

# 你的阶梯锁盈（单位：%）
# 0.15 -> lock 0.10
# 0.45 -> lock 0.20
# 0.55 -> lock 0.30
# 0.65 -> lock 0.40
# 之后每 +0.10 pnl -> lock +0.10
TRIGGER_0 = Decimal(os.getenv("TP_TRIGGER_0", "0.15"))
LOCK_0 = Decimal(os.getenv("TP_LOCK_0", "0.10"))
TRIGGER_1 = Decimal(os.getenv("TP_TRIGGER_1", "0.45"))
LOCK_1 = Decimal(os.getenv("TP_LOCK_1", "0.20"))
TRIGGER_2 = Decimal(os.getenv("TP_TRIGGER_2", "0.55"))
LOCK_2 = Decimal(os.getenv("TP_LOCK_2", "0.30"))
TRIGGER_3 = Decimal(os.getenv("TP_TRIGGER_3", "0.65"))
LOCK_3 = Decimal(os.getenv("TP_LOCK_3", "0.40"))
AFTER_STEP = Decimal(os.getenv("TP_AFTER_STEP", "0.10"))
AFTER_LOCK_STEP = Decimal(os.getenv("TP_AFTER_LOCK_STEP", "0.10"))

_MONITOR_STARTED = False
_MONITOR_LOCK = threading.Lock()


def update_runtime_bot_groups(long_bots: Set[str], short_bots: Set[str]):
    global _LONG_RISK_BOTS, _SHORT_RISK_BOTS
    _LONG_RISK_BOTS = set(long_bots or set())
    _SHORT_RISK_BOTS = set(short_bots or set())
    print(f"[RISK] groups updated long={sorted(_LONG_RISK_BOTS)} short={sorted(_SHORT_RISK_BOTS)}")


def _bot_allows_direction(bot_id: str, direction: str) -> bool:
    d = str(direction or "").upper()
    if d == "LONG" and bot_id in _LONG_RISK_BOTS:
        return True
    if d == "SHORT" and bot_id in _SHORT_RISK_BOTS:
        return True
    return False


def _calc_pnl_pct(direction: str, entry_price: Decimal, current_price: Decimal) -> Decimal:
    if entry_price <= 0:
        return Decimal("0")
    if direction.upper() == "LONG":
        return (current_price - entry_price) * Decimal("100") / entry_price
    else:
        return (entry_price - current_price) * Decimal("100") / entry_price


def _base_sl_hit(direction: str, entry_price: Decimal, current_price: Decimal) -> bool:
    pct = BASE_SL_PCT / Decimal("100")
    if direction.upper() == "LONG":
        stop_price = entry_price * (Decimal("1") - pct)
        return current_price <= stop_price
    else:
        stop_price = entry_price * (Decimal("1") + pct)
        return current_price >= stop_price


def _desired_lock_level_pct(pnl_pct: Decimal) -> Decimal:
    # 你的固定档位 + 后续线性扩展
    if pnl_pct < TRIGGER_0:
        return Decimal("0")
    if pnl_pct < TRIGGER_1:
        return LOCK_0
    if pnl_pct < TRIGGER_2:
        return LOCK_1
    if pnl_pct < TRIGGER_3:
        return LOCK_2

    # >= 0.65 起，每 +0.10 pnl -> lock +0.10
    extra_steps = (pnl_pct - TRIGGER_3) // AFTER_STEP
    return LOCK_3 + (AFTER_LOCK_STEP * extra_steps)


def _lock_hit(direction: str, entry_price: Decimal, current_price: Decimal, lock_level_pct: Decimal) -> bool:
    if lock_level_pct <= 0:
        return False
    pct = lock_level_pct / Decimal("100")
    if direction.upper() == "LONG":
        lock_price = entry_price * (Decimal("1") + pct)
        return current_price <= lock_price
    else:
        lock_price = entry_price * (Decimal("1") - pct)
        return current_price >= lock_price


def _get_current_price(symbol: str, direction: str) -> Optional[Decimal]:
    try:
        rules = _get_symbol_rules(symbol)
        min_qty = rules["min_qty"]
        exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
        px_str = get_market_price(symbol, exit_side, str(min_qty))
        px = Decimal(str(px_str))
        return px if px > 0 else None
    except Exception as e:
        print(f"[RISK] get_market_price error symbol={symbol} direction={direction}:", e)
        return None


def _execute_exit(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    if qty <= 0:
        return

    direction_u = direction.upper()
    entry_side = "BUY" if direction_u == "LONG" else "SELL"
    exit_side = "SELL" if direction_u == "LONG" else "BUY"

    exit_price = _get_current_price(symbol, direction_u)
    if exit_price is None:
        print(f"[RISK] skip exit due to no price bot={bot_id} symbol={symbol}")
        return

    print(f"[RISK] EXIT bot={bot_id} symbol={symbol} dir={direction_u} qty={qty} reason={reason}")

    try:
        order = create_market_order(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            client_id=None,
        )
        # 记账（用 mark 价；你如果要完全用 fills，这个在 app.py 的策略 exit 已经做了 fill-first）
        record_exit_fifo(
            bot_id=bot_id,
            symbol=symbol,
            entry_side=entry_side,
            exit_qty=qty,
            exit_price=exit_price,
            reason=reason,
        )
        clear_lock_level_pct(bot_id, symbol, direction_u)
        return
    except Exception as e:
        print(f"[RISK] exit error bot={bot_id} symbol={symbol}:", e)


def _risk_loop():
    print("[RISK] thread started")
    while True:
        try:
            bots = sorted(list(_LONG_RISK_BOTS | _SHORT_RISK_BOTS))
            for bot_id in bots:
                opens = get_bot_open_positions(bot_id)

                for (symbol, direction), v in opens.items():
                    direction_u = direction.upper()
                    if not _bot_allows_direction(bot_id, direction_u):
                        continue

                    qty = v["qty"]
                    entry_price = v["weighted_entry"]
                    if qty <= 0 or entry_price <= 0:
                        continue

                    current_price = _get_current_price(symbol, direction_u)
                    if current_price is None:
                        continue

                    pnl_pct = _calc_pnl_pct(direction_u, entry_price, current_price)

                    # 1) 基础止损优先
                    if _base_sl_hit(direction_u, entry_price, current_price):
                        _execute_exit(bot_id, symbol, direction_u, qty, "base_sl_exit")
                        continue

                    # 2) 锁盈档位更新
                    desired_lock = _desired_lock_level_pct(pnl_pct)
                    current_lock = get_lock_level_pct(bot_id, symbol, direction_u)

                    if desired_lock > current_lock:
                        set_lock_level_pct(bot_id, symbol, direction_u, desired_lock)
                        current_lock = desired_lock
                        print(f"[RISK] LOCK UP bot={bot_id} {symbol} {direction_u} pnl={pnl_pct:.4f}% lock={current_lock}%")

                    # 3) 锁盈触发
                    if _lock_hit(direction_u, entry_price, current_price, current_lock):
                        _execute_exit(bot_id, symbol, direction_u, qty, "ladder_lock_exit")

        except Exception as e:
            print("[RISK] top-level error:", e)

        time.sleep(RISK_POLL_INTERVAL)


def start_risk_manager():
    global _MONITOR_STARTED
    with _MONITOR_LOCK:
        if _MONITOR_STARTED:
            return
        t = threading.Thread(target=_risk_loop, daemon=True)
        t.start()
        _MONITOR_STARTED = True
