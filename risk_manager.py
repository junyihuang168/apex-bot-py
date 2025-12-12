import os
import threading
import time
from decimal import Decimal

import apex_client
import pnl_store


# Global registry to avoid starting duplicated watchers
_WATCHERS = {}
_WATCHERS_LOCK = threading.Lock()


def _d(x) -> Decimal:
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _calc_profit_pct(direction: str, entry_price: Decimal, current_price: Decimal) -> Decimal:
    """
    Profit in percent (not decimal), e.g. 0.15 means +0.15%
    """
    if entry_price <= 0 or current_price <= 0:
        return Decimal("0")
    if direction.upper() == "LONG":
        return (current_price - entry_price) / entry_price * Decimal("100")
    else:
        return (entry_price - current_price) / entry_price * Decimal("100")


def _ladder_lock_pct(peak_profit_pct: Decimal) -> Decimal:
    """
    Implements EXACTLY your rule style:
    - fee round trip = 0.10, so first lock is 0.10 at profit 0.15
    - 0.45 -> lock 0.2
    - 0.55 -> lock 0.3
    - 0.65 -> lock 0.4
    - keep going: each +0.10 profit beyond 0.65 increases lock by +0.10
    """
    p = _d(peak_profit_pct)

    if p < Decimal("0.15"):
        return Decimal("0")

    # base points
    if p < Decimal("0.45"):
        return Decimal("0.10")
    if p < Decimal("0.55"):
        return Decimal("0.20")
    if p < Decimal("0.65"):
        return Decimal("0.30")

    # p >= 0.65
    # 0.65 -> 0.40, 0.75 -> 0.50, 0.85 -> 0.60 ...
    extra = p - Decimal("0.65")
    k = int((extra // Decimal("0.10")))
    return Decimal("0.40") + Decimal("0.10") * Decimal(k)


def _get_executable_exit_price(symbol: str, direction: str, qty: Decimal) -> Decimal:
    """
    Use executable worst price for the exit side as a conservative proxy for current.
    This is NOT used as baseline; baseline is fill avg entry.
    """
    exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
    px = apex_client.get_market_price(symbol, exit_side, str(qty))
    return _d(px)


def _risk_loop(bot_id: str, symbol: str, direction: str, poll_sec: float):
    key = (bot_id, symbol, direction)
    try:
        while True:
            st = pnl_store.get_risk_state(bot_id, symbol, direction)
            if not st:
                return

            if str(st.get("status")) != "ACTIVE":
                return

            entry_price = _d(st["entry_fill_price"])
            entry_qty = _d(st["entry_qty"])
            sl_pct = _d(st["sl_pct"])
            peak_profit = _d(st["peak_profit_pct"])
            lock_pct = _d(st["lock_level_pct"])

            # reconcile remote position size if possible
            pos = apex_client.get_open_position_for_symbol(symbol)
            if pos and pos.get("side") in ("LONG", "SHORT"):
                # if direction mismatched, freeze
                if pos["side"] != direction.upper():
                    pnl_store.set_risk_status(bot_id, symbol, direction, "FROZEN")
                    return
                if pos.get("size") and _d(pos["size"]) > 0:
                    entry_qty = _d(pos["size"])

            if entry_qty <= 0:
                pnl_store.set_risk_status(bot_id, symbol, direction, "CLOSED")
                pnl_store.clear_risk_state(bot_id, symbol, direction)
                return

            # Current executable exit price (conservative)
            current_px = _get_executable_exit_price(symbol, direction, entry_qty)
            profit_pct = _calc_profit_pct(direction, entry_price, current_px)

            # update peak / lock
            if profit_pct > peak_profit:
                peak_profit = profit_pct

            desired_lock = _ladder_lock_pct(peak_profit)
            if desired_lock > lock_pct:
                lock_pct = desired_lock

            pnl_store.update_risk_progress(bot_id, symbol, direction, peak_profit, lock_pct)

            # Stop loss
            if profit_pct <= sl_pct:
                _close_position_fill_first(bot_id, symbol, direction, entry_qty, reason=f"SL_{sl_pct}%")
                return

            # Trailing lock take-profit
            if lock_pct > 0 and profit_pct <= lock_pct:
                _close_position_fill_first(bot_id, symbol, direction, entry_qty, reason=f"LOCK_{lock_pct}%")
                return

            time.sleep(poll_sec)

    finally:
        with _WATCHERS_LOCK:
            _WATCHERS.pop(key, None)


def _close_position_fill_first(bot_id: str, symbol: str, direction: str, qty: Decimal, reason: str):
    """
    Reduce-only close using fill-first (must obtain real exit fills).
    """
    exit_side = "SELL" if direction.upper() == "LONG" else "BUY"
    entry_side = "BUY" if direction.upper() == "LONG" else "SELL"

    try:
        res = apex_client.create_market_order_fill_first(
            symbol=symbol,
            side=exit_side,
            size=str(qty),
            reduce_only=True,
            tv_signal_id=f"{bot_id}:{symbol}:{direction}:RISK_CLOSE",
        )
    except apex_client.FillUnavailableError as e:
        # cannot obtain exit fills -> freeze (do not write fake pnl)
        pnl_store.set_risk_status(bot_id, symbol, direction, "FROZEN")
        print("[risk_manager] EXIT fill unavailable, freeze:", e)
        return

    filled_qty = Decimal(res["fill"]["filled_qty"])
    avg_exit = Decimal(res["fill"]["avg_fill_price"])

    pnl_store.record_exit_fifo(
        bot_id=bot_id,
        symbol=symbol,
        entry_side=entry_side,
        exit_qty=filled_qty,
        exit_price=avg_exit,
        reason=reason,
    )

    pnl_store.set_risk_status(bot_id, symbol, direction, "CLOSED")
    pnl_store.clear_risk_state(bot_id, symbol, direction)
    print(f"[risk_manager] closed {bot_id} {symbol} {direction} qty={filled_qty} exit={avg_exit} reason={reason}")


def start_risk_watcher(bot_id: str, symbol: str, direction: str, poll_sec: float = 0.6):
    """
    Starts a per-bot watcher if not running.
    """
    key = (bot_id, symbol, direction.upper())
    with _WATCHERS_LOCK:
        if key in _WATCHERS:
            return
        t = threading.Thread(target=_risk_loop, args=(bot_id, symbol, direction.upper(), poll_sec), daemon=True)
        _WATCHERS[key] = t
        t.start()
