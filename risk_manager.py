# risk_manager.py
import os
import time
from decimal import Decimal

import pnl_store
import apex_client


def _parse_bot_ids(s: str) -> set[str]:
    out = set()
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.add(x)
    return out


LONG_RISK_BOTS = _parse_bot_ids(os.getenv("LONG_RISK_BOTS", "BOT_1,BOT_2,BOT_3,BOT_4,BOT_5"))
SHORT_RISK_BOTS = _parse_bot_ids(os.getenv("SHORT_RISK_BOTS", "BOT_6,BOT_7,BOT_8,BOT_9,BOT_10"))

BASE_SL_PCT = Decimal(os.getenv("BASE_SL_PCT", "-0.5"))  # -0.5 (%)

# Your ladder:
# +0.15 => lock 0.10
# +0.45 => lock 0.20
# +0.55 => lock 0.30
# +0.65 => lock 0.40
# then continue +0.10 each +0.10 pnl beyond 0.65
T1 = Decimal(os.getenv("LOCK_T1_PNL", "0.15"))
L1 = Decimal(os.getenv("LOCK_L1", "0.10"))
T2 = Decimal(os.getenv("LOCK_T2_PNL", "0.45"))
L2 = Decimal(os.getenv("LOCK_L2", "0.20"))
T3 = Decimal(os.getenv("LOCK_T3_PNL", "0.55"))
L3 = Decimal(os.getenv("LOCK_L3", "0.30"))
T4 = Decimal(os.getenv("LOCK_T4_PNL", "0.65"))
L4 = Decimal(os.getenv("LOCK_L4", "0.40"))
STEP = Decimal(os.getenv("LOCK_STEP", "0.10"))


def _pnl_pct(pos_side: str, entry: Decimal, mark: Decimal) -> Decimal:
    if entry <= 0:
        return Decimal("0")
    if pos_side == "LONG":
        return (mark - entry) / entry * Decimal("100")
    else:
        return (entry - mark) / entry * Decimal("100")


def _next_lock_level(pnl: Decimal, current_lock: Decimal) -> Decimal:
    lock = current_lock
    if pnl >= T1:
        lock = max(lock, L1)
    if pnl >= T2:
        lock = max(lock, L2)
    if pnl >= T3:
        lock = max(lock, L3)
    if pnl >= T4:
        lock = max(lock, L4)
        # continue upward
        extra = pnl - T4
        if extra >= STEP:
            n = (extra / STEP).to_integral_value(rounding="ROUND_FLOOR")
            lock = max(lock, L4 + n * STEP)
    return lock


def _bot_allowed(bot_id: str, pos_side: str) -> bool:
    if pos_side == "LONG":
        return bot_id in LONG_RISK_BOTS
    if pos_side == "SHORT":
        return bot_id in SHORT_RISK_BOTS
    return False


def run_loop(poll_s: float = 1.0):
    while True:
        try:
            for p in pnl_store.list_positions():
                bot_id = p["bot_id"]
                symbol = p["symbol"]
                pos_side = p["side"]  # LONG/SHORT

                if not _bot_allowed(bot_id, pos_side):
                    continue
                if int(p.get("closing") or 0) == 1:
                    continue

                qty = Decimal(p["qty"])
                entry = Decimal(p["entry_price"])
                current_lock = Decimal(p["lock_level_pct"])

                # mark price: prefer public ticker, fallback to order-based later
                mark = apex_client.get_ticker_price(symbol) or entry
                pnl = _pnl_pct(pos_side, entry, mark)

                new_lock = _next_lock_level(pnl, current_lock)
                if new_lock != current_lock:
                    pnl_store.set_lock_level(bot_id, symbol, new_lock)

                stop_line = max(BASE_SL_PCT, new_lock)

                # stop triggers when pnl <= stop_line
                if pnl <= stop_line:
                    # trigger close
                    pnl_store.mark_closing(bot_id, symbol, True)
                    exit_side = "SELL" if pos_side == "LONG" else "BUY"

                    res, client_id = apex_client.create_market_order(
                        symbol=symbol,
                        side=exit_side,
                        size=qty,
                        reduce_only=True,
                    )
                    order_id = (res.get("data") or {}).get("id") if isinstance(res, dict) else None

                    fs = apex_client.wait_for_fill(order_id or "", client_id, fast_wait_s=3.0, slow_wait_s=60.0)
                    exit_price = fs["avg_price"] if fs["avg_price"] > 0 else (mark)

                    pnl_store.add_trade(bot_id, symbol, "CLOSE", exit_side, qty, exit_price, f"RISK_STOP pnl={pnl:.4f}% stop={stop_line:.4f}%", order_id, client_id)
                    pnl_store.delete_position(bot_id, symbol)

        except Exception as e:
            # keep loop alive
            print("[RISK] loop error:", repr(e))

        time.sleep(poll_s)
