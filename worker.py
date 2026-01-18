import os
import time
import threading
from decimal import Decimal

from pnl_store import (
    init_db,
    list_pending_orders,
    touch_pending_try,
    mark_pending_done,
    mark_pending_failed,
    record_entry,
    record_exit_fifo,
    set_lock_level_pct,
    clear_lock_level_pct,
)

from apex_client import get_fill_summary

# Reuse the monitor/risk threads implemented inside the web app module
import app_any_amount_v2 as app_module


def _call_if_exists(name: str) -> bool:
    fn = getattr(app_module, name, None)
    if callable(fn):
        fn()
        return True
    return False


def main():
    # Ensure DB initialized for PnL / positions
    init_db()

    # Fail-safe defaults (if supervisor/env didn't inject them)
    os.environ.setdefault("ENABLE_WS", "1")
    os.environ.setdefault("ENABLE_REST_POLL", "1")
    os.environ.setdefault("ENABLE_RISK_LOOP", "1")  # <-- bot-side SL + ladder trailing
    os.environ.setdefault("ENABLE_EXCHANGE_PROTECTIVE", "0")

    # Pending reconcile defaults
    os.environ.setdefault("PENDING_RETRY_GAP_SEC", "1")
    os.environ.setdefault("PENDING_FILL_WAIT_SEC", "2.0")
    os.environ.setdefault("PENDING_FILL_POLL_INTERVAL", "0.2")

    started_any = False

    # Start monitor thread (private WS + fills/orders handling)
    for n in ("_ensure_monitor_thread", "ensure_monitor_thread", "start_monitor_thread"):
        if _call_if_exists(n):
            print(f"[worker] started monitor via {n}()")
            started_any = True
            break
    else:
        print("[worker] WARNING: no monitor starter found in app module")

    # Start risk thread (ladder SL/TS loop)
    for n in ("_ensure_risk_thread", "ensure_risk_thread", "start_risk_thread"):
        if _call_if_exists(n):
            print(f"[worker] started risk loop via {n}()")
            started_any = True
            break
    else:
        print("[worker] WARNING: no risk-loop starter found in app module")

    if not started_any:
        print("[worker] ERROR: nothing was started. Check the app module for thread entrypoints.")
    else:
        print("[worker] OK: worker running (WS + optional risk loop).")

    # ------------------------------------------------------------------
    # Background reconciliation: record entries/exits when fills are delayed
    # ------------------------------------------------------------------
    def _reconcile_pending_loop():
        while True:
            try:
                gap = int(float(os.getenv("PENDING_RETRY_GAP_SEC", "1")))
                pendings = list_pending_orders(limit=50, min_retry_gap_sec=max(1, gap))
                if not pendings:
                    time.sleep(0.5)
                    continue

                for p in pendings:
                    bot_id = str(p.get("bot_id"))
                    sig_id = str(p.get("signal_id"))
                    symbol = str(p.get("symbol"))
                    mode = str(p.get("mode"))
                    side = str(p.get("side"))
                    direction = str(p.get("direction"))
                    order_id = str(p.get("order_id") or "")
                    client_order_id = str(p.get("client_order_id") or "")

                    if not bot_id or not sig_id or not symbol or (not order_id and not client_order_id):
                        mark_pending_failed(bot_id, sig_id, note="missing_keys")
                        continue

                    touch_pending_try(bot_id, sig_id)

                    try:
                        fill = get_fill_summary(
                            symbol=symbol,
                            order_id=order_id,
                            client_order_id=client_order_id,
                            max_wait_sec=float(os.getenv("PENDING_FILL_WAIT_SEC", "2.0")),
                            poll_interval=float(os.getenv("PENDING_FILL_POLL_INTERVAL", "0.2")),
                        )
                    except Exception:
                        continue

                    try:
                        qty = Decimal(str(fill.get("filled_qty") or "0"))
                        px = Decimal(str(fill.get("avg_fill_price") or "0"))
                    except Exception:
                        continue

                    if qty <= 0 or px <= 0:
                        continue

                    if mode == "entry":
                        try:
                            record_entry(
                                bot_id=bot_id,
                                symbol=symbol,
                                side=side,
                                qty=qty,
                                price=px,
                                reason="pending_entry_reconcile",
                            )
                        except Exception:
                            # likely already recorded; treat as done
                            pass

                        # Initialize ladder base lock if needed
                        try:
                            if hasattr(app_module, "_bot_uses_ladder") and app_module._bot_uses_ladder(bot_id, direction):
                                base = getattr(app_module, "LADDER_BASE_SL_PCT", Decimal("0.5"))
                                try:
                                    set_lock_level_pct(bot_id, symbol, direction, -Decimal(str(base)))
                                except Exception:
                                    pass
                        except Exception:
                            pass

                        mark_pending_done(bot_id, sig_id, note=f"reconciled_entry qty={qty} px={px}")

                    elif mode == "exit":
                        entry_side = side  # side is the entry side for record_exit_fifo
                        try:
                            record_exit_fifo(
                                bot_id=bot_id,
                                symbol=symbol,
                                entry_side=entry_side,
                                exit_qty=qty,
                                exit_price=px,
                                reason="pending_exit_reconcile",
                            )
                        except Exception:
                            pass
                        try:
                            clear_lock_level_pct(bot_id, symbol, direction)
                        except Exception:
                            pass
                        mark_pending_done(bot_id, sig_id, note=f"reconciled_exit qty={qty} px={px}")
                    else:
                        mark_pending_failed(bot_id, sig_id, note=f"unknown_mode:{mode}")

            except Exception as e:
                print("[worker][pending] reconcile loop error:", e)
                time.sleep(1)

    threading.Thread(target=_reconcile_pending_loop, daemon=True, name="pending-reconcile").start()
    print("[worker] started pending reconcile loop")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
