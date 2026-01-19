import os
import time
import threading
from decimal import Decimal
import importlib

import requests

import pnl_store as _pnl

# --- Required pnl_store APIs (must exist) ---
init_db = _pnl.init_db
record_entry = _pnl.record_entry
record_exit_fifo = _pnl.record_exit_fifo
set_lock_level_pct = _pnl.set_lock_level_pct
clear_lock_level_pct = _pnl.clear_lock_level_pct

# --- Optional pnl_store APIs (may not exist in some repo versions) ---
list_pending_orders = getattr(_pnl, "list_pending_orders", None)
touch_pending_try = getattr(_pnl, "touch_pending_try", None)
mark_pending_done = getattr(_pnl, "mark_pending_done", None)
mark_pending_failed = getattr(_pnl, "mark_pending_failed", None)

from apex_client import get_fill_summary


# Will be set in main() via _import_app_module()
app_module = None


def _import_app_module():
    """Import the web app module that contains monitor/risk thread entrypoints.

    Different repo versions may use different filenames:
      - app.py            -> module "app"
      - app_any_amount_v2.py -> module "app_any_amount_v2"
      - app_any_amount.py -> module "app_any_amount"
      - main.py / server.py etc.

    Priority:
      1) env WORKER_APP_MODULE if set
      2) fallback candidates list
    """
    env_mod = (os.getenv("WORKER_APP_MODULE") or "").strip()
    candidates = [env_mod] if env_mod else []
    candidates += ["app", "app_any_amount_v2", "app_any_amount", "main", "server"]

    last_err = None
    for mod in candidates:
        if not mod:
            continue
        try:
            m = importlib.import_module(mod)
            print(f"[worker] using app module: {mod}")
            return m
        except Exception as e:
            last_err = e

    raise ModuleNotFoundError(
        f"[worker] cannot import any app module. candidates={candidates}. "
        f"Set WORKER_APP_MODULE to your correct module name. last_error={last_err}"
    )


def _call_if_exists(name: str) -> bool:
    global app_module
    if app_module is None:
        return False
    fn = getattr(app_module, name, None)
    if callable(fn):
        fn()
        return True
    return False


def main():
    global app_module

    # Import app module dynamically (fixes ModuleNotFoundError on app_any_amount_v2)
    app_module = _import_app_module()

    # init db early
    try:
        init_db()
    except Exception as e:
        print("[worker] init_db error:", e)

    # Start monitor thread (WS / REST polling) from app module if present
    started_any = False
    if _call_if_exists("_ensure_monitor_thread") or _call_if_exists("ensure_monitor_thread"):
        print("[worker] started monitor via app module")
        started_any = True

    # Start ladder/risk loop from app module if present
    if _call_if_exists("_ensure_risk_thread") or _call_if_exists("ensure_risk_thread"):
        print("[worker] started risk loop via app module")
        started_any = True

    if not started_any:
        print("[worker] WARNING: app module has no monitor/risk thread entrypoints")

    # ------------------------------------------------------------------
    # Background reconciliation: record entries/exits when fills are delayed
    # This is optional and depends on pnl_store pending helpers.
    # ------------------------------------------------------------------
    def _reconcile_pending_loop():
        while True:
            try:
                gap = int(float(os.getenv("PENDING_RETRY_GAP_SEC", "1")))
                max_age = int(float(os.getenv("PENDING_MAX_AGE_SEC", "60")))
                max_tries = int(float(os.getenv("PENDING_MAX_TRIES", "20")))

                pendings = list_pending_orders(max_age_sec=max_age, max_tries=max_tries)
                if not pendings:
                    time.sleep(gap)
                    continue

                for p in pendings:
                    bot_id = str(p.get("bot_id"))
                    sig_id = str(p.get("sig_id"))
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

    helpers_ok = all(
        fn is not None
        for fn in (
            list_pending_orders,
            touch_pending_try,
            mark_pending_done,
            mark_pending_failed,
        )
    )

    if os.getenv("ENABLE_PENDING_RECONCILE", "0") == "1" and helpers_ok:
        threading.Thread(target=_reconcile_pending_loop, daemon=True, name="pending-reconcile").start()
        print("[worker] pending reconcile ENABLED")
    else:
        if not helpers_ok:
            print("[worker] pending reconcile DISABLED: pnl_store.py has no pending-order helpers")
        else:
            print("[worker] pending reconcile disabled (ENABLE_PENDING_RECONCILE!=1)")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
