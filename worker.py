import os
import time

from pnl_store import init_db

# Reuse the monitor/risk threads implemented inside app.py
import app as app_module


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

    started_any = False

    # Start monitor thread (private WS + fills/orders handling)
    for n in ("_ensure_monitor_thread", "ensure_monitor_thread", "start_monitor_thread"):
        if _call_if_exists(n):
            print(f"[worker] started monitor via {n}()")
            started_any = True
            break
    else:
        print("[worker] WARNING: no monitor starter found in app.py")

    # Start risk thread (ladder SL/TS loop)
    for n in ("_ensure_risk_thread", "ensure_risk_thread", "start_risk_thread"):
        if _call_if_exists(n):
            print(f"[worker] started risk loop via {n}()")
            started_any = True
            break
    else:
        print("[worker] WARNING: no risk-loop starter found in app.py")

    if not started_any:
        print("[worker] ERROR: nothing was started. Check app.py for thread entrypoints.")
    else:
        print("[worker] OK: worker running (WS + optional risk loop).")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
