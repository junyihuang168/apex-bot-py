import os
import time
import importlib

from pnl_store import init_db

"""
Worker (Plan A - Exchange-native trailing stop, trigger = LAST)

Goals:
- Keep worker robust: never crash due to missing optional helpers.
- Start:
  1) WS/monitor thread (fills/orders stream)
  2) Risk loop thread (ladder trailing stop manager)
- Plan A:
  - Use exchange-native STOP-MARKET (reduce-only) and cancel/replace to trail
  - Trigger price type = LAST (per your request)

Notes:
- This worker intentionally does NOT import pending-order reconciliation helpers
  (list_pending_orders, etc.) because your current pnl_store.py does not provide them.
- If you later upgrade pnl_store.py and want reconciliation back, we can add it safely
  behind a try/except (optional).
"""


# ----------------------------
# App module import
# ----------------------------
def _import_app_module():
    """
    Your repo may name the web app file differently.
    Prefer env WORKER_APP_MODULE, otherwise try common names.
    """
    candidates = [
        os.getenv("WORKER_APP_MODULE", "").strip(),
        "app_any_amount_v2",
        "app_any_amount",
        "app",
        "main",
        "server",
    ]

    last_err = None
    for name in candidates:
        if not name:
            continue
        try:
            mod = importlib.import_module(name)
            print(f"[worker] using app module: {name}")
            return mod
        except Exception as e:
            last_err = e
            continue

    raise ModuleNotFoundError(
        f"[worker] cannot import any app module. candidates={candidates}. "
        f"Set WORKER_APP_MODULE to your correct module name. last_error={last_err}"
    )


def _call_if_exists(app_module, func_name: str) -> bool:
    fn = getattr(app_module, func_name, None)
    if callable(fn):
        fn()
        return True
    return False


# ----------------------------
# Environment defaults
# ----------------------------
def _apply_env_defaults():
    # Core switches
    os.environ.setdefault("ENABLE_WS", "1")
    os.environ.setdefault("ENABLE_REST_POLL", "1")
    os.environ.setdefault("ENABLE_RISK_LOOP", "1")

    # Plan A: exchange-native stop management on
    os.environ.setdefault("ENABLE_EXCHANGE_STOP", "1")

    # Use LAST as trigger reference
    os.environ.setdefault("RISK_PRICE_SOURCE", "LAST")
    os.environ.setdefault("STOP_TRIGGER_PRICE_TYPE", "LAST")

    # Avoid too frequent cancel/replace
    os.environ.setdefault("STOP_REPLACE_MIN_INTERVAL_SEC", "1.0")

    # Optional: debug
    os.environ.setdefault("WORKER_DEBUG", "0")


# ----------------------------
# Thread starters
# ----------------------------
def _start_monitor(app_module) -> bool:
    # Try common names used in your codebase history
    for n in ("_ensure_monitor_thread", "ensure_monitor_thread", "start_monitor_thread"):
        if _call_if_exists(app_module, n):
            print(f"[worker] started monitor via {n}()")
            return True
    print("[worker] WARNING: no monitor starter found in app module")
    return False


def _start_risk_loop(app_module) -> bool:
    for n in ("_ensure_risk_thread", "ensure_risk_thread", "start_risk_thread"):
        if _call_if_exists(app_module, n):
            print(f"[worker] started risk loop via {n}()")
            return True
    print("[worker] WARNING: no risk-loop starter found in app module")
    return False


# ----------------------------
# Main
# ----------------------------
def main():
    # Init local DB
    init_db()

    # Apply default env (Plan A)
    _apply_env_defaults()

    debug = os.getenv("WORKER_DEBUG", "0") == "1"
    if debug:
        print("[worker] DEBUG env snapshot:")
        for k in (
            "ENABLE_WS",
            "ENABLE_REST_POLL",
            "ENABLE_RISK_LOOP",
            "ENABLE_EXCHANGE_STOP",
            "RISK_PRICE_SOURCE",
            "STOP_TRIGGER_PRICE_TYPE",
            "STOP_REPLACE_MIN_INTERVAL_SEC",
            "WORKER_APP_MODULE",
        ):
            print(f"  - {k}={os.getenv(k)}")

    # Import your app module
    app_module = _import_app_module()

    # Start threads
    ok1 = _start_monitor(app_module)
    ok2 = _start_risk_loop(app_module)

    if ok1 or ok2:
        print("[worker] OK: worker running (WS + risk loop).")
    else:
        print("[worker] ERROR: nothing started. Check app module entrypoints.")

    # Keep alive
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
