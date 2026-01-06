import os
import time

from pnl_store import init_db

# Import the Flask app module so we can reuse its monitor threads in the worker.
import app as app_module


def main():
    # Fail-safe: if supervisor didn't inject env, default worker to ENABLE_WS=1.
    os.environ.setdefault("ENABLE_WS", "1")
    os.environ.setdefault("ENABLE_REST_POLL", "1")
    os.environ.setdefault("ENABLE_EXCHANGE_PROTECTIVE", "0")  # bot-side stops only

    # Public WS (L1) defaults (used by ladder risk checks)
    os.environ.setdefault("L1_FALLBACK_TO_MARK", "1")
    os.environ.setdefault("L1_STALE_SEC", os.getenv("L1_STALE_SEC", "2.0"))
    os.environ.setdefault("PUBLIC_WS_PING_SEC", os.getenv("PUBLIC_WS_PING_SEC", "15"))

    init_db()

    try:
        # Starts: private WS, order-delta consumer loop, and (optionally) REST order poller.
        app_module._ensure_monitor_thread()  # type: ignore[attr-defined]
        print("[worker] monitor thread started")
    except Exception as e:
        print(f"[worker] monitor thread start failed: {e}")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
