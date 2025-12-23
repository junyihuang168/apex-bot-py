import time

from pnl_store import init_db

# Import the Flask app module so we can reuse its monitor threads in the worker.
import app as app_module


def main():
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
