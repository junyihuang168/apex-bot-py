# worker.py
import os
import time

# Worker 进程必须启用 WS
os.environ["ENABLE_WS"] = "1"

from app import _ensure_monitor_thread  # 复用你现有逻辑（init_db + ws + fills thread）

if __name__ == "__main__":
    _ensure_monitor_thread()
    print("[WORKER] ws fills worker running (ENABLE_WS=1)")
    while True:
        time.sleep(60)
