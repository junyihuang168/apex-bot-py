import json
import os
import threading
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

_STORE_LOCK = threading.Lock()
_STORE_PATH = os.getenv("PNL_STORE_PATH", "/tmp/pnl_store.json")


def _d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def _load() -> Dict[str, Any]:
    if not os.path.exists(_STORE_PATH):
        return {"positions": {}}
    try:
        with open(_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {"positions": {}}
    except Exception:
        return {"positions": {}}


def _save(data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(_STORE_PATH), exist_ok=True)
    with open(_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _key(bot_id: str, symbol: str) -> str:
    return f"{bot_id}::{symbol}"


def get_position(bot_id: str, symbol: str) -> Optional[Dict[str, Any]]:
    with _STORE_LOCK:
        data = _load()
        return data.get("positions", {}).get(_key(bot_id, symbol))


def set_position(
    bot_id: str,
    symbol: str,
    direction: str,           # "LONG" or "SHORT"
    qty: str,
    entry_price: str,
    sl_price: str = "0",
    tp_price: str = "0",
) -> None:
    with _STORE_LOCK:
        data = _load()
        data.setdefault("positions", {})
        data["positions"][_key(bot_id, symbol)] = {
            "bot_id": bot_id,
            "symbol": symbol,
            "direction": direction.upper(),
            "qty": str(qty),
            "entry_price": str(entry_price),
            "sl_price": str(sl_price),
            "tp_price": str(tp_price),
            # ladder lock
            "lock_level_pct": "0",     # e.g. 0.10 means lock +0.10%
            "max_profit_pct": "0",
            "updated_at": int(__import__("time").time()),
        }
        _save(data)


def update_position(bot_id: str, symbol: str, patch: Dict[str, Any]) -> None:
    with _STORE_LOCK:
        data = _load()
        k = _key(bot_id, symbol)
        pos = data.get("positions", {}).get(k)
        if not pos:
            return
        pos.update(patch)
        pos["updated_at"] = int(__import__("time").time())
        data["positions"][k] = pos
        _save(data)


def clear_position(bot_id: str, symbol: str) -> None:
    with _STORE_LOCK:
        data = _load()
        data.setdefault("positions", {})
        data["positions"].pop(_key(bot_id, symbol), None)
        _save(data)


def list_positions() -> Dict[str, Any]:
    with _STORE_LOCK:
        return _load()


# ---- 你日志里报过 ImportError: cannot import name 'get_lock_level_pct' ----
# 这里补齐，app.py 直接 import 就不会再炸。
def get_lock_level_pct(bot_id: str, symbol: str) -> Decimal:
    pos = get_position(bot_id, symbol) or {}
    return _d(pos.get("lock_level_pct", "0"))
