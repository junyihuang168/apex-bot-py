import json
import os
import threading
from typing import Any, Dict, Optional

STORE_PATH = os.getenv("PNL_STORE_PATH", "/tmp/pnl_store.json")
_LOCK = threading.Lock()

# data schema:
# {
#   "bots": {
#     "BOT_1": {
#        "ZEC-USDT": {
#           "direction": "LONG|SHORT",
#           "qty": "0.03",
#           "entry_price": "457.12",
#           "lock_level_pct": "0.00",
#           "is_closing": false
#        }
#     }
#   }
# }


def _load() -> Dict[str, Any]:
    if not os.path.exists(STORE_PATH):
        return {"bots": {}}
    try:
        with open(STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"bots": {}}


def _save(data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STORE_PATH), exist_ok=True)
    with open(STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_position(bot_id: str, symbol: str) -> Optional[Dict[str, Any]]:
    with _LOCK:
        data = _load()
        return data.get("bots", {}).get(bot_id, {}).get(symbol)


def set_position(bot_id: str, symbol: str, pos: Dict[str, Any]) -> None:
    with _LOCK:
        data = _load()
        data.setdefault("bots", {}).setdefault(bot_id, {})[symbol] = pos
        _save(data)


def clear_position(bot_id: str, symbol: str) -> None:
    with _LOCK:
        data = _load()
        bots = data.setdefault("bots", {})
        if bot_id in bots and symbol in bots[bot_id]:
            del bots[bot_id][symbol]
        _save(data)


def list_open_positions() -> Dict[str, Dict[str, Dict[str, Any]]]:
    with _LOCK:
        data = _load()
        return data.get("bots", {})


def get_lock_level_pct(bot_id: str, symbol: str) -> float:
    pos = get_position(bot_id, symbol)
    if not pos:
        return 0.0
    try:
        return float(pos.get("lock_level_pct", "0"))
    except Exception:
        return 0.0


def set_lock_level_pct(bot_id: str, symbol: str, lock_level_pct: float) -> None:
    pos = get_position(bot_id, symbol) or {}
    pos["lock_level_pct"] = f"{lock_level_pct:.4f}"
    set_position(bot_id, symbol, pos)


def mark_closing(bot_id: str, symbol: str, is_closing: bool) -> None:
    pos = get_position(bot_id, symbol) or {}
    pos["is_closing"] = bool(is_closing)
    set_position(bot_id, symbol, pos)
