import os
import sqlite3
import time
from decimal import Decimal
from typing import Dict, Tuple, Any, List, Callable, Optional, Set

# ✅ 修改：使用绝对路径，避免不同运行环境找不到文件
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, os.getenv("PNL_DB_PATH", "pnl.sqlite3"))

print(f"[PNL] Database path set to: {DB_PATH}")

SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))
SQLITE_WRITE_RETRY = int(os.getenv("SQLITE_WRITE_RETRY", "5"))
SQLITE_WRITE_RETRY_SLEEP = float(os.getenv("SQLITE_WRITE_RETRY_SLEEP", "0.15"))


def _now() -> int:
    return int(time.time())


def _d(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=max(1, SQLITE_BUSY_TIMEOUT_MS // 1000))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
    except Exception:
        pass
    return conn


def _with_retry(fn: Callable, *args, **kwargs):
    last = None
    for _ in range(max(1, SQLITE_WRITE_RETRY)):
        try:
            return fn(*args, **kwargs)
        except sqlite3.OperationalError as e:
            last = e
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                time.sleep(SQLITE_WRITE_RETRY_SLEEP)
                continue
            raise
    if last:
        raise last


# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------

def init_db():
    conn = _connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                qty TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                order_id TEXT,
                client_order_id TEXT,
                meta TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                qty TEXT NOT NULL,
                exit_price TEXT NOT NULL,
                pnl_usdt TEXT NOT NULL,
                entry_id INTEGER,
                order_id TEXT,
                client_order_id TEXT,
                reason TEXT,
                meta TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS locks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                lock_level_pct TEXT NOT NULL,
                updated_ts INTEGER NOT NULL,
                UNIQUE(bot_id, symbol, direction)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_dedupe (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT NOT NULL UNIQUE,
                ts INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS protective_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                sl_order_id TEXT,
                sl_client_id TEXT,
                tp_order_id TEXT,
                tp_client_id TEXT,
                created_ts INTEGER NOT NULL,
                UNIQUE(bot_id, symbol, direction)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS protective_owner_map (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL UNIQUE,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                created_ts INTEGER NOT NULL
            );
            """
        )
        conn.commit()
        print("[PNL] Database initialized/migrated successfully.")
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Signal dedupe
# -----------------------------------------------------------------------------

def is_signal_processed(signal_id: str) -> bool:
    if not signal_id:
        return False
    conn = _connect()
    try:
        cur = conn.execute("SELECT 1 FROM signal_dedupe WHERE signal_id=? LIMIT 1", (signal_id,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_signal_processed(signal_id: str) -> None:
    if not signal_id:
        return
    conn = _connect()
    try:
        _with_retry(conn.execute, "INSERT OR IGNORE INTO signal_dedupe(signal_id, ts) VALUES(?, ?)", (signal_id, _now()))
        conn.commit()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Entries / Exits
# -----------------------------------------------------------------------------

def record_entry(
    bot_id: str,
    symbol: str,
    direction: str,
    qty: Decimal,
    entry_price: Decimal,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    meta: Optional[str] = None,
) -> int:
    conn = _connect()
    try:
        cur = _with_retry(
            conn.execute,
            """
            INSERT INTO entries(ts, bot_id, symbol, direction, qty, entry_price, order_id, client_order_id, meta)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _now(),
                str(bot_id),
                str(symbol).upper(),
                str(direction).upper(),
                str(_d(qty)),
                str(_d(entry_price)),
                str(order_id) if order_id else None,
                str(client_order_id) if client_order_id else None,
                meta,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _get_open_entries_fifo(bot_id: str, symbol: str, direction: str) -> List[sqlite3.Row]:
    conn = _connect()
    try:
        cur = conn.execute(
            """
            SELECT e.*
            FROM entries e
            WHERE e.bot_id=? AND e.symbol=? AND e.direction=?
              AND e.id NOT IN (SELECT entry_id FROM exits WHERE entry_id IS NOT NULL)
            ORDER BY e.id ASC
            """,
            (str(bot_id), str(symbol).upper(), str(direction).upper()),
        )
        rows = cur.fetchall()
        return rows or []
    finally:
        conn.close()


def record_exit_fifo(
    bot_id: str,
    symbol: str,
    direction: str,
    qty: Decimal,
    exit_price: Decimal,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    reason: str = "",
    meta: Optional[str] = None,
) -> Dict[str, Any]:
    """FIFO exits against open entries. Returns summary."""
    qty_left = _d(qty)
    if qty_left <= 0:
        return {"closed_qty": "0", "pnl_usdt": "0"}

    exit_p = _d(exit_price)
    if exit_p <= 0:
        return {"closed_qty": "0", "pnl_usdt": "0"}

    closed_qty = Decimal("0")
    total_pnl = Decimal("0")
    used_entry_ids: List[int] = []

    open_entries = _get_open_entries_fifo(bot_id, symbol, direction)

    conn = _connect()
    try:
        for e in open_entries:
            if qty_left <= 0:
                break
            entry_id = int(e["id"])
            entry_qty = _d(e["qty"])
            entry_price = _d(e["entry_price"])

            take = min(entry_qty, qty_left)
            qty_left -= take

            if str(direction).upper() == "LONG":
                pnl = (exit_p - entry_price) * take
            else:
                pnl = (entry_price - exit_p) * take

            closed_qty += take
            total_pnl += pnl
            used_entry_ids.append(entry_id)

            _with_retry(
                conn.execute,
                """
                INSERT INTO exits(ts, bot_id, symbol, direction, qty, exit_price, pnl_usdt,
                                 entry_id, order_id, client_order_id, reason, meta)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _now(),
                    str(bot_id),
                    str(symbol).upper(),
                    str(direction).upper(),
                    str(take),
                    str(exit_p),
                    str(pnl),
                    entry_id,
                    str(order_id) if order_id else None,
                    str(client_order_id) if client_order_id else None,
                    reason,
                    meta,
                ),
            )

        # If we closed more than existing entries (shouldn't), still record remainder without entry_id
        if qty_left > 0:
            _with_retry(
                conn.execute,
                """
                INSERT INTO exits(ts, bot_id, symbol, direction, qty, exit_price, pnl_usdt,
                                 entry_id, order_id, client_order_id, reason, meta)
                VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
                """,
                (
                    _now(),
                    str(bot_id),
                    str(symbol).upper(),
                    str(direction).upper(),
                    str(qty_left),
                    str(exit_p),
                    "0",
                    str(order_id) if order_id else None,
                    str(client_order_id) if client_order_id else None,
                    reason,
                    meta,
                ),
            )
            closed_qty += qty_left
            qty_left = Decimal("0")

        conn.commit()
    finally:
        conn.close()

    return {"closed_qty": str(closed_qty), "pnl_usdt": str(total_pnl), "used_entry_ids": used_entry_ids}


# -----------------------------------------------------------------------------
# Locks (ladder)
# -----------------------------------------------------------------------------

def get_lock_level_pct(bot_id: str, symbol: str, direction: str) -> Optional[Decimal]:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT lock_level_pct FROM locks WHERE bot_id=? AND symbol=? AND direction=? LIMIT 1",
            (str(bot_id), str(symbol).upper(), str(direction).upper()),
        )
        row = cur.fetchone()
        if not row:
            return None
        return _d(row["lock_level_pct"])
    finally:
        conn.close()


def set_lock_level_pct(bot_id: str, symbol: str, direction: str, lock_level_pct: Decimal) -> None:
    conn = _connect()
    try:
        _with_retry(
            conn.execute,
            """
            INSERT INTO locks(bot_id, symbol, direction, lock_level_pct, updated_ts)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(bot_id, symbol, direction)
            DO UPDATE SET lock_level_pct=excluded.lock_level_pct, updated_ts=excluded.updated_ts
            """,
            (str(bot_id), str(symbol).upper(), str(direction).upper(), str(_d(lock_level_pct)), _now()),
        )
        conn.commit()
    finally:
        conn.close()


def clear_lock_level_pct(bot_id: str, symbol: str, direction: str) -> None:
    conn = _connect()
    try:
        _with_retry(
            conn.execute,
            "DELETE FROM locks WHERE bot_id=? AND symbol=? AND direction=?",
            (str(bot_id), str(symbol).upper(), str(direction).upper()),
        )
        conn.commit()
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Dash / Summary helpers
# -----------------------------------------------------------------------------

def list_bots_with_activity() -> List[str]:
    conn = _connect()
    try:
        cur = conn.execute(
            """
            SELECT DISTINCT bot_id FROM (
                SELECT bot_id FROM entries
                UNION
                SELECT bot_id FROM exits
            )
            ORDER BY bot_id
            """
        )
        return [r["bot_id"] for r in cur.fetchall()]
    finally:
        conn.close()


def get_bot_open_positions(bot_id: str) -> List[Dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.execute(
            """
            SELECT symbol, direction, SUM(CAST(qty AS REAL)) as qty, AVG(CAST(entry_price AS REAL)) as avg_entry
            FROM entries
            WHERE bot_id=?
              AND id NOT IN (SELECT entry_id FROM exits WHERE entry_id IS NOT NULL)
            GROUP BY symbol, direction
            """,
            (str(bot_id),),
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append(
                {
                    "symbol": r["symbol"],
                    "direction": r["direction"],
                    "qty": str(r["qty"] or 0),
                    "avg_entry": str(r["avg_entry"] or 0),
                }
            )
        return out
    finally:
        conn.close()


def get_bot_summary(bot_id: str) -> Dict[str, Any]:
    conn = _connect()
    try:
        cur1 = conn.execute("SELECT COUNT(*) as c FROM entries WHERE bot_id=?", (str(bot_id),))
        cur2 = conn.execute("SELECT COUNT(*) as c FROM exits WHERE bot_id=?", (str(bot_id),))
        cur3 = conn.execute("SELECT SUM(CAST(pnl_usdt AS REAL)) as pnl FROM exits WHERE bot_id=?", (str(bot_id),))
        return {
            "bot_id": str(bot_id),
            "entries": int(cur1.fetchone()["c"]),
            "exits": int(cur2.fetchone()["c"]),
            "pnl_usdt": str(cur3.fetchone()["pnl"] or 0),
            "open_positions": get_bot_open_positions(bot_id),
        }
    finally:
        conn.close()


def get_symbol_open_directions(bot_id: str, symbol: str) -> Set[str]:
    conn = _connect()
    try:
        cur = conn.execute(
            """
            SELECT direction, SUM(CAST(qty AS REAL)) as qty
            FROM entries
            WHERE bot_id=? AND symbol=?
              AND id NOT IN (SELECT entry_id FROM exits WHERE entry_id IS NOT NULL)
            GROUP BY direction
            """,
            (str(bot_id), str(symbol).upper()),
        )
        out: Set[str] = set()
        for r in cur.fetchall():
            if (r["qty"] or 0) != 0:
                out.add(str(r["direction"]).upper())
        return out
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Protective orders state (optional)
# -----------------------------------------------------------------------------

def set_protective_orders(bot_id: str, symbol: str, direction: str, sl_order_id: str = "", sl_client_id: str = "", tp_order_id: str = "", tp_client_id: str = "") -> None:
    conn = _connect()
    try:
        _with_retry(
            conn.execute,
            """
            INSERT INTO protective_orders(bot_id, symbol, direction, sl_order_id, sl_client_id, tp_order_id, tp_client_id, created_ts)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bot_id, symbol, direction)
            DO UPDATE SET sl_order_id=excluded.sl_order_id,
                          sl_client_id=excluded.sl_client_id,
                          tp_order_id=excluded.tp_order_id,
                          tp_client_id=excluded.tp_client_id,
                          created_ts=excluded.created_ts
            """,
            (str(bot_id), str(symbol).upper(), str(direction).upper(), sl_order_id, sl_client_id, tp_order_id, tp_client_id, _now()),
        )
        conn.commit()
    finally:
        conn.close()


def get_protective_orders(bot_id: str, symbol: str, direction: str) -> Dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT * FROM protective_orders WHERE bot_id=? AND symbol=? AND direction=? LIMIT 1",
            (str(bot_id), str(symbol).upper(), str(direction).upper()),
        )
        row = cur.fetchone()
        if not row:
            return {}
        return dict(row)
    finally:
        conn.close()


def clear_protective_orders(bot_id: str, symbol: str, direction: str) -> None:
    conn = _connect()
    try:
        _with_retry(
            conn.execute,
            "DELETE FROM protective_orders WHERE bot_id=? AND symbol=? AND direction=?",
            (str(bot_id), str(symbol).upper(), str(direction).upper()),
        )
        conn.commit()
    finally:
        conn.close()


def set_protective_owner_by_order_id(order_id: str, bot_id: str, symbol: str, direction: str) -> None:
    if not order_id:
        return
    conn = _connect()
    try:
        _with_retry(
            conn.execute,
            """
            INSERT OR REPLACE INTO protective_owner_map(order_id, bot_id, symbol, direction, created_ts)
            VALUES(?, ?, ?, ?, ?)
            """,
            (str(order_id), str(bot_id), str(symbol).upper(), str(direction).upper(), _now()),
        )
        conn.commit()
    finally:
        conn.close()


def find_protective_owner_by_order_id(order_id: str) -> Optional[Dict[str, str]]:
    if not order_id:
        return None
    conn = _connect()
    try:
        cur = conn.execute("SELECT bot_id, symbol, direction FROM protective_owner_map WHERE order_id=? LIMIT 1", (str(order_id),))
        row = cur.fetchone()
        if not row:
            return None
        return {"bot_id": row["bot_id"], "symbol": row["symbol"], "direction": row["direction"]}
    finally:
        conn.close()


# ✅ 兼容别名：你 app.py 之前 import 的名字
def get_protective_owner_by_order_id(order_id: str) -> Optional[Dict[str, str]]:
    return find_protective_owner_by_order_id(order_id)
