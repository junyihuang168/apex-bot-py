import os
import time
import sqlite3
from decimal import Decimal
from typing import Dict, Tuple, Any, Optional, List

DB_PATH = os.getenv("PNL_DB_PATH", "pnl.sqlite3")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    c = _conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        entry_side TEXT NOT NULL,   -- BUY / SELL
        direction TEXT NOT NULL,    -- LONG / SHORT
        qty TEXT NOT NULL,
        price TEXT NOT NULL,
        ts INTEGER NOT NULL,
        reason TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        entry_side TEXT NOT NULL,
        exit_side TEXT NOT NULL,
        qty TEXT NOT NULL,
        entry_price TEXT NOT NULL,
        exit_price TEXT NOT NULL,
        realized TEXT NOT NULL,
        ts INTEGER NOT NULL,
        reason TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS locks (
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        lock_level_pct TEXT NOT NULL,
        updated_ts INTEGER NOT NULL,
        PRIMARY KEY (bot_id, symbol, direction)
    );
    """)

    c.commit()
    c.close()


def _now() -> int:
    return int(time.time())


def record_entry(bot_id: str, symbol: str, side: str, qty: Decimal, price: Decimal, reason: str = "") -> None:
    init_db()
    direction = "LONG" if side.upper() == "BUY" else "SHORT"

    c = _conn()
    cur = c.cursor()
    cur.execute(
        "INSERT INTO lots (bot_id, symbol, entry_side, direction, qty, price, ts, reason) VALUES (?,?,?,?,?,?,?,?)",
        (bot_id, symbol, side.upper(), direction, str(qty), str(price), _now(), reason),
    )
    c.commit()
    c.close()


def _fetch_open_lots(bot_id: str, symbol: str, entry_side: str) -> List[sqlite3.Row]:
    c = _conn()
    cur = c.cursor()
    cur.execute(
        "SELECT * FROM lots WHERE bot_id=? AND symbol=? AND entry_side=? ORDER BY id ASC",
        (bot_id, symbol, entry_side.upper()),
    )
    rows = cur.fetchall()
    c.close()
    return rows


def record_exit_fifo(
    bot_id: str,
    symbol: str,
    entry_side: str,
    exit_qty: Decimal,
    exit_price: Decimal,
    reason: str = "",
) -> None:
    """
    FIFO 平仓：只扣该 bot 自己的 lots
    """
    init_db()
    qty_left = Decimal(str(exit_qty))
    entry_side_u = entry_side.upper()
    exit_side = "SELL" if entry_side_u == "BUY" else "BUY"

    lots = _fetch_open_lots(bot_id, symbol, entry_side_u)
    if not lots:
        return

    c = _conn()
    cur = c.cursor()

    for lot in lots:
        if qty_left <= 0:
            break

        lot_id = int(lot["id"])
        lot_qty = Decimal(str(lot["qty"]))
        lot_price = Decimal(str(lot["price"]))

        take = lot_qty if lot_qty <= qty_left else qty_left

        # realized
        if entry_side_u == "BUY":
            realized = (exit_price - lot_price) * take
        else:
            realized = (lot_price - exit_price) * take

        cur.execute(
            "INSERT INTO trades (bot_id, symbol, entry_side, exit_side, qty, entry_price, exit_price, realized, ts, reason) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                bot_id, symbol, entry_side_u, exit_side, str(take),
                str(lot_price), str(exit_price), str(realized),
                _now(), reason
            ),
        )

        # 扣减 lot
        remain = lot_qty - take
        if remain <= 0:
            cur.execute("DELETE FROM lots WHERE id=?", (lot_id,))
        else:
            cur.execute("UPDATE lots SET qty=? WHERE id=?", (str(remain), lot_id))

        qty_left -= take

    c.commit()
    c.close()


def list_bots_with_activity() -> List[str]:
    init_db()
    c = _conn()
    cur = c.cursor()
    cur.execute("""
        SELECT bot_id FROM (
            SELECT bot_id FROM lots
            UNION
            SELECT bot_id FROM trades
        ) GROUP BY bot_id
        ORDER BY bot_id ASC
    """)
    rows = [r[0] for r in cur.fetchall()]
    c.close()
    return rows


def get_bot_open_positions(bot_id: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    返回：{(symbol, direction): {qty, weighted_entry}}
    """
    init_db()
    c = _conn()
    cur = c.cursor()
    cur.execute("""
        SELECT symbol, direction, SUM(CAST(qty AS REAL)) AS q,
               SUM(CAST(qty AS REAL) * CAST(price AS REAL)) AS notional
        FROM lots
        WHERE bot_id=?
        GROUP BY symbol, direction
    """, (bot_id,))
    rows = cur.fetchall()
    c.close()

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        symbol = r["symbol"]
        direction = r["direction"]
        q = Decimal(str(r["q"] or 0))
        notional = Decimal(str(r["notional"] or 0))
        if q > 0:
            wentry = notional / q
        else:
            wentry = Decimal("0")
        out[(symbol, direction)] = {"qty": q, "weighted_entry": wentry}
    return out


def get_bot_summary(bot_id: str) -> Dict[str, Any]:
    init_db()
    c = _conn()
    cur = c.cursor()

    # total realized
    cur.execute("SELECT SUM(CAST(realized AS REAL)) FROM trades WHERE bot_id=?", (bot_id,))
    total = cur.fetchone()[0] or 0

    # 24h realized
    t24 = _now() - 24 * 3600
    cur.execute("SELECT SUM(CAST(realized AS REAL)) FROM trades WHERE bot_id=? AND ts>=?", (bot_id, t24))
    day = cur.fetchone()[0] or 0

    # 7d realized
    t7 = _now() - 7 * 24 * 3600
    cur.execute("SELECT SUM(CAST(realized AS REAL)) FROM trades WHERE bot_id=? AND ts>=?", (bot_id, t7))
    week = cur.fetchone()[0] or 0

    # trades count
    cur.execute("SELECT COUNT(*) FROM trades WHERE bot_id=?", (bot_id,))
    cnt = cur.fetchone()[0] or 0

    c.close()

    return {
        "bot_id": bot_id,
        "realized_total": str(Decimal(str(total))),
        "realized_day": str(Decimal(str(day))),
        "realized_week": str(Decimal(str(week))),
        "trades_count": int(cnt),
    }


def get_lock_level_pct(bot_id: str, symbol: str, direction: str) -> Decimal:
    init_db()
    c = _conn()
    cur = c.cursor()
    cur.execute(
        "SELECT lock_level_pct FROM locks WHERE bot_id=? AND symbol=? AND direction=?",
        (bot_id, symbol, direction.upper()),
    )
    row = cur.fetchone()
    c.close()
    if not row:
        return Decimal("0")
    try:
        return Decimal(str(row[0]))
    except Exception:
        return Decimal("0")


def set_lock_level_pct(bot_id: str, symbol: str, direction: str, lock_level_pct: Decimal) -> None:
    init_db()
    c = _conn()
    cur = c.cursor()
    cur.execute(
        "INSERT INTO locks (bot_id, symbol, direction, lock_level_pct, updated_ts) VALUES (?,?,?,?,?) "
        "ON CONFLICT(bot_id, symbol, direction) DO UPDATE SET lock_level_pct=excluded.lock_level_pct, updated_ts=excluded.updated_ts",
        (bot_id, symbol, direction.upper(), str(lock_level_pct), _now()),
    )
    c.commit()
    c.close()


def clear_lock_level_pct(bot_id: str, symbol: str, direction: str) -> None:
    init_db()
    c = _conn()
    cur = c.cursor()
    cur.execute(
        "DELETE FROM locks WHERE bot_id=? AND symbol=? AND direction=?",
        (bot_id, symbol, direction.upper()),
    )
    c.commit()
    c.close()
