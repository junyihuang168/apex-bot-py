import os
import sqlite3
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.getenv("PNL_DB_PATH", "pnl.sqlite")


def _d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    con = _conn()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        entry_side TEXT NOT NULL,   -- BUY (LONG entry) or SELL (SHORT entry)
        qty TEXT NOT NULL,
        entry_price TEXT NOT NULL,
        remaining_qty TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS exits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        entry_side TEXT NOT NULL,
        exit_side TEXT NOT NULL,
        qty TEXT NOT NULL,
        entry_price TEXT NOT NULL,
        exit_price TEXT NOT NULL,
        realized_pnl TEXT NOT NULL,
        reason TEXT,
        created_at INTEGER NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS locks (
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,   -- LONG / SHORT
        lock_level_pct TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (bot_id, symbol, direction)
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_bot_symbol ON lots(bot_id, symbol);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exits_bot_symbol ON exits(bot_id, symbol);")
    con.commit()
    con.close()


def record_entry(bot_id: str, symbol: str, entry_side: str, qty: Decimal, entry_price: Decimal) -> None:
    init_db()
    con = _conn()
    cur = con.cursor()
    ts = int(time.time() * 1000)
    cur.execute(
        "INSERT INTO lots(bot_id,symbol,entry_side,qty,entry_price,remaining_qty,created_at) VALUES(?,?,?,?,?,?,?)",
        (bot_id, symbol, entry_side.upper(), str(qty), str(entry_price), str(qty), ts),
    )
    con.commit()
    con.close()


def get_open_position(bot_id: str, symbol: str, entry_side: str) -> Tuple[Decimal, Decimal]:
    """
    Return (open_qty, avg_entry_price) for a given bot/symbol/entry_side.
    """
    init_db()
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT remaining_qty, entry_price FROM lots WHERE bot_id=? AND symbol=? AND entry_side=?",
        (bot_id, symbol, entry_side.upper()),
    )
    rows = cur.fetchall()
    con.close()

    qty_sum = Decimal("0")
    value_sum = Decimal("0")
    for r in rows:
        q = _d(r["remaining_qty"])
        p = _d(r["entry_price"])
        if q > 0 and p > 0:
            qty_sum += q
            value_sum += q * p

    if qty_sum <= 0:
        return Decimal("0"), Decimal("0")
    return qty_sum, (value_sum / qty_sum)


def get_all_open_positions(bot_id: str) -> List[Dict[str, Any]]:
    init_db()
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT symbol, entry_side, remaining_qty, entry_price FROM lots WHERE bot_id=?",
        (bot_id,),
    )
    rows = cur.fetchall()
    con.close()

    # aggregate
    agg: Dict[Tuple[str, str], Tuple[Decimal, Decimal]] = {}
    for r in rows:
        sym = r["symbol"]
        side = r["entry_side"]
        q = _d(r["remaining_qty"])
        p = _d(r["entry_price"])
        if q <= 0 or p <= 0:
            continue
        key = (sym, side)
        if key not in agg:
            agg[key] = (Decimal("0"), Decimal("0"))
        qsum, vsum = agg[key]
        agg[key] = (qsum + q, vsum + q * p)

    out = []
    for (sym, side), (qsum, vsum) in agg.items():
        out.append({
            "symbol": sym,
            "entry_side": side,
            "qty": str(qsum),
            "avg_entry_price": str((vsum / qsum) if qsum > 0 else Decimal("0")),
        })
    return out


def record_exit_fifo(
    bot_id: str,
    symbol: str,
    entry_side: str,
    exit_side: str,
    exit_qty: Decimal,
    exit_price: Decimal,
    reason: str = "",
) -> Dict[str, Any]:
    """
    Consume lots FIFO for (bot_id, symbol, entry_side).
    Realized PnL:
      - If entry_side=BUY (LONG): (exit - entry) * qty
      - If entry_side=SELL (SHORT): (entry - exit) * qty
    """
    init_db()
    con = _conn()
    cur = con.cursor()

    entry_side = entry_side.upper()
    exit_side = exit_side.upper()

    cur.execute(
        "SELECT id, remaining_qty, entry_price FROM lots WHERE bot_id=? AND symbol=? AND entry_side=? AND CAST(remaining_qty as REAL) > 0 ORDER BY created_at ASC, id ASC",
        (bot_id, symbol, entry_side),
    )
    lots = cur.fetchall()

    remaining = exit_qty
    realized_total = Decimal("0")
    consumed_total = Decimal("0")
    ts = int(time.time() * 1000)

    for lot in lots:
        if remaining <= 0:
            break

        lot_id = lot["id"]
        lot_rem = _d(lot["remaining_qty"])
        lot_entry = _d(lot["entry_price"])
        if lot_rem <= 0 or lot_entry <= 0:
            continue

        take = lot_rem if lot_rem <= remaining else remaining

        if entry_side == "BUY":  # LONG
            realized = (exit_price - lot_entry) * take
        else:  # SHORT entry_side=SELL
            realized = (lot_entry - exit_price) * take

        # update lot remaining
        new_rem = lot_rem - take
        cur.execute("UPDATE lots SET remaining_qty=? WHERE id=?", (str(new_rem), lot_id))

        # record exit slice
        cur.execute(
            "INSERT INTO exits(bot_id,symbol,entry_side,exit_side,qty,entry_price,exit_price,realized_pnl,reason,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (bot_id, symbol, entry_side, exit_side, str(take), str(lot_entry), str(exit_price), str(realized), reason, ts),
        )

        realized_total += realized
        consumed_total += take
        remaining -= take

    con.commit()
    con.close()

    return {
        "ok": True,
        "consumed_qty": str(consumed_total),
        "requested_qty": str(exit_qty),
        "realized_pnl": str(realized_total),
        "unfilled_qty": str(remaining if remaining > 0 else Decimal("0")),
    }


# -----------------------
# Lock level (ladder)
# -----------------------
def get_lock_level_pct(bot_id: str, symbol: str, direction: str) -> Decimal:
    init_db()
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT lock_level_pct FROM locks WHERE bot_id=? AND symbol=? AND direction=?",
        (bot_id, symbol, direction.upper()),
    )
    row = cur.fetchone()
    con.close()
    if not row:
        return Decimal("0")
    return _d(row["lock_level_pct"])


def set_lock_level_pct(bot_id: str, symbol: str, direction: str, lock_level_pct: Decimal) -> None:
    init_db()
    con = _conn()
    cur = con.cursor()
    ts = int(time.time() * 1000)
    cur.execute(
        "INSERT INTO locks(bot_id,symbol,direction,lock_level_pct,updated_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(bot_id,symbol,direction) DO UPDATE SET lock_level_pct=excluded.lock_level_pct, updated_at=excluded.updated_at",
        (bot_id, symbol, direction.upper(), str(lock_level_pct), ts),
    )
    con.commit()
    con.close()


def clear_lock_level_pct(bot_id: str, symbol: str, direction: str) -> None:
    init_db()
    con = _conn()
    cur = con.cursor()
    cur.execute("DELETE FROM locks WHERE bot_id=? AND symbol=? AND direction=?", (bot_id, symbol, direction.upper()))
    con.commit()
    con.close()


def list_bots_with_activity() -> List[str]:
    init_db()
    con = _conn()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT bot_id FROM lots UNION SELECT DISTINCT bot_id FROM exits")
    rows = cur.fetchall()
    con.close()
    return sorted([r[0] for r in rows if r and r[0]])


def get_bot_summary(bot_id: str) -> Dict[str, Any]:
    init_db()
    con = _conn()
    cur = con.cursor()

    # realized pnl sum
    cur.execute("SELECT realized_pnl FROM exits WHERE bot_id=?", (bot_id,))
    rows = cur.fetchall()
    realized = sum([_d(r["realized_pnl"]) for r in rows], Decimal("0"))

    open_pos = get_all_open_positions(bot_id)

    con.close()
    return {
        "bot_id": bot_id,
        "realized_pnl": str(realized),
        "open_positions": open_pos,
    }
