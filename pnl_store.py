import os
import sqlite3
import time
from decimal import Decimal
from typing import Dict, Tuple, Any, List, Optional

DB_PATH = os.getenv("PNL_DB_PATH", "pnl.sqlite3")


def _now() -> int:
    return int(time.time())


def _d(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # concurrency hardening
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=3000;")
    except Exception:
        pass

    return conn


def init_db():
    conn = _connect()
    cur = conn.cursor()

    # lots: each entry
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,        -- LONG / SHORT
        entry_side TEXT NOT NULL,       -- BUY / SELL
        qty TEXT NOT NULL,              -- Decimal as text
        entry_price TEXT NOT NULL,      -- Decimal as text (FILL AVG)
        remaining_qty TEXT NOT NULL,    -- Decimal as text
        reason TEXT,
        ts INTEGER NOT NULL
    )
    """)

    # exits: each exit record
    cur.execute("""
    CREATE TABLE IF NOT EXISTS exits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        entry_side TEXT NOT NULL,
        exit_side TEXT NOT NULL,
        exit_qty TEXT NOT NULL,
        entry_price TEXT NOT NULL,
        exit_price TEXT NOT NULL,
        realized_pnl TEXT NOT NULL,
        reason TEXT,
        ts INTEGER NOT NULL
    )
    """)

    # lock levels (your stepwise lock)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lock_levels (
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        lock_level_pct TEXT NOT NULL,
        updated_ts INTEGER NOT NULL,
        PRIMARY KEY (bot_id, symbol, direction)
    )
    """)

    # processed signals for idempotency
    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_signals (
        bot_id TEXT NOT NULL,
        signal_id TEXT NOT NULL,
        ts INTEGER NOT NULL,
        PRIMARY KEY (bot_id, signal_id)
    )
    """)

    # risk state (fill-first baseline)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS risk_states (
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,             -- LONG/SHORT
        entry_fill_price TEXT NOT NULL,      -- Decimal text
        entry_qty TEXT NOT NULL,             -- Decimal text
        peak_profit_pct TEXT NOT NULL,       -- Decimal text
        lock_level_pct TEXT NOT NULL,        -- Decimal text
        sl_pct TEXT NOT NULL,                -- Decimal text (e.g. -0.5)
        status TEXT NOT NULL,                -- ACTIVE / FROZEN / CLOSED
        updated_ts INTEGER NOT NULL,
        PRIMARY KEY (bot_id, symbol, direction)
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_bot_symbol ON lots(bot_id, symbol, direction, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_exits_bot_ts ON exits(bot_id, ts)")
    conn.commit()
    conn.close()


def _side_to_direction(entry_side: str) -> str:
    s = str(entry_side).upper()
    return "LONG" if s == "BUY" else "SHORT"


def record_entry(
    bot_id: str,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    reason: str = "strategy_entry",
):
    direction = _side_to_direction(side)
    q = _d(qty)
    p = _d(price)

    if q <= 0 or p <= 0:
        raise ValueError("record_entry requires qty>0 and price>0 (fill-first)")

    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO lots (bot_id, symbol, direction, entry_side, qty, entry_price, remaining_qty, reason, ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        bot_id, symbol, direction, str(side).upper(),
        str(q), str(p), str(q), reason, _now()
    ))
    conn.commit()
    conn.close()


def record_exit_fifo(
    bot_id: str,
    symbol: str,
    entry_side: str,
    exit_qty: Decimal,
    exit_price: Decimal,
    reason: str = "strategy_exit",
):
    direction = _side_to_direction(entry_side)
    exit_side = "SELL" if direction == "LONG" else "BUY"

    need = _d(exit_qty)
    px_exit = _d(exit_price)

    if need <= 0 or px_exit <= 0:
        raise ValueError("record_exit_fifo requires exit_qty>0 and exit_price>0 (fill-first)")

    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM lots
        WHERE bot_id=? AND symbol=? AND direction=? AND CAST(remaining_qty AS REAL) > 0
        ORDER BY ts ASC, id ASC
    """, (bot_id, symbol, direction))

    rows = cur.fetchall()
    if not rows:
        conn.close()
        return

    ts = _now()
    for r in rows:
        if need <= 0:
            break

        lot_id = r["id"]
        rem = _d(r["remaining_qty"])
        entry_price = _d(r["entry_price"])

        if rem <= 0:
            continue

        take = rem if rem <= need else need

        # realized pnl
        if direction == "LONG":
            pnl = (px_exit - entry_price) * take
        else:
            pnl = (entry_price - px_exit) * take

        new_rem = rem - take

        cur.execute("""
            UPDATE lots SET remaining_qty=?
            WHERE id=?
        """, (str(new_rem), lot_id))

        cur.execute("""
            INSERT INTO exits (
                bot_id, symbol, direction, entry_side, exit_side,
                exit_qty, entry_price, exit_price, realized_pnl, reason, ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bot_id, symbol, direction, str(entry_side).upper(), exit_side,
            str(take), str(entry_price), str(px_exit), str(pnl), reason, ts
        ))

        need -= take

    conn.commit()
    conn.close()


def get_bot_open_positions(bot_id: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT symbol, direction,
               SUM(CAST(remaining_qty AS REAL)) AS qty_sum,
               SUM(CAST(remaining_qty AS REAL) * CAST(entry_price AS REAL)) AS notional_sum
        FROM lots
        WHERE bot_id=? AND CAST(remaining_qty AS REAL) > 0
        GROUP BY symbol, direction
    """, (bot_id,))

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in cur.fetchall():
        symbol = r["symbol"]
        direction = r["direction"]
        qty_sum = _d(r["qty_sum"] or "0")
        notional_sum = _d(r["notional_sum"] or "0")

        weighted = (notional_sum / qty_sum) if qty_sum > 0 else Decimal("0")
        out[(symbol, direction)] = {"qty": qty_sum, "weighted_entry": weighted}

    conn.close()
    return out


def list_bots_with_activity() -> List[str]:
    conn = _connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT bot_id FROM lots
        UNION
        SELECT DISTINCT bot_id FROM exits
    """)
    bots = sorted([r[0] for r in cur.fetchall() if r[0]])
    conn.close()
    return bots


def get_bot_summary(bot_id: str) -> Dict[str, Any]:
    conn = _connect()
    cur = conn.cursor()

    now = _now()
    day_ago = now - 24 * 3600
    week_ago = now - 7 * 24 * 3600

    def _sum_since(ts_from: int) -> Decimal:
        cur.execute("""
            SELECT SUM(CAST(realized_pnl AS REAL)) AS s
            FROM exits
            WHERE bot_id=? AND ts>=?
        """, (bot_id, ts_from))
        v = cur.fetchone()["s"]
        return _d(v or "0")

    cur.execute("""
        SELECT SUM(CAST(realized_pnl AS REAL)) AS s, COUNT(*) AS c
        FROM exits WHERE bot_id=?
    """, (bot_id,))
    row = cur.fetchone()
    total = _d(row["s"] or "0")
    count = int(row["c"] or 0)

    out = {
        "bot_id": bot_id,
        "realized_day": str(_sum_since(day_ago)),
        "realized_week": str(_sum_since(week_ago)),
        "realized_total": str(total),
        "trades_count": count,
    }

    conn.close()
    return out


# ---------------------------
# Idempotency (signal dedup)
# ---------------------------
def is_signal_processed(bot_id: str, signal_id: str) -> bool:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM processed_signals
        WHERE bot_id=? AND signal_id=?
    """, (bot_id, signal_id))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_signal_processed(bot_id: str, signal_id: str):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO processed_signals (bot_id, signal_id, ts)
        VALUES (?, ?, ?)
    """, (bot_id, signal_id, _now()))
    conn.commit()
    conn.close()


# ---------------------------
# Risk state persistence
# ---------------------------
def upsert_risk_state(
    bot_id: str,
    symbol: str,
    direction: str,
    entry_fill_price: Decimal,
    entry_qty: Decimal,
    sl_pct: Decimal,
    peak_profit_pct: Decimal = Decimal("0"),
    lock_level_pct: Decimal = Decimal("0"),
    status: str = "ACTIVE",
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO risk_states (
            bot_id, symbol, direction,
            entry_fill_price, entry_qty,
            peak_profit_pct, lock_level_pct,
            sl_pct, status, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bot_id, symbol, direction)
        DO UPDATE SET
            entry_fill_price=excluded.entry_fill_price,
            entry_qty=excluded.entry_qty,
            peak_profit_pct=excluded.peak_profit_pct,
            lock_level_pct=excluded.lock_level_pct,
            sl_pct=excluded.sl_pct,
            status=excluded.status,
            updated_ts=excluded.updated_ts
    """, (
        bot_id, symbol, direction.upper(),
        str(_d(entry_fill_price)), str(_d(entry_qty)),
        str(_d(peak_profit_pct)), str(_d(lock_level_pct)),
        str(_d(sl_pct)), str(status), _now()
    ))
    conn.commit()
    conn.close()


def get_risk_state(bot_id: str, symbol: str, direction: str) -> Optional[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM risk_states
        WHERE bot_id=? AND symbol=? AND direction=?
    """, (bot_id, symbol, direction.upper()))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


def update_risk_progress(bot_id: str, symbol: str, direction: str, peak_profit_pct: Decimal, lock_level_pct: Decimal):
    st = get_risk_state(bot_id, symbol, direction)
    if not st:
        return
    upsert_risk_state(
        bot_id=bot_id,
        symbol=symbol,
        direction=direction,
        entry_fill_price=_d(st["entry_fill_price"]),
        entry_qty=_d(st["entry_qty"]),
        sl_pct=_d(st["sl_pct"]),
        peak_profit_pct=_d(peak_profit_pct),
        lock_level_pct=_d(lock_level_pct),
        status=str(st["status"]),
    )


def set_risk_status(bot_id: str, symbol: str, direction: str, status: str):
    st = get_risk_state(bot_id, symbol, direction)
    if not st:
        return
    upsert_risk_state(
        bot_id=bot_id,
        symbol=symbol,
        direction=direction,
        entry_fill_price=_d(st["entry_fill_price"]),
        entry_qty=_d(st["entry_qty"]),
        sl_pct=_d(st["sl_pct"]),
        peak_profit_pct=_d(st["peak_profit_pct"]),
        lock_level_pct=_d(st["lock_level_pct"]),
        status=str(status),
    )


def clear_risk_state(bot_id: str, symbol: str, direction: str):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM risk_states
        WHERE bot_id=? AND symbol=? AND direction=?
    """, (bot_id, symbol, direction.upper()))
    conn.commit()
    conn.close()
