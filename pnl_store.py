import os
import sqlite3
import time
from decimal import Decimal
from typing import Dict, Tuple, Any, List, Callable, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, os.getenv("PNL_DB_PATH", "pnl.sqlite3"))

print(f"[PNL] Database path set to: {DB_PATH}")

SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))
SQLITE_WRITE_RETRY = int(os.getenv("SQLITE_WRITE_RETRY", "5"))
SQLITE_WRITE_RETRY_SLEEP = float(os.getenv("SQLITE_WRITE_RETRY_SLEEP", "0.15"))

# ✅ 迁移覆盖：设置 PNL_MIGRATE_OVERWRITE=1 会 DROP 旧表并重建
PNL_MIGRATE_OVERWRITE = os.getenv("PNL_MIGRATE_OVERWRITE", "0").lower() in ("1", "true", "yes", "y", "on")


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


def _write_with_retry(fn: Callable[[sqlite3.Connection], Any]) -> Any:
    last_err = None
    for i in range(SQLITE_WRITE_RETRY):
        try:
            conn = _connect()
            try:
                out = fn(conn)
                conn.commit()
                return out
            finally:
                conn.close()
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                time.sleep(SQLITE_WRITE_RETRY_SLEEP * (i + 1))
                continue
            raise
        except Exception as e:
            last_err = e
            raise
    raise RuntimeError(f"[PNL] sqlite write failed after retries: {last_err!r}")


def init_db():
    conn = _connect()
    try:
        cur = conn.cursor()

        if PNL_MIGRATE_OVERWRITE:
            print("[PNL] PNL_MIGRATE_OVERWRITE=1 -> dropping existing tables...")
            cur.execute("DROP TABLE IF EXISTS lots")
            cur.execute("DROP TABLE IF EXISTS exits")
            cur.execute("DROP TABLE IF EXISTS lock_levels")
            cur.execute("DROP TABLE IF EXISTS processed_signals")
            cur.execute("DROP TABLE IF EXISTS orders")
            cur.execute("DROP TABLE IF EXISTS brackets")
            cur.execute("DROP TABLE IF EXISTS fills_seen")

        # lots: 每次 entry 一条（WS fill 可多笔 -> 多条 lot）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,        -- LONG / SHORT
            entry_side TEXT NOT NULL,       -- BUY / SELL
            qty TEXT NOT NULL,              -- Decimal as text
            entry_price TEXT NOT NULL,      -- Decimal as text
            remaining_qty TEXT NOT NULL,    -- Decimal as text
            reason TEXT,
            ts INTEGER NOT NULL
        )
        """)

        # exits: 每次出场记录（FIFO）
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

        # lock levels（你旧逻辑保留，不一定用）
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

        # processed signals（你旧逻辑保留）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_signals (
            bot_id TEXT NOT NULL,
            signal_id TEXT NOT NULL,
            kind TEXT,
            ts INTEGER NOT NULL,
            PRIMARY KEY (bot_id, signal_id)
        )
        """)

        # ✅ NEW: orders 映射（orderId -> bot/意图），用于 WS fill 归因
        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            client_order_id TEXT,
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,      -- LONG/SHORT（仓位方向）
            entry_side TEXT NOT NULL,     -- BUY/SELL（开仓方向）
            intent TEXT NOT NULL,         -- ENTRY/EXIT/TP/SL
            reduce_only INTEGER NOT NULL, -- 0/1
            ts INTEGER NOT NULL
        )
        """)

        # ✅ NEW: brackets（entry 对应的 TP/SL 订单）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS brackets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_order_id TEXT,
            tp_order_id TEXT,
            sl_order_id TEXT,
            ts INTEGER NOT NULL
        )
        """)

        # ✅ NEW: fills_seen（WS fills 去重：fillId 唯一）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fills_seen (
            fill_id TEXT PRIMARY KEY,
            order_id TEXT,
            ts INTEGER NOT NULL
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_bot_symbol ON lots(bot_id, symbol, direction, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exits_bot_ts ON exits(bot_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_bot_ts ON processed_signals(bot_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_bot_symbol ON orders(bot_id, symbol, direction, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_brackets_bot_symbol ON brackets(bot_id, symbol, direction, ts)")

        conn.commit()
        print("[PNL] Database initialized successfully.")
    finally:
        conn.close()


def _side_to_direction(entry_side: str) -> str:
    s = str(entry_side).upper()
    return "LONG" if s == "BUY" else "SHORT"


# ---------------------------
# Idempotency helpers（保留你原逻辑）
# ---------------------------
def is_signal_processed(bot_id: str, signal_id: str) -> bool:
    if not bot_id or not signal_id:
        return False
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM processed_signals
            WHERE bot_id=? AND signal_id=?
            LIMIT 1
        """, (str(bot_id), str(signal_id)))
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_signal_processed(bot_id: str, signal_id: str, kind: str = ""):
    if not bot_id or not signal_id:
        return

    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO processed_signals (bot_id, signal_id, kind, ts)
            VALUES (?, ?, ?, ?)
        """, (str(bot_id), str(signal_id), str(kind or ""), _now()))
        return True

    _write_with_retry(_w)


# ---------------------------
# ✅ WS fill 去重
# ---------------------------
def is_fill_seen(fill_id: str) -> bool:
    if not fill_id:
        return False
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM fills_seen WHERE fill_id=? LIMIT 1", (str(fill_id),))
        return cur.fetchone() is not None
    finally:
        conn.close()


def mark_fill_seen(fill_id: str, order_id: str = ""):
    if not fill_id:
        return

    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO fills_seen (fill_id, order_id, ts)
            VALUES (?, ?, ?)
        """, (str(fill_id), str(order_id or ""), _now()))
        return True

    _write_with_retry(_w)


# ---------------------------
# ✅ orders / brackets 映射
# ---------------------------
def record_order_map(
    order_id: str,
    client_order_id: str,
    bot_id: str,
    symbol: str,
    direction: str,
    entry_side: str,
    intent: str,
    reduce_only: bool,
):
    if not order_id:
        return

    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO orders (
                order_id, client_order_id, bot_id, symbol, direction, entry_side, intent, reduce_only, ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(order_id),
            str(client_order_id or ""),
            str(bot_id),
            str(symbol),
            str(direction).upper(),
            str(entry_side).upper(),
            str(intent).upper(),
            1 if reduce_only else 0,
            _now(),
        ))
        return True

    _write_with_retry(_w)


def find_order_map(order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id=? LIMIT 1", (str(order_id),))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def record_bracket(bot_id: str, symbol: str, direction: str, entry_order_id: str, tp_order_id: str, sl_order_id: str):
    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO brackets (bot_id, symbol, direction, entry_order_id, tp_order_id, sl_order_id, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            str(bot_id), str(symbol), str(direction).upper(),
            str(entry_order_id or ""), str(tp_order_id or ""), str(sl_order_id or ""),
            _now()
        ))
        return True
    _write_with_retry(_w)


def find_bracket_by_order(order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM brackets
            WHERE tp_order_id=? OR sl_order_id=? OR entry_order_id=?
            ORDER BY id DESC
            LIMIT 1
        """, (str(order_id), str(order_id), str(order_id)))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ---------------------------
# 记账：entry / exit（你原逻辑，略作封装）
# ---------------------------
def record_entry(
    bot_id: str,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    reason: str = "ws_entry_fill",
):
    direction = _side_to_direction(side)
    q = _d(qty)
    p = _d(price)

    if q <= 0 or p <= 0:
        raise ValueError("record_entry requires qty>0 and price>0")

    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO lots (bot_id, symbol, direction, entry_side, qty, entry_price, remaining_qty, reason, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            bot_id, symbol, direction, str(side).upper(),
            str(q), str(p), str(q), reason, _now()
        ))
        return True

    _write_with_retry(_w)
    print(f"[PNL] record_entry SUCCESS: bot={bot_id} {direction} {symbol} qty={q} @ {p}")


def record_exit_fifo(
    bot_id: str,
    symbol: str,
    entry_side: str,
    exit_qty: Decimal,
    exit_price: Decimal,
    reason: str = "exchange_exit",
):
    direction = _side_to_direction(entry_side)
    exit_side = "SELL" if direction == "LONG" else "BUY"

    need = _d(exit_qty)
    px_exit = _d(exit_price)

    if need <= 0 or px_exit <= 0:
        raise ValueError("record_exit_fifo requires exit_qty>0 and exit_price>0")

    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM lots
            WHERE bot_id=? AND symbol=? AND direction=? AND CAST(remaining_qty AS REAL) > 0
            ORDER BY ts ASC, id ASC
        """, (bot_id, symbol, direction))

        rows = cur.fetchall()
        if not rows:
            print(f"[PNL] WARNING: record_exit_fifo found NO open lots for {bot_id} {symbol}")
            return {"remaining_need": str(need)}

        ts = _now()
        remaining_need = need

        for r in rows:
            if remaining_need <= 0:
                break

            lot_id = r["id"]
            rem = _d(r["remaining_qty"])
            entry_price = _d(r["entry_price"])

            if rem <= 0:
                continue

            take = rem if rem <= remaining_need else remaining_need

            if direction == "LONG":
                pnl = (px_exit - entry_price) * take
            else:
                pnl = (entry_price - px_exit) * take

            new_rem = rem - take

            cur.execute("UPDATE lots SET remaining_qty=? WHERE id=?", (str(new_rem), lot_id))

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

            remaining_need -= take

        return {"remaining_need": str(remaining_need)}

    out = _write_with_retry(_w)
    print(f"[PNL] record_exit_fifo DONE for {bot_id} {symbol}. Remaining need={out.get('remaining_need')}")


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
    rows = cur.fetchall()

    for r in rows:
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


def get_lock_level_pct(bot_id: str, symbol: str, direction: str) -> Decimal:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT lock_level_pct FROM lock_levels
        WHERE bot_id=? AND symbol=? AND direction=?
    """, (bot_id, symbol, direction.upper()))
    row = cur.fetchone()
    conn.close()
    if not row:
        return Decimal("0")
    try:
        return _d(row["lock_level_pct"])
    except Exception:
        return Decimal("0")


def set_lock_level_pct(bot_id: str, symbol: str, direction: str, lock_level_pct: Decimal):
    lvl = _d(lock_level_pct)

    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO lock_levels (bot_id, symbol, direction, lock_level_pct, updated_ts)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(bot_id, symbol, direction)
            DO UPDATE SET lock_level_pct=excluded.lock_level_pct, updated_ts=excluded.updated_ts
        """, (bot_id, symbol, direction.upper(), str(lvl), _now()))
        return True

    _write_with_retry(_w)


def clear_lock_level_pct(bot_id: str, symbol: str, direction: str):
    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM lock_levels
            WHERE bot_id=? AND symbol=? AND direction=?
        """, (bot_id, symbol, direction.upper()))
        return True

    _write_with_retry(_w)
