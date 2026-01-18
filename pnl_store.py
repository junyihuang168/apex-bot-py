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
    """
    ✅ 迁移策略：
    - CREATE TABLE IF NOT EXISTS：补齐缺失表
    - CREATE INDEX IF NOT EXISTS：补齐索引
    - 不删除老表/不改老字段，确保“直接迁移覆盖”安全
    """
    try:
        conn = _connect()
        cur = conn.cursor()

        # lots: 每次 entry 一条
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

        # exits: 每次出场记录（可多笔 lot 贡献）
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

        # lock levels（保留旧逻辑兼容）
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

        # processed_signals：幂等去重（你已有）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_signals (
            bot_id TEXT NOT NULL,
            signal_id TEXT NOT NULL,
            kind TEXT,
            ts INTEGER NOT NULL,
            PRIMARY KEY (bot_id, signal_id)
        )
        """)

        # ✅ NEW：交易所挂 TP/SL 的保护单记录（用于 WS fills 自动记账 + OCO）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS protective_orders (
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,              -- LONG / SHORT
            sl_order_id TEXT,
            tp_order_id TEXT,
            sl_client_id TEXT,
            tp_client_id TEXT,
            sl_price TEXT,
            tp_price TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            updated_ts INTEGER NOT NULL,
            PRIMARY KEY (bot_id, symbol, direction)
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_bot_symbol ON lots(bot_id, symbol, direction, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_exits_bot_ts ON exits(bot_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_bot_ts ON processed_signals(bot_id, ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_po_sl ON protective_orders(sl_order_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_po_tp ON protective_orders(tp_order_id)")

        conn.commit()
        conn.close()
        print("[PNL] Database initialized/migrated successfully.")
    except Exception as e:
        print(f"[PNL] CRITICAL ERROR initializing database at {DB_PATH}: {e}")


def _side_to_direction(entry_side: str) -> str:
    s = str(entry_side).upper()
    return "LONG" if s == "BUY" else "SHORT"


# ---------------------------
# Idempotency helpers
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
        row = cur.fetchone()
        return row is not None
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


# Backward-compatible alias (older app.py / worker.py may import this name)
def upsert_protective_orders(
    bot_id: str,
    symbol: str,
    direction: str,
    sl_order_id: Optional[str] = None,
    tp_order_id: Optional[str] = None,
    sl_client_id: Optional[str] = None,
    tp_client_id: Optional[str] = None,
    sl_price: Optional[Decimal] = None,
    tp_price: Optional[Decimal] = None,
    is_active: bool = True,
):
    """
    Compatibility wrapper for older code paths.
    Internally calls set_protective_orders().
    """
    return set_protective_orders(
        bot_id=bot_id,
        symbol=symbol,
        direction=direction,
        sl_order_id=sl_order_id,
        tp_order_id=tp_order_id,
        sl_client_id=sl_client_id,
        tp_client_id=tp_client_id,
        sl_price=sl_price,
        tp_price=tp_price,
        is_active=is_active,
    )


# Older worker builds may import these names. Provide no-op stubs for safety.
def list_pending_orders(*args, **kwargs):
    return []

def add_pending_order(*args, **kwargs):
    return True

def remove_pending_order(*args, **kwargs):
    return True

# ---------------------------
# Core PnL
# ---------------------------
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
    reason: str = "strategy_exit",
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

            # realized pnl
            if direction == "LONG":
                pnl = (px_exit - entry_price) * take
            else:
                pnl = (entry_price - px_exit) * take

            new_rem = rem - take

            # update lot remaining
            cur.execute("""
                UPDATE lots SET remaining_qty=?
                WHERE id=?
            """, (str(new_rem), lot_id))

            # write exit record
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
    """
    返回：
    {
      (symbol, direction): {
          "qty": Decimal,
          "weighted_entry": Decimal
      }
    }
    """
    conn = _connect()
    try:
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

            out[(symbol, direction)] = {
                "qty": qty_sum,
                "weighted_entry": weighted
            }

        return out
    finally:
        conn.close()


def get_symbol_open_directions(symbol: str) -> Set[str]:
    """Return a set of open directions {'LONG','SHORT'} for the given symbol across ALL bots."""
    symbol = str(symbol).upper().strip()
    if not symbol:
        return set()

    conn = _connect()
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT DISTINCT direction
            FROM lots
            WHERE symbol=? AND CAST(remaining_qty AS REAL) > 0
            """,
            (symbol,),
        ).fetchall()

        dirs: Set[str] = set()
        for (direction,) in rows:
            d = str(direction or "").upper().strip()
            if d in ("LONG", "SHORT"):
                dirs.add(d)
        return dirs
    finally:
        conn.close()


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
# Lock level persistence（保留兼容）
# ---------------------------
def get_lock_level_pct(bot_id: str, symbol: str, direction: str) -> Decimal:
    bot_id = str(bot_id).upper().strip()
    symbol = str(symbol).upper().strip()
    direction = str(direction).upper().strip()
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
    bot_id = str(bot_id).upper().strip()
    symbol = str(symbol).upper().strip()
    direction = str(direction).upper().strip()
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
    bot_id = str(bot_id).upper().strip()
    symbol = str(symbol).upper().strip()
    direction = str(direction).upper().strip()
    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM lock_levels
            WHERE bot_id=? AND symbol=? AND direction=?
        """, (bot_id, symbol, direction.upper()))
        return True

    _write_with_retry(_w)


# ---------------------------
# ✅ Protective Orders (TP/SL) persistence
# ---------------------------
def set_protective_orders(
    bot_id: str,
    symbol: str,
    direction: str,
    sl_order_id: Optional[str],
    tp_order_id: Optional[str],
    sl_client_id: Optional[str],
    tp_client_id: Optional[str],
    sl_price: Optional[Decimal],
    tp_price: Optional[Decimal],
    is_active: bool = True,
):
    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO protective_orders (
                bot_id, symbol, direction,
                sl_order_id, tp_order_id, sl_client_id, tp_client_id,
                sl_price, tp_price, is_active, updated_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bot_id, symbol, direction)
            DO UPDATE SET
                sl_order_id=excluded.sl_order_id,
                tp_order_id=excluded.tp_order_id,
                sl_client_id=excluded.sl_client_id,
                tp_client_id=excluded.tp_client_id,
                sl_price=excluded.sl_price,
                tp_price=excluded.tp_price,
                is_active=excluded.is_active,
                updated_ts=excluded.updated_ts
        """, (
            bot_id, symbol, direction.upper(),
            str(sl_order_id) if sl_order_id else None,
            str(tp_order_id) if tp_order_id else None,
            str(sl_client_id) if sl_client_id else None,
            str(tp_client_id) if tp_client_id else None,
            str(_d(sl_price)) if sl_price is not None else None,
            str(_d(tp_price)) if tp_price is not None else None,
            1 if is_active else 0,
            _now(),
        ))
        return True

    _write_with_retry(_w)


def get_protective_orders(bot_id: str, symbol: str, direction: str) -> Dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM protective_orders
            WHERE bot_id=? AND symbol=? AND direction=?
            LIMIT 1
        """, (bot_id, symbol, direction.upper()))
        row = cur.fetchone()
        if not row:
            return {}
        return dict(row)
    finally:
        conn.close()


def clear_protective_orders(bot_id: str, symbol: str, direction: str):
    def _w(conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM protective_orders
            WHERE bot_id=? AND symbol=? AND direction=?
        """, (bot_id, symbol, direction.upper()))
        return True

    _write_with_retry(_w)


def list_active_protective_orders(bot_id: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    """
    List active protective orders (is_active=1).
    Used by worker risk loop to reconcile triggered STOP orders.
    """
    try:
        lim = int(limit)
        if lim < 1:
            lim = 200
        lim = min(lim, 1000)
    except Exception:
        lim = 200

    conn = _connect()
    try:
        cur = conn.cursor()
        if bot_id:
            cur.execute("""
                SELECT * FROM protective_orders
                WHERE is_active=1 AND bot_id=?
                ORDER BY ts DESC
                LIMIT ?
            """, (str(bot_id), lim))
        else:
            cur.execute("""
                SELECT * FROM protective_orders
                WHERE is_active=1
                ORDER BY ts DESC
                LIMIT ?
            """, (lim,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()



def find_protective_owner_by_order_id(order_id: str) -> Dict[str, Any]:
    """
    给 WS fill 用：通过 orderId 找到属于哪个 bot/symbol/direction，以及是 SL 还是 TP
    """
    if not order_id:
        return {}
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM protective_orders
            WHERE (sl_order_id=? OR tp_order_id=?)
              AND is_active=1
            LIMIT 1
        """, (str(order_id), str(order_id)))
        row = cur.fetchone()
        if not row:
            return {}
        d = dict(row)
        kind = "SL" if d.get("sl_order_id") == str(order_id) else "TP"
        d["kind"] = kind
        return d
    finally:
        conn.close()


# Backward-compatible alias (older app.py expects this name)
def get_protective_owner_by_order_id(order_id: str) -> Dict[str, Any]:
    return find_protective_owner_by_order_id(order_id)


def list_recent_trades(bot_id: Optional[str] = None, limit: int = 200) -> list:
    """
    Recent realized trades (from exits table), newest first.
    Used by /dashboard and /api/trades.
    """
    try:
        limit_i = int(limit)
        if limit_i < 1:
            limit_i = 200
        limit_i = min(limit_i, 500)
    except Exception:
        limit_i = 200

    conn = _connect()
    try:
        cur = conn.cursor()
        if bot_id:
            cur.execute("""
                SELECT ts, bot_id, symbol, direction, entry_side, exit_qty, entry_price, exit_price,
                       realized_pnl, reason
                FROM exits
                WHERE bot_id=?
                ORDER BY ts DESC
                LIMIT ?
            """, (str(bot_id), limit_i))
        else:
            cur.execute("""
                SELECT ts, bot_id, symbol, direction, entry_side, exit_qty, entry_price, exit_price,
                       realized_pnl, reason
                FROM exits
                ORDER BY ts DESC
                LIMIT ?
            """, (limit_i,))
        rows = cur.fetchall()
        out = []
        for r in rows:
            d = dict(r)
            out.append({
                "ts": int(d.get("ts") or 0),
                "bot_id": d.get("bot_id"),
                "symbol": d.get("symbol"),
                "direction": d.get("direction"),
                "entry_side": d.get("entry_side"),
                "exit_qty": str(d.get("exit_qty") or ""),
                "entry_price": str(d.get("entry_price") or ""),
                "exit_price": str(d.get("exit_price") or ""),
                "realized_pnl": str(d.get("realized_pnl") or "0"),
                "reason": d.get("reason") or "",
            })
        return out
    finally:
        conn.close()
