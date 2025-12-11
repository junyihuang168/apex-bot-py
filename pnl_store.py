import os
import sqlite3
import time
from decimal import Decimal
from typing import Dict, Tuple, Any, List

# ✅ 优化：使用绝对路径，防止找不到数据库
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, os.getenv("PNL_DB_PATH", "pnl.sqlite3"))

print(f"[PNL] Database initialized at: {DB_PATH}")

def _now() -> int:
    return int(time.time())

def _d(x) -> Decimal:
    if x is None: return Decimal("0")
    if isinstance(x, Decimal): return x
    return Decimal(str(x))

def _connect():
    # ✅ 关键优化：允许 check_same_thread=False 以支持多线程监控
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    try:
        conn = _connect()
        cur = conn.cursor()
        
        # 1. 批次表 (Lots)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_side TEXT NOT NULL,
            qty TEXT NOT NULL,
            entry_price TEXT NOT NULL,
            remaining_qty TEXT NOT NULL,
            reason TEXT,
            ts INTEGER NOT NULL
        )""")
        
        # 2. 出场表 (Exits)
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
        )""")
        
        # 3. 锁盈状态表 (Lock Levels)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS lock_levels (
            bot_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            lock_level_pct TEXT NOT NULL,
            updated_ts INTEGER NOT NULL,
            PRIMARY KEY (bot_id, symbol, direction)
        )""")
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_active ON lots(bot_id, symbol, direction) WHERE remaining_qty > 0")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[PNL] DB Init Error: {e}")

def _side_to_direction(side: str) -> str:
    return "LONG" if str(side).upper() == "BUY" else "SHORT"

def record_entry(bot_id: str, symbol: str, side: str, qty: Decimal, price: Decimal, reason: str):
    direction = _side_to_direction(side)
    q, p = _d(qty), _d(price)
    if q <= 0: return

    conn = _connect()
    conn.execute("""
        INSERT INTO lots (bot_id, symbol, direction, entry_side, qty, entry_price, remaining_qty, reason, ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (bot_id, symbol, direction, str(side).upper(), str(q), str(p), str(q), reason, _now()))
    conn.commit()
    conn.close()
    print(f"[PNL] Recorded Entry: {bot_id} {symbol} {direction} +{q}")

def record_exit_fifo(bot_id: str, symbol: str, entry_side: str, exit_qty: Decimal, exit_price: Decimal, reason: str):
    direction = _side_to_direction(entry_side)
    exit_side = "SELL" if direction == "LONG" else "BUY"
    need, px_exit = _d(exit_qty), _d(exit_price)
    
    if need <= 0: return

    conn = _connect()
    cur = conn.cursor()
    
    # FIFO: 找最早的剩余仓位
    cur.execute("""
        SELECT * FROM lots 
        WHERE bot_id=? AND symbol=? AND direction=? AND CAST(remaining_qty AS REAL) > 0 
        ORDER BY ts ASC, id ASC
    """, (bot_id, symbol, direction))
    rows = cur.fetchall()

    ts = _now()
    for r in rows:
        if need <= 0: break
        
        rem = _d(r["remaining_qty"])
        entry_price = _d(r["entry_price"])
        take = min(rem, need)
        
        # 计算已实现盈亏
        pnl = (px_exit - entry_price) * take if direction == "LONG" else (entry_price - px_exit) * take
        
        # 更新剩余量
        cur.execute("UPDATE lots SET remaining_qty=? WHERE id=?", (str(rem - take), r["id"]))
        
        # 记录 exit
        cur.execute("""
            INSERT INTO exits (bot_id, symbol, direction, entry_side, exit_side, exit_qty, entry_price, exit_price, realized_pnl, reason, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (bot_id, symbol, direction, str(entry_side).upper(), exit_side, str(take), str(entry_price), str(px_exit), str(pnl), reason, ts))
        
        need -= take

    conn.commit()
    conn.close()
    print(f"[PNL] Recorded Exit: {bot_id} {symbol} {direction} -{exit_qty}")

def get_bot_open_positions(bot_id: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, direction, 
               SUM(CAST(remaining_qty AS REAL)) as q, 
               SUM(CAST(remaining_qty AS REAL) * CAST(entry_price AS REAL)) as notional
        FROM lots 
        WHERE bot_id=? AND CAST(remaining_qty AS REAL) > 0
        GROUP BY symbol, direction
    """, (bot_id,))
    
    out = {}
    for r in cur.fetchall():
        q = _d(r["q"])
        if q > 0:
            avg_entry = _d(r["notional"]) / q
            out[(r["symbol"], r["direction"])] = {"qty": q, "weighted_entry": avg_entry}
    conn.close()
    return out

def list_bots_with_activity() -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT bot_id FROM lots UNION SELECT DISTINCT bot_id FROM exits")
    bots = sorted([r[0] for r in cur.fetchall() if r[0]])
    conn.close()
    return bots

def get_bot_summary(bot_id: str) -> Dict[str, Any]:
    conn = _connect()
    cur = conn.cursor()
    now = _now()
    
    def _sum(ts_start):
        cur.execute("SELECT SUM(CAST(realized_pnl AS REAL)) as s FROM exits WHERE bot_id=? AND ts >= ?", (bot_id, ts_start))
        return _d(cur.fetchone()["s"])

    total = _sum(0)
    day = _sum(now - 86400)
    week = _sum(now - 604800)
    
    cur.execute("SELECT COUNT(*) as c FROM exits WHERE bot_id=?", (bot_id,))
    count = cur.fetchone()["c"]
    
    conn.close()
    return {
        "bot_id": bot_id, 
        "realized_total": str(total), 
        "realized_day": str(day), 
        "realized_week": str(week), 
        "trades_count": count
    }

def get_lock_level_pct(bot_id: str, symbol: str, direction: str) -> Decimal:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT lock_level_pct FROM lock_levels WHERE bot_id=? AND symbol=? AND direction=?", (bot_id, symbol, direction))
    r = cur.fetchone()
    conn.close()
    return _d(r["lock_level_pct"]) if r else Decimal("0")

def set_lock_level_pct(bot_id: str, symbol: str, direction: str, val: Decimal):
    conn = _connect()
    conn.execute("""
        INSERT INTO lock_levels (bot_id, symbol, direction, lock_level_pct, updated_ts)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(bot_id, symbol, direction) DO UPDATE SET lock_level_pct=excluded.lock_level_pct, updated_ts=excluded.updated_ts
    """, (bot_id, symbol, direction, str(val), _now()))
    conn.commit()
    conn.close()

def clear_lock_level_pct(bot_id: str, symbol: str, direction: str):
    conn = _connect()
    conn.execute("DELETE FROM lock_levels WHERE bot_id=? AND symbol=? AND direction=?", (bot_id, symbol, direction))
    conn.commit()
    conn.close()
