import os
import time
import sqlite3
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple

# -----------------------------
# SQLite configuration
# -----------------------------
PNL_DB_PATH = os.getenv("PNL_DB", "bot_pnl.sqlite")


def _now_ts() -> int:
    return int(time.time())


def _conn():
    # check_same_thread=False 允许你在 Flask + 监控线程里共用
    c = sqlite3.connect(PNL_DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    con = _conn()
    cur = con.cursor()

    # 交易事件表（每次 entry/exit 都记一条）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,          -- LONG / SHORT
        action TEXT NOT NULL,             -- ENTRY / EXIT
        qty TEXT NOT NULL,
        price TEXT NOT NULL,
        reason TEXT,
        realized_pnl TEXT                 -- EXIT 记录时写入（可为空）
    )
    """)

    # 虚拟 lots 账本（每个 bot 独立）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,          -- LONG / SHORT
        entry_ts INTEGER NOT NULL,
        entry_price TEXT NOT NULL,
        qty_open TEXT NOT NULL            -- 该 lot 剩余数量
    )
    """)

    # 索引让查询更快
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_bot ON trades(bot_id, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lots_bot ON lots(bot_id, symbol, direction)")

    con.commit()
    con.close()


# -----------------------------
# Direction mapping
# -----------------------------
def side_to_direction(side: str) -> str:
    s = str(side).upper()
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    # fallback
    return "LONG"


# -----------------------------
# Trade logging
# -----------------------------
def record_entry(
    bot_id: str,
    symbol: str,
    side: str,
    qty: Decimal,
    price: Decimal,
    reason: str = "strategy_entry",
):
    direction = side_to_direction(side)
    ts = _now_ts()

    con = _conn()
    cur = con.cursor()

    # 1) 写 trades
    cur.execute("""
        INSERT INTO trades (ts, bot_id, symbol, direction, action, qty, price, reason, realized_pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ts, bot_id, symbol, direction, "ENTRY",
          str(qty), str(price), reason, None))

    # 2) 加一个 lot
    cur.execute("""
        INSERT INTO lots (bot_id, symbol, direction, entry_ts, entry_price, qty_open)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (bot_id, symbol, direction, ts, str(price), str(qty)))

    con.commit()
    con.close()


def _fetch_open_lots(
    cur,
    bot_id: str,
    symbol: str,
    direction: str,
) -> List[sqlite3.Row]:
    cur.execute("""
        SELECT * FROM lots
        WHERE bot_id=? AND symbol=? AND direction=?
        AND CAST(qty_open AS REAL) > 0
        ORDER BY entry_ts ASC, id ASC
    """, (bot_id, symbol, direction))
    return cur.fetchall()


def record_exit_fifo(
    bot_id: str,
    symbol: str,
    entry_side: str,
    exit_qty: Decimal,
    exit_price: Decimal,
    reason: str = "strategy_exit",
) -> Decimal:
    """
    按 FIFO 用该 bot 自己的 lots 扣减。
    只计算并记录“该 bot 自己的已实现 PnL”。

    返回：本次 exit 的 realized pnl（Decimal）
    """
    direction = side_to_direction(entry_side)
    ts = _now_ts()

    # 硬约束：不允许负数或 0
    if exit_qty <= 0:
        return Decimal("0")

    con = _conn()
    cur = con.cursor()

    # 拿该 bot 的 open lots
    lots = _fetch_open_lots(cur, bot_id, symbol, direction)

    remaining = exit_qty
    realized = Decimal("0")

    # FIFO 扣减
    for lot in lots:
        if remaining <= 0:
            break

        lot_id = lot["id"]
        lot_price = Decimal(str(lot["entry_price"]))
        lot_open = Decimal(str(lot["qty_open"]))

        if lot_open <= 0:
            continue

        match_qty = lot_open if lot_open <= remaining else remaining

        # 计算该 lot 的 realized
        if direction == "LONG":
            realized += (exit_price - lot_price) * match_qty
        else:
            realized += (lot_price - exit_price) * match_qty

        # 更新 lot 剩余
        new_open = lot_open - match_qty
        cur.execute("""
            UPDATE lots SET qty_open=? WHERE id=?
        """, (str(new_open), lot_id))

        remaining -= match_qty

    # 记录 trades（EXIT）
    # 注意：这里允许 “退出数量 > 自己 lots 的剩余” 的情况被自然截断为 FIFO 匹配结果
    # 你要的独立性铁律在“执行层”应当配合 size clamp；
    # 这里仍然记录原始 exit_qty，方便你之后审计
    cur.execute("""
        INSERT INTO trades (ts, bot_id, symbol, direction, action, qty, price, reason, realized_pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ts, bot_id, symbol, direction, "EXIT",
          str(exit_qty), str(exit_price), reason, str(realized)))

    con.commit()
    con.close()

    return realized


# -----------------------------
# PnL summary helpers
# -----------------------------
def _sum_realized(cur, bot_id: str, ts_from: Optional[int] = None) -> Decimal:
    if ts_from is None:
        cur.execute("""
            SELECT realized_pnl FROM trades
            WHERE bot_id=? AND action='EXIT' AND realized_pnl IS NOT NULL
        """, (bot_id,))
    else:
        cur.execute("""
            SELECT realized_pnl FROM trades
            WHERE bot_id=? AND action='EXIT' AND realized_pnl IS NOT NULL AND ts >= ?
        """, (bot_id, ts_from))

    rows = cur.fetchall()
    total = Decimal("0")
    for r in rows:
        try:
            total += Decimal(str(r["realized_pnl"]))
        except Exception:
            pass
    return total


def _fetch_open_lots_all(cur, bot_id: str) -> List[sqlite3.Row]:
    cur.execute("""
        SELECT * FROM lots
        WHERE bot_id=? AND CAST(qty_open AS REAL) > 0
        ORDER BY symbol, direction, entry_ts ASC, id ASC
    """, (bot_id,))
    return cur.fetchall()


def get_bot_open_positions(bot_id: str) -> Dict[Tuple[str, str], Dict[str, Decimal]]:
    """
    返回该 bot 的 open lots 汇总：
    key = (symbol, direction)
    value = {qty, weighted_entry}
    """
    con = _conn()
    cur = con.cursor()

    lots = _fetch_open_lots_all(cur, bot_id)
    agg: Dict[Tuple[str, str], Dict[str, Decimal]] = {}

    for lot in lots:
        symbol = str(lot["symbol"])
        direction = str(lot["direction"])
        qty = Decimal(str(lot["qty_open"]))
        price = Decimal(str(lot["entry_price"]))

        key = (symbol, direction)
        if key not in agg:
            agg[key] = {"qty": Decimal("0"), "cost": Decimal("0")}

        agg[key]["qty"] += qty
        agg[key]["cost"] += qty * price

    # cost -> weighted entry
    out: Dict[Tuple[str, str], Dict[str, Decimal]] = {}
    for key, v in agg.items():
        qty = v["qty"]
        cost = v["cost"]
        wavg = (cost / qty) if qty > 0 else Decimal("0")
        out[key] = {"qty": qty, "weighted_entry": wavg}

    con.close()
    return out


def get_bot_summary(bot_id: str) -> Dict[str, Any]:
    """
    不含未实现（由上层用当前价格计算更合理）
    """
    con = _conn()
    cur = con.cursor()

    now = _now_ts()
    day_from = now - 24 * 3600
    week_from = now - 7 * 24 * 3600

    realized_total = _sum_realized(cur, bot_id, None)
    realized_day = _sum_realized(cur, bot_id, day_from)
    realized_week = _sum_realized(cur, bot_id, week_from)

    # 交易次数
    cur.execute("""
        SELECT COUNT(*) AS c FROM trades WHERE bot_id=?
    """, (bot_id,))
    trades_count = int(cur.fetchone()["c"] or 0)

    con.close()

    return {
        "bot_id": bot_id,
        "realized_total": realized_total,
        "realized_day": realized_day,
        "realized_week": realized_week,
        "trades_count": trades_count,
    }


def list_bots_with_activity() -> List[str]:
    con = _conn()
    cur = con.cursor()

    cur.execute("""
        SELECT DISTINCT bot_id FROM trades
        ORDER BY bot_id ASC
    """)
    rows = cur.fetchall()
    con.close()

    return [str(r["bot_id"]) for r in rows]
