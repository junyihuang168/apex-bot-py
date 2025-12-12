# pnl_store.py
import os
import sqlite3
import time
from decimal import Decimal

DB_PATH = os.getenv("PNL_DB_PATH", "pnl_store.sqlite3")


def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,              -- LONG / SHORT
                qty TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                lock_level_pct TEXT NOT NULL,    -- percent, e.g. 0.10
                closing INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (bot_id, symbol)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,            -- OPEN / CLOSE
                side TEXT NOT NULL,              -- BUY / SELL
                qty TEXT NOT NULL,
                price TEXT NOT NULL,
                reason TEXT,
                order_id TEXT,
                client_id TEXT,
                ts INTEGER NOT NULL
            )
            """
        )


def get_position(bot_id: str, symbol: str):
    with _conn() as con:
        r = con.execute(
            "SELECT * FROM positions WHERE bot_id=? AND symbol=?",
            (bot_id, symbol),
        ).fetchone()
        return dict(r) if r else None


def upsert_entry(bot_id: str, symbol: str, side: str, qty: Decimal, entry_price: Decimal):
    now = int(time.time())
    pos_side = "LONG" if side == "BUY" else "SHORT"

    with _conn() as con:
        r = con.execute(
            "SELECT * FROM positions WHERE bot_id=? AND symbol=?",
            (bot_id, symbol),
        ).fetchone()

        if r:
            # same direction only; otherwise reject
            if r["side"] != pos_side:
                raise RuntimeError(f"Position side mismatch: existing={r['side']} new={pos_side}")

            old_qty = Decimal(r["qty"])
            old_px = Decimal(r["entry_price"])
            new_qty = old_qty + qty
            wavg = (old_qty * old_px + qty * entry_price) / new_qty

            con.execute(
                """
                UPDATE positions
                SET qty=?, entry_price=?, updated_at=?, closing=0
                WHERE bot_id=? AND symbol=?
                """,
                (str(new_qty), str(wavg), now, bot_id, symbol),
            )
        else:
            con.execute(
                """
                INSERT INTO positions (bot_id, symbol, side, qty, entry_price, lock_level_pct, closing, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (bot_id, symbol, pos_side, str(qty), str(entry_price), "0", now),
            )


def mark_closing(bot_id: str, symbol: str, closing: bool):
    now = int(time.time())
    with _conn() as con:
        con.execute(
            "UPDATE positions SET closing=?, updated_at=? WHERE bot_id=? AND symbol=?",
            (1 if closing else 0, now, bot_id, symbol),
        )


def delete_position(bot_id: str, symbol: str):
    with _conn() as con:
        con.execute("DELETE FROM positions WHERE bot_id=? AND symbol=?", (bot_id, symbol))


def set_lock_level(bot_id: str, symbol: str, lock_level_pct: Decimal):
    now = int(time.time())
    with _conn() as con:
        con.execute(
            "UPDATE positions SET lock_level_pct=?, updated_at=? WHERE bot_id=? AND symbol=?",
            (str(lock_level_pct), now, bot_id, symbol),
        )


def list_positions():
    with _conn() as con:
        rows = con.execute("SELECT * FROM positions").fetchall()
        return [dict(r) for r in rows]


def add_trade(bot_id: str, symbol: str, action: str, side: str, qty: Decimal, price: Decimal, reason: str | None, order_id: str | None, client_id: str | None):
    ts = int(time.time())
    with _conn() as con:
        con.execute(
            """
            INSERT INTO trades (bot_id, symbol, action, side, qty, price, reason, order_id, client_id, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (bot_id, symbol, action, side, str(qty), str(price), reason, order_id, client_id, ts),
        )
