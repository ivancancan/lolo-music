"""
price_history.py
────────────────
SQLite-based price history tracker.

Every time main.py scrapes a US listing, it records the price.
This lets us:
  - Detect price drops since last observation
  - Know how long a guitar has been listed (seller motivation proxy)
  - Build per-model price trend data over time

Schema:
    observations(id, url, source, title, price_usd, scraped_at)
    price_alerts(id, url, title, old_price, new_price, drop_pct, alerted_at)

Usage:
    from price_history import PriceHistory
    ph = PriceHistory()
    drops = ph.record_batch(us_items)  # returns list of price drop events
    ph.close()
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = "price_history.db"

# Alert if price drops more than this % since last observation
PRICE_DROP_ALERT_PCT = 5.0

# Ignore observations older than this many days when computing "last seen price"
LOOKBACK_DAYS = 30


class PriceHistory:

    def __init__(self, db_path: str = DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS observations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT NOT NULL,
                source      TEXT NOT NULL,
                title       TEXT NOT NULL,
                price_usd   REAL NOT NULL,
                scraped_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_obs_url
                ON observations(url);

            CREATE INDEX IF NOT EXISTS idx_obs_scraped
                ON observations(scraped_at);

            CREATE TABLE IF NOT EXISTS price_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT NOT NULL,
                title       TEXT NOT NULL,
                old_price   REAL NOT NULL,
                new_price   REAL NOT NULL,
                drop_pct    REAL NOT NULL,
                alerted_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sent_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                us_url      TEXT NOT NULL,
                score       INTEGER NOT NULL,
                verdict     TEXT NOT NULL,
                sent_at     TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sent_url
                ON sent_alerts(us_url);
        """)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ── Recording ─────────────────────────────────────────────────────────────

    def record(self, item: dict) -> Optional[dict]:
        """
        Record a single price observation.
        Returns a price-drop event dict if a significant drop is detected, else None.
        """
        url       = item.get("url", "")
        source    = item.get("source", "")
        title     = item.get("title", "")
        price_usd = item.get("price_usd")

        if not url or not price_usd:
            return None

        now = datetime.utcnow().isoformat()

        # Get last known price within lookback window
        cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).isoformat()
        row = self.conn.execute(
            """
            SELECT price_usd, scraped_at
            FROM   observations
            WHERE  url = ?
              AND  scraped_at >= ?
            ORDER  BY scraped_at DESC
            LIMIT  1
            """,
            (url, cutoff),
        ).fetchone()

        # Insert new observation
        self.conn.execute(
            "INSERT INTO observations (url, source, title, price_usd, scraped_at) VALUES (?,?,?,?,?)",
            (url, source, title, price_usd, now),
        )
        self.conn.commit()

        # Check for price drop
        if row:
            old_price = row["price_usd"]
            if old_price > price_usd:
                drop_pct = (old_price - price_usd) / old_price * 100
                if drop_pct >= PRICE_DROP_ALERT_PCT:
                    self.conn.execute(
                        """
                        INSERT INTO price_alerts
                            (url, title, old_price, new_price, drop_pct, alerted_at)
                        VALUES (?,?,?,?,?,?)
                        """,
                        (url, title, old_price, price_usd, round(drop_pct, 1), now),
                    )
                    self.conn.commit()
                    return {
                        "url":       url,
                        "title":     title,
                        "source":    source,
                        "old_price": old_price,
                        "new_price": price_usd,
                        "drop_pct":  round(drop_pct, 1),
                    }

        return None

    def record_batch(self, items: list[dict]) -> list[dict]:
        """
        Record a list of items. Returns all price-drop events detected.
        """
        drops = []
        for item in items:
            drop = self.record(item)
            if drop:
                drops.append(drop)
        return drops

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_first_seen(self, url: str) -> Optional[str]:
        """Return ISO date when this URL was first recorded."""
        row = self.conn.execute(
            "SELECT scraped_at FROM observations WHERE url = ? ORDER BY scraped_at ASC LIMIT 1",
            (url,),
        ).fetchone()
        return row["scraped_at"] if row else None

    def get_days_on_market(self, url: str) -> Optional[int]:
        """Return how many days since this listing was first seen."""
        first = self.get_first_seen(url)
        if not first:
            return None
        first_dt = datetime.fromisoformat(first)
        return (datetime.utcnow() - first_dt).days

    def get_price_history(self, url: str, days: int = 30) -> list[dict]:
        """Return all price observations for a URL within last N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """
            SELECT price_usd, scraped_at
            FROM   observations
            WHERE  url = ? AND scraped_at >= ?
            ORDER  BY scraped_at ASC
            """,
            (url, cutoff),
        ).fetchall()
        return [{"price_usd": r["price_usd"], "scraped_at": r["scraped_at"]} for r in rows]

    def get_min_price(self, url: str, days: int = 30) -> Optional[float]:
        """Return the lowest price seen for this URL in the last N days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        row = self.conn.execute(
            "SELECT MIN(price_usd) as min_p FROM observations WHERE url = ? AND scraped_at >= ?",
            (url, cutoff),
        ).fetchone()
        return row["min_p"] if row else None

    def get_model_price_stats(self, title_keyword: str, days: int = 60) -> Optional[dict]:
        """
        Return avg/min/max price for a guitar model across all sources.
        Useful for benchmarking a new listing against historical market data.
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        row = self.conn.execute(
            """
            SELECT
                COUNT(*)        as count,
                AVG(price_usd)  as avg_p,
                MIN(price_usd)  as min_p,
                MAX(price_usd)  as max_p
            FROM observations
            WHERE title LIKE ?
              AND scraped_at >= ?
            """,
            (f"%{title_keyword}%", cutoff),
        ).fetchone()
        if not row or not row["count"]:
            return None
        return {
            "count": row["count"],
            "avg":   round(row["avg_p"], 2),
            "min":   round(row["min_p"], 2),
            "max":   round(row["max_p"], 2),
        }

    def get_drop_velocity(self, url: str, days: int = 30) -> Optional[float]:
        """
        Return the price drop rate for a listing in drops-per-week.

        Counts how many times the price dropped significantly (>=5%) within
        the last N days, normalized to a weekly rate.

        A velocity of 1.0 = dropped once per week = motivated seller.
        A velocity of 2.0 = dropped twice per week = very motivated.

        Returns None if fewer than 2 observations (can't compute velocity).
        """
        history = self.get_price_history(url, days=days)
        if len(history) < 2:
            return None

        drop_count = 0
        for i in range(1, len(history)):
            prev_price = history[i - 1]["price_usd"]
            curr_price = history[i]["price_usd"]
            if prev_price > 0 and curr_price < prev_price:
                drop_pct = (prev_price - curr_price) / prev_price * 100
                if drop_pct >= PRICE_DROP_ALERT_PCT:
                    drop_count += 1

        if drop_count == 0:
            return None

        weeks = days / 7.0
        return round(drop_count / weeks, 2)

    def get_recent_drops(self, hours: int = 24) -> list[dict]:
        """Return all price drop alerts from the last N hours."""
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        rows = self.conn.execute(
            """
            SELECT * FROM price_alerts
            WHERE  alerted_at >= ?
            ORDER  BY drop_pct DESC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Alert dedup ────────────────────────────────────────────────────────────

    def was_alerted(self, us_url: str, verdict: str) -> bool:
        """Check if this URL was already alerted with this verdict (BUY NOW / REVIEW)."""
        row = self.conn.execute(
            "SELECT id FROM sent_alerts WHERE us_url = ? AND verdict = ? LIMIT 1",
            (us_url, verdict),
        ).fetchone()
        return row is not None

    def record_alert(self, us_url: str, score: int, verdict: str) -> None:
        """Record that an alert was sent for this URL."""
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "INSERT INTO sent_alerts (us_url, score, verdict, sent_at) VALUES (?,?,?,?)",
            (us_url, score, verdict, now),
        )
        self.conn.commit()

    def cleanup_old_alerts(self, days: int = 30) -> int:
        """Remove sent_alerts older than N days. Returns count deleted."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        cursor = self.conn.execute(
            "DELETE FROM sent_alerts WHERE sent_at < ?", (cutoff,)
        )
        self.conn.commit()
        return cursor.rowcount

    def summary(self) -> dict:
        """Return database stats."""
        total = self.conn.execute("SELECT COUNT(*) as c FROM observations").fetchone()["c"]
        urls  = self.conn.execute("SELECT COUNT(DISTINCT url) as c FROM observations").fetchone()["c"]
        drops = self.conn.execute("SELECT COUNT(*) as c FROM price_alerts").fetchone()["c"]
        return {"total_observations": total, "unique_listings": urls, "price_drops_detected": drops}
