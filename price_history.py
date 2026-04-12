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

    # ── Guitar's Home listing tracker ─────────────────────────────────────────
    # Track GH web listings across runs. When a listing disappears → it sold.
    # This gives us REAL days-on-market for GH instead of Instagram approximations.

    def _init_gh_schema(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS gh_listings (
                url         TEXT PRIMARY KEY,
                title       TEXT NOT NULL,
                price_mxn   REAL,
                first_seen  TEXT NOT NULL,
                last_seen   TEXT NOT NULL,
                sold_at     TEXT          -- NULL = still active, ISO date = sold
            );

            CREATE TABLE IF NOT EXISTS gh_sold (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT NOT NULL,
                title       TEXT NOT NULL,
                price_mxn   REAL,
                first_seen  TEXT NOT NULL,
                sold_at     TEXT NOT NULL,
                days_listed INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_gh_sold_title ON gh_sold(title);
        """)
        self.conn.commit()

    def update_gh_listings(self, gh_items: list[dict]) -> list[dict]:
        """
        Call once per pipeline run with the current GH web listings.

        - Inserts new listings (first time seen).
        - Updates last_seen for listings still active.
        - Marks missing listings as sold and records real days_listed.

        Returns list of newly-sold guitars: [{"title", "price_mxn", "days_listed"}]
        """
        self._init_gh_schema()
        now = datetime.utcnow().isoformat()
        today = datetime.utcnow().date()

        current_urls = {item["url"] for item in gh_items if item.get("url")}

        # Upsert active listings
        for item in gh_items:
            url = item.get("url", "")
            if not url:
                continue
            title     = item.get("title", "")
            price_mxn = item.get("price_mxn")
            existing = self.conn.execute(
                "SELECT url, first_seen FROM gh_listings WHERE url = ?", (url,)
            ).fetchone()
            if existing:
                self.conn.execute(
                    "UPDATE gh_listings SET last_seen = ?, price_mxn = ?, sold_at = NULL WHERE url = ?",
                    (now, price_mxn, url),
                )
            else:
                self.conn.execute(
                    "INSERT INTO gh_listings (url, title, price_mxn, first_seen, last_seen, sold_at) VALUES (?,?,?,?,?,NULL)",
                    (url, title, price_mxn, now, now),
                )

        # Detect sold: active listings (sold_at IS NULL) not in current scrape
        active_rows = self.conn.execute(
            "SELECT url, title, price_mxn, first_seen FROM gh_listings WHERE sold_at IS NULL"
        ).fetchall()

        newly_sold = []
        for row in active_rows:
            if row["url"] not in current_urls:
                first_seen_dt = datetime.fromisoformat(row["first_seen"]).date()
                days_listed   = (today - first_seen_dt).days
                self.conn.execute(
                    "UPDATE gh_listings SET sold_at = ? WHERE url = ?", (now, row["url"])
                )
                self.conn.execute(
                    """INSERT INTO gh_sold (url, title, price_mxn, first_seen, sold_at, days_listed)
                       VALUES (?,?,?,?,?,?)""",
                    (row["url"], row["title"], row["price_mxn"],
                     row["first_seen"], now, days_listed),
                )
                newly_sold.append({
                    "title":       row["title"],
                    "price_mxn":   row["price_mxn"],
                    "days_listed": days_listed,
                })

        self.conn.commit()
        return newly_sold

    def get_gh_liquidity(self, title: str, threshold: int = 72) -> dict | None:
        """
        Return real days-on-market stats for a GH guitar model based on
        actual sell events (listing disappeared from GH web).

        Falls back to None if fewer than 2 data points — catalog.py will
        use the Instagram approximation in that case.
        """
        self._init_gh_schema()
        from rapidfuzz import fuzz

        rows = self.conn.execute(
            "SELECT title, days_listed FROM gh_sold"
        ).fetchall()

        matches = []
        for row in rows:
            score = fuzz.token_set_ratio(title.lower(), row["title"].lower())
            if score >= threshold:
                matches.append(row["days_listed"])

        if len(matches) < 2:
            return None

        avg_days  = sum(matches) / len(matches)
        sell_rate = 1.0  # every row in gh_sold DID sell — 100% by definition
        return {
            "avg_days_to_sell": round(avg_days, 1),
            "sell_rate":        sell_rate,
            "count_sold":       len(matches),
            "count_total":      len(matches),
            "source":           "gh_web",   # distinguishes from Instagram estimate
        }
