"""
catalog.py
──────────
Loads the Guitar's Home Instagram sold catalog (gh_sold_catalog.csv)
and provides fuzzy-match lookups for:

  1. REACTIVE mode  — given a US listing, find if GH sold something similar
                      and return the historical MX price as benchmark.

  2. PROACTIVE mode — return all guitar models GH has sold historically
                      so main.py can search US stores for them directly.
"""

import csv
import re
from datetime import datetime, timedelta
from typing import Optional

from rapidfuzz import fuzz

SOLD_CSV        = "gh_sold_catalog.csv"
HISTORY_CSV     = "gh_instagram_history.csv"
FUZZY_THRESHOLD = 72   # minimum score to consider a title match
USD_MXN_DEFAULT = 19.5


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def _load_csv(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def load_sold_catalog() -> list[dict]:
    """Load only confirmed sold guitars with a price."""
    rows = _load_csv(SOLD_CSV)
    result = []
    for row in rows:
        price_mxn = _parse_price(row.get("price_mxn", ""))
        price_usd = _parse_price(row.get("price_usd", ""))
        title     = (row.get("title") or "").strip()
        if not title:
            continue
        if not price_mxn and not price_usd:
            continue
        result.append({
            "title":     title,
            "price_mxn": price_mxn,
            "price_usd": price_usd,
            "date":      row.get("date", ""),
            "url":       row.get("url", ""),
        })
    return result


def load_full_history() -> list[dict]:
    """Load all posts (including unsold) that have a price."""
    rows = _load_csv(HISTORY_CSV)
    result = []
    for row in rows:
        price_mxn = _parse_price(row.get("price_mxn", ""))
        price_usd = _parse_price(row.get("price_usd", ""))
        title     = (row.get("title") or "").strip()
        if not title or (not price_mxn and not price_usd):
            continue
        result.append({
            "title":     title,
            "price_mxn": price_mxn,
            "price_usd": price_usd,
            "is_sold":   row.get("is_sold", "NO") == "SI",
            "date":      row.get("date", ""),
            "url":       row.get("url", ""),
        })
    return result


def get_fresh_posts(history: list[dict], hours: int = 48) -> list[dict]:
    """
    Return posts from the last N hours that have a price.
    These represent guitars GH just posted — highest-confidence benchmark.
    """
    cutoff = datetime.now() - timedelta(hours=hours)
    fresh = []
    for row in history:
        date_str = row.get("date", "")
        if not date_str:
            continue
        try:
            post_date = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        if post_date >= cutoff and (row.get("price_mxn") or row.get("price_usd")):
            fresh.append(row)
    return fresh


def _parse_price(val: str) -> Optional[float]:
    val = str(val).strip()
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Title normalization
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(title: str) -> str:
    title = title.lower()
    title = re.sub(r"[''`]", "", title)
    title = re.sub(r"[-–—/]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


# ─────────────────────────────────────────────────────────────────────────────
# Reactive lookup — find historical GH price for a given guitar title
# ─────────────────────────────────────────────────────────────────────────────

def find_gh_historical_price(
    title: str,
    catalog: list[dict],
    usd_mxn: float = USD_MXN_DEFAULT,
    threshold: int = FUZZY_THRESHOLD,
) -> Optional[dict]:
    """
    Given a guitar title (from a US store), find the best matching entry
    in GH's historical sold catalog and return its price in USD.

    Returns:
        {
          "price_usd": float,        # price GH sold it for, in USD
          "price_mxn": float | None,
          "score":     int,
          "match_title": str,
          "match_date":  str,
          "match_url":   str,
          "source": "instagram_sold"
        }
        or None if no match above threshold.
    """
    if not catalog:
        return None

    norm_query = _normalize(title)
    best_score = 0
    best_row   = None

    for row in catalog:
        norm_title = _normalize(row["title"])
        score = fuzz.token_set_ratio(norm_query, norm_title)
        if score > best_score:
            best_score = score
            best_row   = row

    if best_score < threshold or best_row is None:
        return None

    # Convert to USD
    price_mxn = best_row.get("price_mxn")
    price_usd = best_row.get("price_usd")

    if price_usd:
        usd = price_usd
    elif price_mxn:
        usd = price_mxn / usd_mxn
    else:
        return None

    return {
        "price_usd":   round(usd, 2),
        "price_mxn":   price_mxn,
        "score":       best_score,
        "match_title": best_row["title"],
        "match_date":  best_row["date"],
        "match_url":   best_row["url"],
        "source":      "instagram_sold",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Proactive catalog — list of models GH sells, for active US searching
# ─────────────────────────────────────────────────────────────────────────────

def build_proactive_targets(
    catalog: list[dict],
    min_price_mxn: float = 35_000,
    usd_mxn: float = USD_MXN_DEFAULT,
    top_n: int = 60,
) -> list[dict]:
    """
    Returns a ranked list of guitar models GH sells most often
    and at the best prices — for proactive US store searching.

    Each entry:
        {
          "title":          str,   # canonical GH title
          "avg_price_usd":  float, # average sell price in USD
          "count":          int,   # how many times GH sold this model
          "search_query":   str,   # simplified query for Reverb/Dave's
        }

    Only includes guitars above min_price_mxn to focus on premium inventory.
    """
    if not catalog:
        return []

    # Group by normalized title (fuzzy cluster)
    clusters: list[dict] = []

    for row in catalog:
        price_mxn = row.get("price_mxn")
        price_usd = row.get("price_usd")

        if price_mxn and price_mxn < min_price_mxn:
            continue

        usd = price_usd if price_usd else (price_mxn / usd_mxn if price_mxn else None)
        if not usd:
            continue

        norm = _normalize(row["title"])
        matched = False

        for cluster in clusters:
            if fuzz.token_set_ratio(norm, cluster["norm"]) >= 80:
                cluster["prices_usd"].append(usd)
                cluster["count"] += 1
                matched = True
                break

        if not matched:
            clusters.append({
                "norm":       norm,
                "title":      row["title"],
                "prices_usd": [usd],
                "count":      1,
            })

    # Build output
    targets = []
    for cl in clusters:
        avg = sum(cl["prices_usd"]) / len(cl["prices_usd"])
        targets.append({
            "title":         cl["title"],
            "avg_price_usd": round(avg, 0),
            "count":         cl["count"],
            "search_query":  _build_search_query(cl["title"]),
        })

    # Sort by count × avg_price (most sold + most expensive first)
    targets.sort(key=lambda x: x["count"] * x["avg_price_usd"], reverse=True)
    return targets[:top_n]


def _build_search_query(title: str) -> str:
    """
    Build a search query keeping the model and recent production year.

    Rules:
    - Keep vintage model-indicator years (≤1999) — "1959 Les Paul" is a model name, not a date
    - Keep recent production years (≥2015) — affects inventory availability and price
    - Strip mid-era years (2000-2014) — too old for current inventory, adds noise

    Examples:
      "Gibson Les Paul Standard 50s Tobacco Burst 2023" → "Gibson Les Paul Standard 50s 2023"
      "Gibson Custom 1959 Les Paul Reissue 2022"        → "Gibson Custom 1959 Les Paul Reissue 2022"
      "Gibson Les Paul Standard 2011"                   → "Gibson Les Paul Standard"
    """
    # Remove mid-era production years (2000-2014 only) — too old, just noise for current stock
    title = re.sub(r"\b(200\d|201[0-4])\b", "", title)
    # Remove common finish/color words
    drops = [
        "sunburst", "burst", "cherry", "tobacco", "honeyburst", "vintage",
        "natural", "black", "white", "red", "blue", "green", "gold", "silver",
        "ebony", "rosewood", "maple", "mahogany", "translucent", "trans",
        "faded", "worn", "gloss", "matte", "satin", "relic", "aged",
        "seafoam", "pelham", "lake placid", "olympic", "surf", "daphne",
        "fiesta", "candy apple", "shoreline", "inca", "shell pink",
        "rare", "mint", "excellent", "used", "new",
    ]
    pattern = r"\b(" + "|".join(drops) + r")\b"
    title = re.sub(pattern, "", title, flags=re.I)
    # Remove trailing punctuation and extra spaces
    title = re.sub(r"[-–—/\\|*]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip(" –-")
    # Limit to first 6 words
    words = title.split()[:6]
    return " ".join(words)


# ─────────────────────────────────────────────────────────────────────────────
# Liquidity score
# ─────────────────────────────────────────────────────────────────────────────

SOLD_ASSUMPTION_DAYS = 60  # posts older than this without SOLD marker → treat as sold

def build_liquidity_scores(history: list[dict]) -> dict:
    """
    Estimate how fast each guitar model sells based on Instagram post dates.

    Logic:
    - We know when GH *posted* a guitar (listing date)
    - We know if it was *sold* (is_sold flag)
    - Guitars posted more recently that are already sold → sold fast
    - We cluster posts by model, compute avg days-to-sell proxy

    Since we don't have an exact sold date, we approximate:
    - Sold post date = listing date (Instagram post = sold announcement)
    - Unsold post date = today (still available)
    - "Days to sell" = difference between listing date and sold date of similar model

    NOTE: GH almost never marks posts as SOLD explicitly. Posts older than
    SOLD_ASSUMPTION_DAYS without a SOLD marker are treated as effectively sold
    (GH sells ~90%+ of inventory; old unlabeled posts are sold, not stale).

    Returns:
        dict mapping normalized_model_key → {
            avg_days_to_sell: float,
            sell_rate: float,      # 0.0-1.0, higher = sells faster
            count_sold: int,
            count_total: int,
        }
    """
    today = datetime.now().date()
    clusters: dict = {}

    for row in history:
        title = row.get("title", "").strip()
        if not title:
            continue

        # Parse listing date
        date_str = row.get("date", "")
        try:
            post_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue

        days_ago = (today - post_date).days
        is_sold = row.get("is_sold", False)
        # GH rarely uses SOLD marker — treat old posts as effectively sold
        effectively_sold = is_sold or (days_ago > SOLD_ASSUMPTION_DAYS)
        days_held = 0 if effectively_sold else days_ago

        norm = _normalize(title)
        # Use first 4 words as cluster key (model family)
        key = " ".join(norm.split()[:4])

        if key not in clusters:
            clusters[key] = {
                "titles":      [],
                "days_held":   [],
                "sold_count":  0,
                "total_count": 0,
            }

        clusters[key]["titles"].append(title)
        clusters[key]["days_held"].append(days_held)
        clusters[key]["total_count"] += 1
        if effectively_sold:
            clusters[key]["sold_count"] += 1

    scores = {}
    for key, cl in clusters.items():
        if cl["total_count"] < 2:
            continue

        avg_days = sum(cl["days_held"]) / len(cl["days_held"])
        sell_rate = cl["sold_count"] / cl["total_count"]

        # sell_rate × (1 / avg_days) → higher = sells fast and often
        # Normalize to 0-1 range with a soft cap at 180 days
        speed_score = sell_rate * (1 - min(avg_days, 180) / 180)

        scores[key] = {
            "avg_days_to_sell": round(avg_days, 1),
            "sell_rate":        round(sell_rate, 2),
            "speed_score":      round(speed_score, 3),
            "count_sold":       cl["sold_count"],
            "count_total":      cl["total_count"],
            "example_title":    cl["titles"][0],
        }

    return scores


def get_liquidity(title: str, scores: dict, threshold: int = 70,
                  ph=None) -> Optional[dict]:
    """
    Look up liquidity score for a given guitar title.

    Priority:
    1. Real GH web sell data (ph.get_gh_liquidity) — actual days-on-market
    2. Instagram history approximation (scores dict) — fallback when DB has < 2 points

    Returns dict with avg_days_to_sell, sell_rate, count_sold, count_total.
    """
    # Priority 1: real data from GH web tracking
    if ph is not None:
        real = ph.get_gh_liquidity(title)
        if real is not None:
            return real

    # Priority 2: Instagram approximation
    if not scores:
        return None

    norm = _normalize(title)
    key4 = " ".join(norm.split()[:4])

    if key4 in scores:
        return scores[key4]

    best_score = 0
    best_key   = None
    for k in scores:
        s = fuzz.token_set_ratio(key4, k)
        if s > best_score:
            best_score = s
            best_key   = k

    if best_score >= threshold and best_key:
        return scores[best_key]

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Quick CLI test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    catalog = load_sold_catalog()
    print(f"Sold catalog: {len(catalog)} entries")

    if catalog:
        targets = build_proactive_targets(catalog)
        print(f"\nTop proactive targets ({len(targets)}):")
        for t in targets[:20]:
            print(f"  [{t['count']}x] ${t['avg_price_usd']:,.0f} USD  {t['title']}")
            print(f"       Query: {t['search_query']}")

        # Test reactive lookup
        test = "Gibson Les Paul Standard 50s"
        result = find_gh_historical_price(test, catalog)
        if result:
            print(f"\nReactive test '{test}':")
            print(f"  Match: {result['match_title']} (score {result['score']})")
            print(f"  Price: ${result['price_usd']:,.0f} USD")
        else:
            print(f"\nNo match for '{test}'")
    else:
        print("No data yet — run instagram_fetch.py + instagram_parse.py first.")
