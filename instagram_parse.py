"""
instagram_parse.py
──────────────────
Parses the JSON files downloaded by instagram_fetch.py and produces:
  - gh_instagram_history.csv   (all posts with parsed price + sold status)
  - gh_sold_catalog.csv        (only confirmed sold guitars, for use as MX price benchmark)

Usage:
    python instagram_parse.py

Output columns (gh_instagram_history.csv):
    shortcode, date, title, price_mxn, price_usd, is_sold, caption_preview, url
"""

import os
import re
import csv
import json
from typing import Optional, Tuple

POSTS_DIR    = os.path.join("ig_posts", "guitarshome")
HISTORY_CSV  = "gh_instagram_history.csv"
SOLD_CSV     = "gh_sold_catalog.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

# Emoji ranges for stripping
_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "\U00002600-\U000026FF"
    "\u2764\u2665\u2666\u2663\u2660"
    "]+",
    flags=re.UNICODE,
)


def _clean_line(line: str) -> str:
    line = _EMOJI_RE.sub("", line)
    line = re.sub(r"#\w+", "", line)          # strip hashtags
    line = re.sub(r"@\w+", "", line)          # strip mentions
    line = re.sub(r"https?://\S+", "", line)  # strip URLs
    line = re.sub(r"\s+", " ", line).strip()
    return line


def _clean_number(raw: str) -> Optional[float]:
    """Parse '85,000' or '85.000' or '85000' → 85000.0"""
    raw = raw.strip().replace(",", "").replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


def extract_prices(caption: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (price_mxn, price_usd).
    Tries explicit currency markers first, then infers MXN from magnitude.
    """
    text = caption.lower()
    price_mxn = None
    price_usd = None

    # USD — explicit markers
    usd_m = re.search(
        r"\$\s*([\d]{1,3}(?:[,\.]\d{3})*|\d+)\s*(?:usd|dlls?|dollars?)",
        text,
    )
    if usd_m:
        price_usd = _clean_number(usd_m.group(1))

    # MXN — explicit markers
    mxn_m = re.search(
        r"\$\s*([\d]{1,3}(?:[,\.]\d{3})*|\d+)\s*(?:mxn|pesos?|mn\b)",
        text,
    )
    if mxn_m:
        price_mxn = _clean_number(mxn_m.group(1))

    # Bare $ — infer currency from magnitude (guitars in MX range 10k-500k MXN)
    if price_mxn is None and price_usd is None:
        bare_m = re.search(
            r"\$\s*([\d]{1,3}(?:[,\.]\d{3})*|\d+)",
            text,
        )
        if bare_m:
            amount = _clean_number(bare_m.group(1))
            if amount is not None:
                if amount >= 5_000:
                    price_mxn = amount   # likely MXN
                elif 500 <= amount < 5_000:
                    price_usd = amount   # likely USD

    # Sanity bounds
    if price_mxn is not None and not (5_000 <= price_mxn <= 2_000_000):
        price_mxn = None
    if price_usd is not None and not (300 <= price_usd <= 100_000):
        price_usd = None

    return price_mxn, price_usd


def extract_title(caption: str) -> str:
    """
    The guitar name is usually in the first 1-3 lines of the caption,
    after the Instagram username and timestamp header.

    Caption format from @guitarshome:
        guitarshome

        2d
        Gibson Les Paul Standard '50s P-90 – Tobacco Burst 2023
        *** $55,999 ***
        ...
    """
    if not caption:
        return ""

    lines = caption.strip().split("\n")
    title_parts = []

    for line in lines[:10]:
        cleaned = _clean_line(line)
        if not cleaned:
            continue

        # Skip the Instagram username line
        if cleaned.lower() in {"guitarshome", "guitars home", "guitar's home"}:
            continue

        # Skip timestamp lines (1h, 2d, 3w, 1m, etc.)
        if re.match(r"^\d+[smhdw]$", cleaned):
            continue

        # Stop when we hit a price line
        if re.search(r"\$[\d,\.]|\*\*\*", cleaned):
            break
        # Stop at sold indicator
        if re.search(r"\bvendid[oa]\b|\bsold\b|\bno disponible\b", cleaned, re.I):
            break
        # Stop at separator lines or URLs
        if re.match(r"^[-_─=•·*]+$", cleaned):
            break
        if cleaned.lower().startswith(("http", "visita", "www.")):
            break

        title_parts.append(cleaned)

        # Two meaningful lines is enough for a guitar name
        if len(title_parts) >= 2:
            break

    return " – ".join(title_parts).strip()


def is_sold(caption: str) -> bool:
    patterns = [
        r"\*+\s*sold\s*\*+",       # *** SOLD *** (GH format)
        r"\bvendid[oa]\b",
        r"\bsold\b",
        r"\bya fue\b",
        r"\bno disponible\b",
        r"\bno\s+disponible\b",
        r"\bvendida\b",
        r"\bagotad[oa]\b",
    ]
    text = caption.lower()
    return any(re.search(p, text) for p in patterns)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def parse_all() -> None:
    if not os.path.isdir(POSTS_DIR):
        raise SystemExit(
            f"No se encontró {POSTS_DIR}/\n"
            "Corre primero: python instagram_fetch.py"
        )

    post_files = sorted(
        [f for f in os.listdir(POSTS_DIR) if f.endswith(".json")],
        reverse=True,  # newest first
    )

    if not post_files:
        raise SystemExit(f"No hay posts en {POSTS_DIR}/")

    print(f"Parseando {len(post_files)} posts...")

    all_rows  = []
    sold_rows = []

    for fname in post_files:
        path = os.path.join(POSTS_DIR, fname)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        caption   = data.get("caption", "")
        shortcode = data.get("shortcode", fname[:-5])
        date      = data.get("date", "")[:10]
        url       = data.get("url", f"https://www.instagram.com/p/{shortcode}/")

        title              = extract_title(caption)
        price_mxn, price_usd = extract_prices(caption)
        sold               = is_sold(caption)
        preview            = caption[:80].replace("\n", " ")

        row = {
            "shortcode":    shortcode,
            "date":         date,
            "title":        title,
            "price_mxn":    int(price_mxn) if price_mxn else "",
            "price_usd":    int(price_usd) if price_usd else "",
            "is_sold":      "SI" if sold else "NO",
            "caption_preview": preview,
            "url":          url,
        }

        all_rows.append(row)
        if sold and (price_mxn or price_usd):
            sold_rows.append(row)

    # ── Write full history ────────────────────────────────────────────────────
    fieldnames = ["shortcode", "date", "title", "price_mxn", "price_usd",
                  "is_sold", "caption_preview", "url"]

    with open(HISTORY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    # ── Write sold-only catalog ───────────────────────────────────────────────
    with open(SOLD_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sold_rows)

    with_price = sum(1 for r in all_rows if r['price_mxn'] or r['price_usd'])
    sold_any   = sum(1 for r in all_rows if r['is_sold'] == 'SI')

    print(f"\nResultados:")
    print(f"  Total posts parseados : {len(all_rows)}")
    print(f"  Con precio detectado  : {with_price}  ← benchmark MX")
    print(f"  Marcados como SOLD    : {sold_any} (GH elimina el precio al marcar SOLD)")
    print(f"  SOLD + con precio     : {len(sold_rows)}")
    print(f"\n  {HISTORY_CSV}  ← benchmark principal (908 precios reales en MXN)")
    print(f"  {SOLD_CSV}  ← vendidos con precio (referencia secundaria)")


if __name__ == "__main__":
    parse_all()
