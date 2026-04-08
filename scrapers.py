import re
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Set, Optional

from matching import has_red_flags, detect_aging_tier, detect_brazilian, detect_flame_top

# ─────────────────────────────────────────────────────────────────────────────
# Shared Constants
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

REQUEST_TIMEOUT        = 30
REQUEST_DELAY_SECONDS  = 0.7
MAX_RETRIES            = 2
MIN_PRICE_USD          = 400.0   # Reject accessories, parts, straps, etc.

ACCESSORY_KEYWORDS = {
    "gig bag", "tremolo arm", "whammy bar",
    # Non-guitar gear that leaks through mixed collections (CME price-drops, etc.)
    "amplifier", "amp head", "amp combo", "combo amp", "guitar combo",
    "guitar amp", "bass amp", "bass head", "bass combo",
    "speaker cabinet", "speaker cab",
    "preamp", "power amp", "attenuator",
    "pedalboard", "power supply",
    "lap steel", "steel guitar", "pedal steel",
    "banjo", "mandolin", "ukulele", "violin", "cello",
    "drum", "cymbal", "snare", "hi-hat",
    "microphone", "audio interface", "mixer",
    # Speaker cab sizes (catches "1x12 Combo", "4x12 Cabinet", etc.)
    "1x8", "1x10", "1x12", "2x10", "2x12", "4x10", "4x12",
    # Wattage in title = amp, not guitar
    "-watt",
}


# ─────────────────────────────────────────────────────────────────────────────
# Generic Helpers
# ─────────────────────────────────────────────────────────────────────────────

def safe_get(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
            print(f"  [WARN] {url} → {r.status_code}")
            if r.status_code == 404:
                return None
        except requests.RequestException as e:
            print(f"  [ERROR] GET {url} (attempt {attempt + 1}): {e}")
        time.sleep(1)
    return None


def safe_get_json(
    url: str,
    params: dict = None,
    extra_headers: dict = None,
    retries: int = MAX_RETRIES,
) -> Optional[dict]:
    req_headers = HEADERS.copy()
    if extra_headers:
        req_headers.update(extra_headers)
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=req_headers, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            print(f"  [WARN] {url} → {r.status_code}")
            if r.status_code in (404, 403):
                return None
            if r.status_code == 429:
                print("  [WARN] Rate limited — waiting 15 s…")
                time.sleep(15)
                continue
        except requests.RequestException as e:
            print(f"  [ERROR] GET {url} (attempt {attempt + 1}): {e}")
        except ValueError as e:
            print(f"  [ERROR] JSON parse {url}: {e}")
            return None
        time.sleep(2)
    return None


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def extract_price_numbers(text: str) -> List[float]:
    """Return all decimal prices found in text (e.g. 1,299.00 → 1299.0)."""
    prices = []
    for m in re.findall(r"[\d,]+\.\d{2}", text):
        try:
            prices.append(float(m.replace(",", "")))
        except ValueError:
            pass
    return prices


def extract_usd_price(text: str) -> Optional[float]:
    """Return the last $X,XXX.XX price found (typically the displayed price)."""
    matches = re.findall(r"\$[\d,]+(?:\.\d{2})?", text)
    if not matches:
        return None
    try:
        return float(matches[-1].replace("$", "").replace(",", ""))
    except ValueError:
        return None


_AMP_RE = re.compile(r'\b\d+[\s-]?watt\b|\b\d+x\d+["\s]', re.I)


def is_accessory_title(title: str) -> bool:
    low = title.lower()
    if any(kw in low for kw in ACCESSORY_KEYWORDS):
        return True
    if _AMP_RE.search(low):
        return True
    return False


# Condition grades in descending order (Mint → Poor)
# Used to filter and score listings.
CONDITION_MAP = [
    ("mint",          "Mint"),
    ("pristine",      "Mint"),
    ("near mint",     "Excellent"),
    ("near-mint",     "Excellent"),
    ("nm",            "Excellent"),
    ("excellent",     "Excellent"),
    ("very good+",    "VG+"),
    ("very good plus","VG+"),
    ("vg+",           "VG+"),
    ("vg +",          "VG+"),
    ("very good",     "VG"),
    ("vg",            "VG"),
    ("good+",         "Good+"),
    ("good plus",     "Good+"),
    ("g+",            "Good+"),
    ("good",          "Good"),
    ("fair",          "Fair"),
    ("poor",          "Poor"),
    ("as-is",         "Fair"),
    ("as is",         "Fair"),
]

CONDITION_SCORE = {
    "Mint":      1.00,
    "Excellent": 0.95,
    "VG+":       0.85,
    "VG":        0.75,
    "Good+":     0.60,
    "Good":      0.50,
    "Fair":      0.30,
    "Poor":      0.10,
}

# Minimum acceptable condition — guitars below this are skipped
MIN_CONDITION = "VG"
MIN_CONDITION_SCORE = CONDITION_SCORE[MIN_CONDITION]


def parse_condition(text: str) -> str:
    """
    Extract condition grade from description or title text.
    Returns one of: Mint, Excellent, VG+, VG, Good+, Good, Fair, Poor, or "".
    """
    if not text:
        return ""
    low = text.lower()
    for keyword, grade in CONDITION_MAP:
        if keyword in low:
            return grade
    return ""


# Keywords that indicate the guitar has been modified (lowers collectible value).
_MOD_KEYWORDS = [
    "replaced pickup", "replaced pickups", "pickup swap", "pickups swapped",
    "changed pickup", "changed pickups", "upgraded pickup", "upgraded pickups",
    "refret", "refretted", "re-fret", "hardware swap", "swapped hardware",
    "modified", "modifications", "non-original", "non original",
]

# Keywords that indicate the COA is missing.
_NO_COA_KEYWORDS = [
    "no coa", "no certificate", "without certificate", "missing certificate",
    "certificate not included", "no cert", "coa not included",
]


def parse_guitar_specs(title: str, description: str = "") -> dict:
    """
    Extract price-relevant spec attributes from a guitar title and description.

    Returns a dict with:
      aging_tier   — int 0-5 or None (from detect_aging_tier)
      flame_top    — 'figured', 'plain', or None
      has_brazilian — bool
      has_mods     — bool  (pickups replaced, refret, hardware swap)
      no_coa       — bool  (certificate of authenticity absent)
    """
    combined = (title + " " + description).lower()

    # Aging tier and tonewoods are reliably declared in the title
    aging_tier    = detect_aging_tier(title)
    has_brazilian = detect_brazilian(title)
    flame_top     = detect_flame_top(title)

    # Mods and COA are typically mentioned in the description
    has_mods = any(kw in combined for kw in _MOD_KEYWORDS)
    no_coa   = any(kw in combined for kw in _NO_COA_KEYWORDS)

    return {
        "aging_tier":    aging_tier,
        "flame_top":     flame_top,
        "has_brazilian": has_brazilian,
        "has_mods":      has_mods,
        "no_coa":        no_coa,
    }


def build_woocommerce_paginated_url(base_url: str, page: int) -> str:
    base_url = base_url.rstrip("/")
    return base_url + "/" if page == 1 else f"{base_url}/page/{page}/"


# ─────────────────────────────────────────────────────────────────────────────
# Generic Shopify JSON Scraper
# Used by: Dave's, Chicago Music Exchange, Cream City Music
# ─────────────────────────────────────────────────────────────────────────────

def scrape_shopify_store(
    base_url: str,
    collection_handles: List[str],
    source_name: str,
    min_price_usd: float = MIN_PRICE_USD,
) -> List[Dict]:
    """
    Scrapes a Shopify store via its products.json API.
    Detects sale prices via compare_at_price > price.
    Filters structural red flags from product descriptions.
    Tags on-sale items for priority review.
    """
    all_items: List[Dict] = []
    seen_handles: Set[str] = set()

    print(f"\n=== Scrapeando {source_name} (Shopify JSON) ===")

    for collection_handle in collection_handles:
        print(f"\n[{source_name}] Colección: {collection_handle}")
        page = 1

        while True:
            url = (
                f"{base_url}/collections/{collection_handle}"
                f"/products.json?limit=250&page={page}"
            )
            data = safe_get_json(url)

            if not data:
                print(f"  [{source_name}] p{page}: sin datos, fin.")
                break

            products = data.get("products", [])
            if not products:
                print(f"  [{source_name}] p{page}: vacío, fin.")
                break

            new_count = 0
            for product in products:
                handle = product.get("handle", "")
                if handle in seen_handles:
                    continue
                seen_handles.add(handle)

                title = normalize_whitespace(product.get("title", "").strip())
                if not title or is_accessory_title(title):
                    continue

                # Scan description for structural damage keywords
                body_html = product.get("body_html") or ""
                try:
                    description = BeautifulSoup(body_html, "lxml").get_text(" ", strip=True)
                except Exception:
                    description = ""

                if has_red_flags(title, description):
                    print(f"  [{source_name}] RED FLAG → {title[:60]}")
                    continue

                # Parse condition from description (Dave's/CME: "Excellent Condition, ...")
                condition = parse_condition(description) or parse_condition(title)

                # Skip unacceptable condition — not worth buying in Fase 1
                if condition and CONDITION_SCORE.get(condition, 1.0) < MIN_CONDITION_SCORE:
                    continue

                # Parse price-relevant spec attributes (aging, tonewoods, COA, mods)
                specs = parse_guitar_specs(title, description)

                # First product image (for BUY NOW Telegram alerts)
                images = product.get("images") or []
                image_url = images[0].get("src", "") if images else ""

                product_url = f"{base_url}/products/{handle}"

                # First available variant with a valid price
                for variant in product.get("variants", []):
                    # Skip sold-out / unavailable variants
                    if variant.get("available") is False:
                        continue

                    try:
                        price       = float(variant.get("price") or 0)
                        compare_raw = variant.get("compare_at_price")
                        compare_at  = float(compare_raw) if compare_raw else None
                    except (ValueError, TypeError):
                        continue

                    if price < min_price_usd:
                        continue

                    on_sale      = compare_at is not None and compare_at > price
                    discount_pct = (
                        round((compare_at - price) / compare_at * 100, 1)
                        if on_sale else 0.0
                    )

                    all_items.append({
                        "source":              source_name,
                        "title":               title,
                        "description":         description,
                        "image_url":           image_url,
                        "price_usd":           price,
                        "original_price_usd":  compare_at,
                        "on_sale":             on_sale,
                        "discount_pct":        discount_pct,
                        "url":                 product_url,
                        "condition":           condition,
                        **specs,
                    })
                    new_count += 1
                    break  # one variant per product

            print(f"  [{source_name}] p{page}: {new_count} nuevos")

            if len(products) < 250:
                break

            page += 1
            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[{source_name}] Total: {len(all_items)}")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Guitar's Home  (WooCommerce, MX)
# ─────────────────────────────────────────────────────────────────────────────

GH_CATEGORY_URLS = [
    {"category": "premium",   "url": "https://guitars-home.com/categoria-producto/guitarras-premium"},
    {"category": "electricas","url": "https://guitars-home.com/categoria-producto/guitarras-electricas"},
]


def parse_guitarshome_listing_page(html: str, category: str, page: int) -> List[Dict]:
    soup  = BeautifulSoup(html, "lxml")
    items = []

    for product in soup.select("li.product"):
        title_el = product.select_one(".woocommerce-loop-product__title, h2")
        link_el  = product.select_one("a")
        price_el = product.select_one(".price")

        if not title_el or not link_el or not price_el:
            continue

        title      = normalize_whitespace(title_el.get_text(" ", strip=True))
        url        = link_el.get("href", "").strip()
        price_text = normalize_whitespace(price_el.get_text(" ", strip=True))

        # WooCommerce shows struck-through original + sale price
        prices = extract_price_numbers(price_text)
        if not title or not url or not prices:
            continue

        # If two prices found, the last is the sale price (lower)
        price_mxn      = prices[-1]
        original_mxn   = prices[0] if len(prices) > 1 and prices[0] > prices[-1] else None
        on_sale        = original_mxn is not None

        items.append({
            "source":             "guitarshome",
            "category":           category,
            "page":               page,
            "title":              title,
            "price_mxn":          price_mxn,
            "original_price_mxn": original_mxn,
            "on_sale":            on_sale,
            "url":                url,
        })

    return items


def fetch_gh_product_description(url: str) -> str:
    """
    Fetch the individual GH product page and extract the specs description.
    GH specs are in plain text paragraphs with "Label: Value" format inside
    the WooCommerce product description area.
    Returns the full description text, or "" on failure.
    """
    response = safe_get(url)
    if not response:
        return ""
    try:
        soup = BeautifulSoup(response.text, "lxml")
        # WooCommerce product description — try multiple selectors
        desc_el = (
            soup.select_one(".woocommerce-product-details__short-description")
            or soup.select_one(".product_meta + div")
            or soup.select_one(".entry-content")
            or soup.select_one(".description")
        )
        if desc_el:
            return desc_el.get_text(" ", strip=True)
    except Exception:
        pass
    return ""


def scrape_guitarshome(max_pages_per_category: int = 100) -> List[Dict]:
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    print("\n=== Scrapeando Guitar's Home ===")

    for cat_info in GH_CATEGORY_URLS:
        category = cat_info["category"]
        base_url = cat_info["url"]
        print(f"\n[GH] Categoría: {category}")

        for page in range(1, max_pages_per_category + 1):
            url      = build_woocommerce_paginated_url(base_url, page)
            response = safe_get(url)
            if not response:
                break

            page_items = parse_guitarshome_listing_page(response.text, category, page)
            if not page_items:
                break

            new_count = 0
            for item in page_items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                all_items.append(item)
                new_count += 1

            print(f"  [GH] p{page}: {new_count} nuevos")
            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY_SECONDS)

    # Fetch individual product pages for descriptions/specs.
    # GH has ~69 active items — this adds ~69 requests but gives us full specs
    # (body wood, pickups, neck, finish details) for much better matching.
    print(f"\n[GH] Fetching descriptions for {len(all_items)} products...")
    for i, item in enumerate(all_items):
        desc = fetch_gh_product_description(item["url"])
        item["description"] = desc
        if (i + 1) % 10 == 0:
            print(f"  [GH] {i + 1}/{len(all_items)} descriptions fetched")
        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[GH] Total: {len(all_items)} (with descriptions)")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Dave's Guitar Shop  (Shopify, US)
# Upgraded to Shopify JSON API for reliable sale-price detection.
# ─────────────────────────────────────────────────────────────────────────────

DAVES_BASE = "https://www.davesguitar.com"
DAVES_COLLECTION_HANDLES = [
    "used-electrics",
    "gibson",
    "gibson-custom-shop",
    "fender",
    "fender-custom-shop",
    "paul-reed-smith",
    "paul-reed-smith-wood-library",
    "paul-reed-smith-private-stock",
    "collings-electrics",
    "ernie-ball-music-man",
    "suhr",
    "misc-electrics",
    "jackson-electrics",
    "lefty-electric",
    "esp-ltd-electric",
]


def scrape_daves() -> List[Dict]:
    return scrape_shopify_store(DAVES_BASE, DAVES_COLLECTION_HANDLES, "daves")


# ─────────────────────────────────────────────────────────────────────────────
# Wildwood Guitars  (Shopify, US — Boulder, CO)
# Famous for "Wildwood Spec" exclusive runs with Gibson/Fender Custom Shop.
# These are boutique dealer exclusives — higher sell price in MX market.
# ─────────────────────────────────────────────────────────────────────────────

WILDWOOD_BASE = "https://www.wildwoodguitars.com"
WILDWOOD_COLLECTION_HANDLES = [
    "used-guitars",
    "pre-owned-guitars",
    "used-electric-guitars",
    "clearance-guitars",
]


def scrape_wildwood() -> List[Dict]:
    return scrape_shopify_store(WILDWOOD_BASE, WILDWOOD_COLLECTION_HANDLES, "wildwood")


# ─────────────────────────────────────────────────────────────────────────────
# Graysons Tune Town  (Shopify, US — Thousand Oaks, CA)
# Boutique dealer with a Custom Shop Showroom.
# Used guitars are mixed into general collections (no dedicated "used" handle),
# so we filter post-fetch to items with "Used" in the title.
# ─────────────────────────────────────────────────────────────────────────────

GRAYSONS_BASE = "https://www.graysonstunetown.com"
GRAYSONS_COLLECTION_HANDLES = [
    "custom-shop-showroom",   # 68 custom shop / boutique guitars
    "guitars",                # general guitar collection (may include used)
]


def scrape_graysons() -> List[Dict]:
    """
    Scrape Graysons Tune Town via Shopify JSON.
    Since they don't have a dedicated used collection, we post-filter:
      - Items with "Used" in the title are included regardless of condition tag.
      - New Custom Shop items are also included (boutique prices can still create
        a MX arbitrage margin vs GH listings).
    """
    raw = scrape_shopify_store(GRAYSONS_BASE, GRAYSONS_COLLECTION_HANDLES, "graysons")
    # Keep items that are either explicitly used OR have no condition label
    # (Custom Shop new items are still relevant for price comparison)
    return [item for item in raw if
            "used" in item["title"].lower() or
            item.get("condition") in ("", None, "Excellent", "VG+", "VG", "Mint")]


# ─────────────────────────────────────────────────────────────────────────────
# Twin Town Guitars  (Shopify, US — Minneapolis, MN)
# Used guitars are mixed into the all-guitars collection.
# Filter post-fetch by "USED" in title (their standard labeling).
# ─────────────────────────────────────────────────────────────────────────────

TWINTOWN_BASE = "https://www.twintownguitars.com"
TWINTOWN_COLLECTION_HANDLES = [
    "all-guitars",    # all guitars incl. used (labeled "USED" in title)
    "bass-guitars",   # used basses mixed in here too
]


def scrape_twin_town() -> List[Dict]:
    raw = scrape_shopify_store(TWINTOWN_BASE, TWINTOWN_COLLECTION_HANDLES, "twin_town")
    # Twin Town labels used items as "USED" in the title — filter to those only
    return [item for item in raw if "used" in item["title"].lower()]


# ─────────────────────────────────────────────────────────────────────────────
# NOTE: Musicians Friend + Music and Arts — NOT ADDED
# Both are Guitar Center family companies running Salesforce Commerce Cloud
# (proprietary platform, same bot-detection issues as Guitar Center).
# Guitar Center is already disabled in this system for the same reason.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Chicago Music Exchange  (Shopify, US)
# Focuses on used electric guitars and their Price Drop collection.
# ─────────────────────────────────────────────────────────────────────────────

CME_BASE = "https://www.chicagomusicexchange.com"
# vintage-used  = dedicated used/vintage collection (~825 available)
# price-drops   = all-category discounted gear (~1,358 available)
# Both are filtered by tag to keep only guitars (not basses, acoustics, accessories)
CME_COLLECTION_HANDLES = [
    "vintage-used",
    "price-drops",
]
# Only accept products tagged as used/vintage inventory
CME_USED_TAGS = {"vintage / used inventory", "used inventory", "new-to-used"}


def scrape_cme() -> List[Dict]:
    """
    CME's 'electric-guitars' collection mixes new + used.
    Filter by 'Item Family' tag to keep only used/vintage inventory.
    'price-drops' is all-category but already pre-filtered by CME for deals.
    """
    all_items: List[Dict] = []
    seen_handles: set = set()
    print("\n=== Scrapeando cme (Shopify JSON) ===")

    for collection_handle in CME_COLLECTION_HANDLES:
        print(f"\n[cme] Colección: {collection_handle}")
        page = 1
        while True:
            url = (
                f"{CME_BASE}/collections/{collection_handle}"
                f"/products.json?limit=250&page={page}"
            )
            data = safe_get_json(url)
            if not data:
                print(f"  [cme] p{page}: sin datos, fin.")
                break
            products = data.get("products", [])
            if not products:
                print(f"  [cme] p{page}: vacío, fin.")
                break

            new_count = 0
            for product in products:
                handle = product.get("handle", "")
                if handle in seen_handles:
                    continue

                # For electric-guitars collection, skip brand-new inventory
                tags_lower = {t.lower() for t in product.get("tags", [])}
                if collection_handle == "electric-guitars":
                    if not tags_lower & CME_USED_TAGS:
                        continue  # brand new, skip

                seen_handles.add(handle)
                title = normalize_whitespace(product.get("title", "").strip())
                if not title or is_accessory_title(title):
                    continue

                body_html = product.get("body_html") or ""
                try:
                    description = BeautifulSoup(body_html, "lxml").get_text(" ", strip=True)
                except Exception:
                    description = ""

                if has_red_flags(title, description):
                    print(f"  [cme] RED FLAG → {title[:60]}")
                    continue

                # Parse condition from description (CME describes condition in the body text)
                condition = parse_condition(description) or parse_condition(title)
                if condition and CONDITION_SCORE.get(condition, 1.0) < MIN_CONDITION_SCORE:
                    continue

                specs = parse_guitar_specs(title, description)

                images = product.get("images") or []
                image_url = images[0].get("src", "") if images else ""

                product_url = f"{CME_BASE}/products/{handle}"
                for variant in product.get("variants", []):
                    if variant.get("available") is False:
                        continue
                    try:
                        price = float(variant.get("price") or 0)
                        compare_raw = variant.get("compare_at_price")
                        compare_at = float(compare_raw) if compare_raw else None
                    except (ValueError, TypeError):
                        continue
                    if price < MIN_PRICE_USD:
                        continue
                    on_sale = compare_at is not None and compare_at > price
                    discount_pct = (
                        round((compare_at - price) / compare_at * 100, 1) if on_sale else 0.0
                    )
                    all_items.append({
                        "source": "cme", "title": title,
                        "description": description,
                        "image_url": image_url,
                        "price_usd": price, "original_price_usd": compare_at,
                        "on_sale": on_sale, "discount_pct": discount_pct,
                        "url": product_url, "condition": condition,
                        **specs,
                    })
                    new_count += 1
                    break

            print(f"  [cme] p{page}: {new_count} nuevos")
            if new_count == 0 and page > 1:
                break
            page += 1
            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[cme] Total: {len(all_items)}")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Cream City Music  (BigCommerce, US — Milwaukee, WI)
# Uses BigCommerce category URLs — not Shopify JSON.
# ─────────────────────────────────────────────────────────────────────────────

CCM_BASE = "https://www.creamcitymusic.com"
CCM_CATEGORY_URLS = [
    "https://www.creamcitymusic.com/vintage-used/?category=electric-guitars",
    "https://www.creamcitymusic.com/electrics/",
]


def parse_ccm_page(html: str) -> List[Dict]:
    """
    Cream City Music uses BigCommerce stencil theme.
    Cards are <article class="card"> with:
      - <h3> for title
      - <a class="card-figure__link" aria-label="Title, $X,XXX.00"> for URL + price
      - Optional <span class="price price--rrp"> for original price (WAS price)
    """
    soup  = BeautifulSoup(html, "lxml")
    items = []

    cards = soup.select("article.card")

    for card in cards:
        # Title from h3
        title_el = card.select_one("h3")
        if not title_el:
            continue
        title = normalize_whitespace(title_el.get_text(" ", strip=True))
        if not title or is_accessory_title(title):
            continue
        if has_red_flags(title):
            continue

        # URL + current price from aria-label on the figure link
        link_el = card.select_one("a.card-figure__link") or card.select_one("a[href]")
        if not link_el:
            continue
        href = link_el.get("href", "")
        url  = href if href.startswith("http") else f"{CCM_BASE}{href}"

        aria = link_el.get("aria-label", "")
        current_price: Optional[float] = extract_usd_price(aria)

        # Original (was) price from strikethrough or rrp element
        strike_el = card.select_one("s") or card.select_one("del") or card.select_one(".price--rrp")
        original_price: Optional[float] = None
        if strike_el:
            original_price = extract_usd_price(strike_el.get_text())

        if not current_price or current_price < MIN_PRICE_USD:
            continue

        on_sale      = bool(original_price and original_price > current_price)
        discount_pct = (
            round((original_price - current_price) / original_price * 100, 1)
            if on_sale else 0.0
        )

        # Cream City doesn't show description on listing pages — parse condition from title
        condition = parse_condition(title)
        specs     = parse_guitar_specs(title)

        # Extract image URL from card
        img_el = card.select_one("img")
        cc_image_url = img_el.get("src", "") if img_el else ""

        items.append({
            "source":             "cream_city",
            "title":              title,
            "description":        "",
            "image_url":          cc_image_url,
            "price_usd":          current_price,
            "original_price_usd": original_price,
            "on_sale":            on_sale,
            "discount_pct":       discount_pct,
            "url":                url,
            "condition":          condition,
            **specs,
        })

    return items


def scrape_cream_city(max_pages: int = 20) -> List[Dict]:
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    print("\n=== Scrapeando Cream City Music ===")

    for base_url in CCM_CATEGORY_URLS:
        print(f"\n[cream_city] Categoría: {base_url}")

        for page in range(1, max_pages + 1):
            url      = f"{base_url}&page={page}" if "?" in base_url and page > 1 else (
                       f"{base_url}?page={page}" if page > 1 else base_url)
            response = safe_get(url)
            if not response:
                print(f"  [cream_city] p{page}: sin response, fin.")
                break

            page_items = parse_ccm_page(response.text)
            if not page_items:
                print(f"  [cream_city] p{page}: sin productos, fin.")
                break

            new_count = 0
            for item in page_items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                all_items.append(item)
                new_count += 1

            print(f"  [cream_city] p{page}: {new_count} nuevos")
            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY_SECONDS)

    # Fetch individual product descriptions for better matching.
    # Cream City has specs in "Label: Value" bullet format on product pages.
    print(f"\n[cream_city] Fetching descriptions for {len(all_items)} products...")
    for i, item in enumerate(all_items):
        resp = safe_get(item["url"])
        if resp:
            try:
                soup = BeautifulSoup(resp.text, "lxml")
                desc_el = (
                    soup.select_one("#tab-description")
                    or soup.select_one("[class*='productView-description']")
                    or soup.select_one(".product-description")
                )
                if desc_el:
                    item["description"] = desc_el.get_text(" ", strip=True)
            except Exception:
                pass
        if (i + 1) % 50 == 0:
            print(f"  [cream_city] {i + 1}/{len(all_items)} descriptions fetched")
        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[cream_city] Total: {len(all_items)} (with descriptions)")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Music Go Round  (BigCommerce — national used music chain)
# Products are embedded as JSON in a jsContext script variable.
# ─────────────────────────────────────────────────────────────────────────────

MGR_BASE = "https://www.musicgoround.com"
MGR_CATEGORY_URLS = [
    "https://www.musicgoround.com/guitars/electric-guitars",
]


def parse_mgr_page(html: str) -> List[Dict]:
    """
    Music Go Round uses BigCommerce with products embedded as JSON
    inside a jsContext script tag on the page.
    """
    import json as _json

    items = []

    # Try to extract embedded JSON product data from jsContext
    match = re.search(r'jsContext\s*=\s*JSON\.parse\((["\'])(.+?)\1\s*\)', html, re.S)
    products_raw = []

    if match:
        try:
            raw = match.group(2).encode().decode("unicode_escape")
            ctx = _json.loads(raw)
            products_raw = ctx.get("products", [])
        except Exception:
            pass

    # Fallback: look for products in a data attribute or window variable
    if not products_raw:
        alt = re.search(r'"products"\s*:\s*(\[.+?\])\s*[,}]', html, re.S)
        if alt:
            try:
                products_raw = _json.loads(alt.group(1))
            except Exception:
                pass

    # Fallback: parse HTML cards (BigCommerce stencil)
    # MGR uses <article class="card" data-name="..." data-product-price="599.99">
    if not products_raw:
        soup  = BeautifulSoup(html, "lxml")
        cards = soup.select("article.card") or soup.select("li.product article")
        for card in cards:
            # data-name is the full product name
            title = normalize_whitespace(card.get("data-name", ""))
            if not title:
                title_el = card.select_one("h3") or card.select_one("h4")
                title = normalize_whitespace(title_el.get_text(" ", strip=True)) if title_el else ""
            if not title or is_accessory_title(title) or has_red_flags(title):
                continue

            # Strip "Used Brand - Model..." prefix common in MGR titles
            title = re.sub(r"^Used\s+", "", title, flags=re.I).strip()

            link_el = card.select_one("a.card-figure__link") or card.select_one("a[href]")
            if not link_el:
                continue
            href = link_el.get("href", "")
            url  = href if href.startswith("http") else f"{MGR_BASE}{href}"

            # Price from data attribute
            price_raw = card.get("data-product-price", "")
            try:
                price = float(price_raw)
            except (ValueError, TypeError):
                price_el = card.select_one("[class*='price']")
                nums = extract_price_numbers(price_el.get_text()) if price_el else []
                price = min(nums) if nums else 0.0

            if not price or price < MIN_PRICE_USD:
                continue

            items.append({
                "source": "music_go_round", "title": title,
                "description": "", "image_url": "",
                "price_usd": price, "original_price_usd": None,
                "on_sale": False, "discount_pct": 0.0,
                "url": url, "condition": "used",
            })
        return items

    for p in products_raw:
        title = normalize_whitespace(str(p.get("name") or p.get("product_name") or ""))
        if not title or is_accessory_title(title) or has_red_flags(title):
            continue

        # Price: BigCommerce stores price as dict or float
        price_data    = p.get("price") or {}
        current_price: Optional[float] = None
        original_price: Optional[float] = None

        if isinstance(price_data, dict):
            current_price  = price_data.get("without_tax", {}).get("value") or \
                             price_data.get("value")
            sale_data      = p.get("sale_price") or {}
            if isinstance(sale_data, dict):
                sale_val = sale_data.get("without_tax", {}).get("value") or sale_data.get("value")
                if sale_val and sale_val < current_price:
                    original_price = current_price
                    current_price  = sale_val
        elif isinstance(price_data, (int, float)):
            current_price = float(price_data)

        if not current_price or current_price < MIN_PRICE_USD:
            continue

        url_part = p.get("url") or p.get("custom_url") or ""
        if isinstance(url_part, dict):
            url_part = url_part.get("url", "")
        url = url_part if url_part.startswith("http") else f"{MGR_BASE}{url_part}"

        on_sale      = bool(original_price and original_price > current_price)
        discount_pct = (
            round((original_price - current_price) / original_price * 100, 1)
            if on_sale else 0.0
        )

        items.append({
            "source":             "music_go_round",
            "title":              title,
            "description":        "", "image_url": "",
            "price_usd":          float(current_price),
            "original_price_usd": original_price,
            "on_sale":            on_sale,
            "discount_pct":       discount_pct,
            "url":                url,
            "condition":          "used",
        })

    return items


def scrape_music_go_round(max_pages: int = 20) -> List[Dict]:
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    print("\n=== Scrapeando Music Go Round ===")

    for base_url in MGR_CATEGORY_URLS:
        print(f"\n[MGR] Categoría: {base_url}")

        for page in range(1, max_pages + 1):
            url      = f"{base_url}?page={page}" if page > 1 else base_url
            response = safe_get(url)
            if not response:
                print(f"  [MGR] p{page}: sin response, fin.")
                break

            page_items = parse_mgr_page(response.text)
            if not page_items:
                print(f"  [MGR] p{page}: sin productos, fin.")
                break

            new_count = 0
            for item in page_items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                all_items.append(item)
                new_count += 1

            print(f"  [MGR] p{page}: {new_count} nuevos")
            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[MGR] Total: {len(all_items)}")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Elderly Instruments  (HTML — East Lansing, MI)
# Monitors "Used & Vintage" and "Sale" sections.
# NOTE: CSS selectors may need adjustment if the site changes.
# ─────────────────────────────────────────────────────────────────────────────

ELDERLY_BASE = "https://www.elderly.com"
ELDERLY_COLLECTION_HANDLES = [
    "electric-guitars",
    "on-sale-instruments",
    "recent-arrivals-as-is",
]


def scrape_elderly() -> List[Dict]:
    return scrape_shopify_store(ELDERLY_BASE, ELDERLY_COLLECTION_HANDLES, "elderly")


# ─────────────────────────────────────────────────────────────────────────────
# Norman's Rare Guitars  (Shopify, US — Tarzana, CA)
# All-vintage and used premium store. All products are available:True by default.
# Specializes in Gibson and Fender vintage/used — high match potential with GH.
# ─────────────────────────────────────────────────────────────────────────────

NORMANS_BASE = "https://normansrareguitars.com"
NORMANS_COLLECTION_HANDLES = [
    "electric-solid-body",
    "electric-semi-hollow-thin-body",
    "electric-hollow-body",
]


def scrape_normans() -> List[Dict]:
    return scrape_shopify_store(NORMANS_BASE, NORMANS_COLLECTION_HANDLES, "normans")


# ─────────────────────────────────────────────────────────────────────────────
# Tone Shop Guitars  (Shopify, US — Addison, TX)
# Used electric guitars + vintage instruments. Active used inventory.
# ─────────────────────────────────────────────────────────────────────────────

TONESHOP_BASE = "https://toneshopguitars.com"
TONESHOP_COLLECTION_HANDLES = [
    "used-electrics",
    "vintage",
]


def scrape_toneshop() -> List[Dict]:
    return scrape_shopify_store(TONESHOP_BASE, TONESHOP_COLLECTION_HANDLES, "toneshop")


# ─────────────────────────────────────────────────────────────────────────────
# Reverb  (active listings + sold price benchmark)
# ─────────────────────────────────────────────────────────────────────────────

REVERB_API_BASE = "https://reverb.com/api/listings"
REVERB_HEADERS  = {
    "Accept":         "application/hal+json",
    "Accept-Version": "3.0",
}

# Focused queries on brands/models that Guitar's Home typically carries
REVERB_SEARCH_QUERIES = [
    "gibson les paul standard",
    "gibson les paul custom",
    "gibson les paul traditional",
    "gibson sg standard",
    "gibson sg custom",
    "gibson es-335",
    "gibson es-339",
    "gibson flying v",
    "gibson explorer",
    "gibson firebird",
    "fender american stratocaster",
    "fender american telecaster",
    "fender jazzmaster",
    "fender jaguar",
    "prs custom 24",
    "prs custom 22",
    "prs mccarty",
    "prs ce 24",
    "prs hollowbody",
    "gretsch country gentleman",
    "gretsch white falcon",
    "gretsch electromatic",
    "music man axis",
    "music man luke",
    "music man silhouette",
    "suhr modern electric",
    "suhr classic electric",
    "collings i-35",
    "collings 290",
    "charvel san dimas",
]


def _dedupe_reverb_title(title: str) -> str:
    """
    Some Reverb sellers paste the title twice in the listing name.
    E.g. "Gibson ES-335 Vintage Ebony Gibson ES-335 Vintage Ebony 2022"
    Detect and remove the duplicate prefix, keeping the longer second half
    (which typically has the year or additional detail).
    """
    words = title.split()
    n = len(words)
    for split in range(n // 2, 2, -1):
        prefix = " ".join(words[:split]).lower()
        rest   = " ".join(words[split:]).lower()
        if rest.startswith(prefix):
            return " ".join(words[split:])
    return title


def parse_reverb_listing(listing: dict) -> Optional[Dict]:
    """Parse a single Reverb API listing dict into our unified schema."""
    try:
        title = _dedupe_reverb_title(listing.get("title", "").strip())
        if not title or is_accessory_title(title):
            return None

        # Build description from Reverb's structured fields + free-text description.
        # Many Reverb sellers use short titles ("Suhr Modern Plus") but put real specs
        # in the structured fields (make, model, year, finish) and description text.
        raw_desc = (listing.get("description") or "").strip()
        make     = (listing.get("make") or "").strip()
        model    = (listing.get("model") or "").strip()
        year     = str(listing.get("year") or "").strip()
        finish   = (listing.get("finish") or "").strip()

        # Enrich title with structured fields when the title is vague.
        # This ensures matching and red-flag checks see the real specs.
        enrichment_parts = []
        title_lower = title.lower()
        if make and make.lower() not in title_lower:
            enrichment_parts.append(make)
        if model and model.lower() not in title_lower:
            enrichment_parts.append(model)
        if year and year not in title:
            enrichment_parts.append(year)
        if finish and finish.lower() not in title_lower:
            enrichment_parts.append(finish)
        if enrichment_parts:
            title = title + " " + " ".join(enrichment_parts)
            title = normalize_whitespace(title)

        description = " | ".join(
            p for p in [f"{make} {model}".strip(), year, finish, raw_desc] if p
        )

        if has_red_flags(title, description):
            return None

        price_data = listing.get("price", {})
        if price_data.get("currency", "USD") != "USD":
            return None

        price_usd = float(price_data.get("amount", 0))
        if price_usd < MIN_PRICE_USD:
            return None

        if listing.get("state", {}).get("slug", "") not in ("live", ""):
            return None

        links = listing.get("_links", {})
        url   = links.get("web", {}).get("href", "")
        if not url:
            lid = listing.get("id", "")
            url = f"https://reverb.com/item/{lid}" if lid else ""
        if not url:
            return None

        condition = listing.get("condition", {}).get("display_name", "")

        # Extract first photo URL for BUY NOW alerts
        photos = listing.get("photos") or []
        if photos:
            photo_links = photos[0].get("_links", {})
            image_url = (photo_links.get("large_crop", {}).get("href", "")
                         or photo_links.get("full", {}).get("href", ""))
        else:
            image_url = ""

        return {
            "source":             "reverb",
            "title":              title,
            "description":        description,
            "image_url":          image_url,
            "price_usd":          price_usd,
            "original_price_usd": None,
            "on_sale":            False,   # Reverb used listings have one price
            "discount_pct":       0.0,
            "url":                url,
            "condition":          condition,
        }

    except (ValueError, TypeError, AttributeError):
        return None


def scrape_reverb_query(query: str, max_pages: int = 3) -> List[Dict]:
    items: List[Dict] = []

    for page in range(1, max_pages + 1):
        params = {
            "query":                    query,
            "condition[]":              "used",
            "ships_from_country_code":  "US",
            "per_page":                 50,
            "page":                     page,
        }
        data = safe_get_json(REVERB_API_BASE, params=params, extra_headers=REVERB_HEADERS)
        if not data:
            break

        listings = data.get("listings", [])
        if not listings:
            break

        parsed = [parse_reverb_listing(l) for l in listings]
        items.extend([i for i in parsed if i])
        print(f"  [Reverb] '{query}' p{page}: {sum(1 for i in parsed if i)} items")

        if page >= data.get("total_pages", 1):
            break

        time.sleep(REQUEST_DELAY_SECONDS)

    return items


def scrape_reverb(max_pages_per_query: int = 3,
                   extra_queries: List[str] = None) -> List[Dict]:
    """
    Scrape Reverb active US listings for target guitar brands/models.

    Args:
        extra_queries: additional search queries (e.g. built from GH titles)
                       to supplement the static REVERB_SEARCH_QUERIES list.
    """
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    # Combine static + dynamic queries, deduplicate
    queries = list(REVERB_SEARCH_QUERIES)
    if extra_queries:
        existing = {q.lower().strip() for q in queries}
        for eq in extra_queries:
            eq_clean = eq.lower().strip()
            if eq_clean and eq_clean not in existing:
                queries.append(eq)
                existing.add(eq_clean)

    static_set = {q.lower().strip() for q in REVERB_SEARCH_QUERIES}

    print(f"\n=== Scrapeando Reverb (active listings) — {len(queries)} queries "
          f"({len(REVERB_SEARCH_QUERIES)} static + "
          f"{len(queries) - len(REVERB_SEARCH_QUERIES)} dynamic) ===")

    for query in queries:
        # Dynamic queries (from GH titles) use 1 page only to limit API load.
        # Static queries use full max_pages_per_query (default 3).
        is_dynamic = query.lower().strip() not in static_set
        pages = 1 if is_dynamic else max_pages_per_query
        print(f"\n[Reverb] Query: '{query}'" + (" (dynamic)" if is_dynamic else ""))
        items = scrape_reverb_query(query, max_pages=pages)

        new_count = 0
        for item in items:
            if item["url"] in seen_urls:
                continue
            seen_urls.add(item["url"])
            all_items.append(item)
            new_count += 1

        print(f"  [Reverb] Únicos nuevos: {new_count}")
        time.sleep(1.5)

    print(f"\n[Reverb] Total activos únicos: {len(all_items)}")
    return all_items


def fetch_reverb_sold_avg(
    query: str,
    max_results: int = 20,
    gh_reverb_shop: str = "",
) -> Optional[float]:
    """
    Fetch the average recent sold price on Reverb for a guitar query.
    Uses a trimmed mean (removes bottom+top ~15 %) to filter outliers.

    If gh_reverb_shop is provided (Guitar's Home shop slug on Reverb),
    it will ALSO fetch GH's own sold listings as a higher-fidelity benchmark
    — since GH sells on Reverb, their completed sales = exact market value.

    Returns USD average or None if insufficient data.
    """
    all_prices: List[float] = []

    # ── 1. General market sold prices ──────────────────────────────────────
    params: dict = {
        "query":                   query,
        "state[]":                 "ended_with_sale",
        "per_page":                max_results,
        "page":                    1,
        "ships_from_country_code": "US",
    }
    data = safe_get_json(REVERB_API_BASE, params=params, extra_headers=REVERB_HEADERS)
    if data:
        for listing in data.get("listings", []):
            pd = listing.get("price", {})
            if pd.get("currency", "USD") != "USD":
                continue
            try:
                amt = float(pd.get("amount", 0))
                if amt > 100:
                    all_prices.append(amt)
            except (ValueError, TypeError):
                pass

    # ── 2. Guitar's Home own Reverb sold prices (optional, higher fidelity) ─
    if gh_reverb_shop:
        shop_params: dict = {
            "query":    query,
            "state[]":  "ended_with_sale",
            "per_page": 10,
            "page":     1,
        }
        # Reverb shop listings endpoint
        shop_url = f"https://reverb.com/api/shop/{gh_reverb_shop}/listings"
        shop_data = safe_get_json(shop_url, params=shop_params, extra_headers=REVERB_HEADERS)
        if shop_data:
            for listing in shop_data.get("listings", []):
                pd = listing.get("price", {})
                if pd.get("currency", "USD") != "USD":
                    continue
                try:
                    amt = float(pd.get("amount", 0))
                    if amt > 100:
                        # GH's own sold prices get 3× weight — most relevant benchmark
                        all_prices.extend([amt, amt, amt])
                except (ValueError, TypeError):
                    pass

    if not all_prices:
        return None

    # Trimmed mean
    all_prices.sort()
    if len(all_prices) >= 6:
        trim      = max(1, len(all_prices) // 7)
        all_prices = all_prices[trim:-trim]

    if not all_prices:
        return None

    avg = sum(all_prices) / len(all_prices)
    print(f"  [Reverb Sold] '{query}': ${avg:,.0f} USD avg ({len(all_prices)} comps)")
    return avg


# ─────────────────────────────────────────────────────────────────────────────
# eBay  (Finding API via RSS — no API key needed)
# Targets used electric guitars shipped from US.
# ─────────────────────────────────────────────────────────────────────────────

EBAY_SEARCH_TERMS = [
    "gibson les paul used",
    "gibson sg used",
    "gibson es-335 used",
    "fender stratocaster american used",
    "fender telecaster american used",
    "prs custom 24 used",
    "prs mccarty used",
    "suhr guitar used",
    "collings electric used",
    "music man axis used",
    "music man luke used",
    "charvel san dimas used",
    "fender custom shop stratocaster used",
    "gibson custom shop les paul used",
]

EBAY_RSS_BASE = (
    "https://www.ebay.com/sch/i.html"
    "?_nkw={query}"
    "&_sacat=33034"          # Electric Guitars category
    "&LH_ItemCondition=3000" # Used
    "&LH_PrefLoc=1"          # US only
    "&_sop=15"               # Sort by: lowest price + shipping
    "&_rss=1"
)

EBAY_BROWSE_BASE = (
    "https://www.ebay.com/sch/i.html"
    "?_nkw={query}"
    "&_sacat=33034"
    "&LH_ItemCondition=3000"
    "&LH_PrefLoc=1"
    "&_sop=10"               # Sort by: newly listed
)


def parse_ebay_html(html: str, query: str) -> List[Dict]:
    soup  = BeautifulSoup(html, "lxml")
    items = []

    cards = soup.select("li.s-item")

    for card in cards:
        title_el = card.select_one(".s-item__title")
        if not title_el:
            continue
        title = normalize_whitespace(title_el.get_text(" ", strip=True))
        # eBay adds "New Listing" prefix to some titles
        title = re.sub(r"^New Listing\s*", "", title, flags=re.I).strip()

        if not title or title.lower() == "shop on ebay":
            continue
        if is_accessory_title(title):
            continue
        if has_red_flags(title):
            continue

        link_el = card.select_one("a.s-item__link")
        url = (link_el.get("href") or "").split("?")[0] if link_el else ""
        if not url:
            continue

        price_el = card.select_one(".s-item__price")
        if not price_el:
            continue
        nums = extract_price_numbers(price_el.get_text())
        if not nums:
            continue
        price = min(nums)
        if price < MIN_PRICE_USD:
            continue

        # Shipping cost (add to price for fair comparison)
        ship_el = card.select_one(".s-item__shipping")
        ship_text = ship_el.get_text() if ship_el else ""
        ship_cost = 0.0
        if "free" in ship_text.lower():
            ship_cost = 0.0
        else:
            ship_nums = extract_price_numbers(ship_text)
            ship_cost = min(ship_nums) if ship_nums else 35.0  # assume $35 if unknown

        total_price = round(price + ship_cost, 2)

        items.append({
            "source":             "ebay",
            "title":              title,
            "price_usd":          total_price,
            "original_price_usd": None,
            "on_sale":            False,
            "discount_pct":       0.0,
            "url":                url,
            "condition":          "used",
        })

    return items


def scrape_ebay(max_pages: int = 3) -> List[Dict]:
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    print("\n=== Scrapeando eBay (used electric guitars) ===")

    for term in EBAY_SEARCH_TERMS:
        query_enc = term.replace(" ", "+")
        print(f"\n[eBay] Query: '{term}'")

        for page in range(1, max_pages + 1):
            url = (
                f"https://www.ebay.com/sch/i.html"
                f"?_nkw={query_enc}"
                f"&_sacat=33034"
                f"&LH_ItemCondition=3000"
                f"&LH_PrefLoc=1"
                f"&_sop=10"
                f"&_pgn={page}"
            )
            resp = safe_get(url)
            if not resp:
                print(f"  [eBay] p{page}: sin response, fin.")
                break

            page_items = parse_ebay_html(resp.text, term)
            if not page_items:
                print(f"  [eBay] p{page}: sin productos, fin.")
                break

            new_count = 0
            for item in page_items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                all_items.append(item)
                new_count += 1

            print(f"  [eBay] p{page}: {new_count} nuevos")
            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY_SECONDS + 0.5)

    print(f"\n[eBay] Total: {len(all_items)}")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Guitar Center  (Shopify-based used section)
# guitar center uses a dedicated used.guitarcenter.com subdomain.
# ─────────────────────────────────────────────────────────────────────────────

GC_BASE = "https://www.guitarcenter.com"

GC_SEARCH_TERMS = [
    "gibson les paul",
    "gibson sg",
    "gibson es-335",
    "fender american stratocaster",
    "fender american telecaster",
    "fender custom shop",
    "gibson custom shop",
    "prs custom 24",
    "prs mccarty",
    "suhr",
    "music man",
    "collings",
    "charvel san dimas",
]


def parse_gc_html(html: str) -> List[Dict]:
    soup  = BeautifulSoup(html, "lxml")
    items = []

    cards = (
        soup.select(".product-tile")
        or soup.select("[class*='ProductTile']")
        or soup.select("[class*='product-card']")
        or soup.select("article.product")
    )

    for card in cards:
        title_el = (
            card.select_one("[class*='product-title']")
            or card.select_one("[class*='ProductTitle']")
            or card.select_one("h3")
            or card.select_one("h2")
        )
        if not title_el:
            continue
        title = normalize_whitespace(title_el.get_text(" ", strip=True))
        if not title or is_accessory_title(title) or has_red_flags(title):
            continue

        link_el = card.select_one("a[href]")
        if not link_el:
            continue
        href = link_el["href"]
        url  = href if href.startswith("http") else f"{GC_BASE}{href}"

        # Sale / original price
        strike_el = card.select_one("s") or card.select_one("del") or card.select_one("[class*='original']")
        original_price: Optional[float] = None
        if strike_el:
            original_price = extract_usd_price(strike_el.get_text())

        price_el = (
            card.select_one("[class*='sale-price']")
            or card.select_one("[class*='salePrice']")
            or card.select_one("[class*='price']")
        )
        current_price: Optional[float] = None
        if price_el:
            nums = extract_price_numbers(price_el.get_text())
            if nums:
                current_price = min(nums)

        if not current_price or current_price < MIN_PRICE_USD:
            continue

        on_sale      = bool(original_price and original_price > current_price)
        discount_pct = (
            round((original_price - current_price) / original_price * 100, 1)
            if on_sale else 0.0
        )

        # Detect condition from title or badge
        condition = "used"
        condition_el = card.select_one("[class*='condition']") or card.select_one("[class*='Condition']")
        if condition_el:
            condition = normalize_whitespace(condition_el.get_text()).lower()

        items.append({
            "source":             "guitar_center",
            "title":              title,
            "price_usd":          current_price,
            "original_price_usd": original_price,
            "on_sale":            on_sale,
            "discount_pct":       discount_pct,
            "url":                url,
            "condition":          condition,
        })

    return items


def scrape_guitar_center(max_pages: int = 5) -> List[Dict]:
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    print("\n=== Scrapeando Guitar Center (used) ===")

    for term in GC_SEARCH_TERMS:
        query_enc = term.replace(" ", "%20")
        print(f"\n[GC] Query: '{term}'")

        for page in range(1, max_pages + 1):
            offset = (page - 1) * 24
            url = (
                f"{GC_BASE}/search?"
                f"Ntt={query_enc}"
                f"&N=4294967131"      # Used condition facet
                f"&Ns=r"
                f"&No={offset}"
            )
            resp = safe_get(url)
            if not resp:
                print(f"  [GC] p{page}: sin response, fin.")
                break

            page_items = parse_gc_html(resp.text)
            if not page_items:
                print(f"  [GC] p{page}: sin productos, fin.")
                break

            new_count = 0
            for item in page_items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                all_items.append(item)
                new_count += 1

            print(f"  [GC] p{page}: {new_count} nuevos")
            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY_SECONDS + 0.3)

    print(f"\n[Guitar Center] Total: {len(all_items)}")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Sam Ash  (used section via search)
# ─────────────────────────────────────────────────────────────────────────────

SAMASH_BASE = "https://www.samash.com"

SAMASH_SEARCH_TERMS = [
    "gibson les paul used",
    "fender stratocaster used",
    "prs used",
    "gibson custom shop used",
    "fender custom shop used",
]


def parse_samash_html(html: str) -> List[Dict]:
    soup  = BeautifulSoup(html, "lxml")
    items = []

    cards = (
        soup.select("[class*='product-item']")
        or soup.select("[class*='ProductCard']")
        or soup.select("article.product")
        or soup.select(".product")
    )

    for card in cards:
        title_el = (
            card.select_one("[class*='product-name']")
            or card.select_one("[class*='title']")
            or card.select_one("h3")
            or card.select_one("h2")
        )
        if not title_el:
            continue
        title = normalize_whitespace(title_el.get_text(" ", strip=True))
        if not title or is_accessory_title(title) or has_red_flags(title):
            continue

        link_el = card.select_one("a[href]")
        if not link_el:
            continue
        href = link_el["href"]
        url  = href if href.startswith("http") else f"{SAMASH_BASE}{href}"

        strike_el = card.select_one("s") or card.select_one("del")
        original_price: Optional[float] = None
        if strike_el:
            original_price = extract_usd_price(strike_el.get_text())

        price_el = card.select_one("[class*='price']") or card.select_one(".price")
        current_price: Optional[float] = None
        if price_el:
            nums = extract_price_numbers(price_el.get_text())
            if nums:
                current_price = min(nums)

        if not current_price or current_price < MIN_PRICE_USD:
            continue

        on_sale      = bool(original_price and original_price > current_price)
        discount_pct = (
            round((original_price - current_price) / original_price * 100, 1)
            if on_sale else 0.0
        )

        items.append({
            "source":             "sam_ash",
            "title":              title,
            "price_usd":          current_price,
            "original_price_usd": original_price,
            "on_sale":            on_sale,
            "discount_pct":       discount_pct,
            "url":                url,
            "condition":          "used",
        })

    return items


def scrape_sam_ash(max_pages: int = 5) -> List[Dict]:
    all_items: List[Dict] = []
    seen_urls: Set[str]   = set()

    print("\n=== Scrapeando Sam Ash (used) ===")

    for term in SAMASH_SEARCH_TERMS:
        query_enc = term.replace(" ", "+")
        print(f"\n[SamAsh] Query: '{term}'")

        for page in range(1, max_pages + 1):
            url = (
                f"{SAMASH_BASE}/search?q={query_enc}"
                f"&page={page}"
                f"&condition=Used"
            )
            resp = safe_get(url)
            if not resp:
                print(f"  [SamAsh] p{page}: sin response, fin.")
                break

            page_items = parse_samash_html(resp.text)
            if not page_items:
                print(f"  [SamAsh] p{page}: sin productos, fin.")
                break

            new_count = 0
            for item in page_items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                all_items.append(item)
                new_count += 1

            print(f"  [SamAsh] p{page}: {new_count} nuevos")
            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[Sam Ash] Total: {len(all_items)}")
    return all_items


# ─────────────────────────────────────────────────────────────────────────────
# Retrofret Vintage Instruments  (Shopify, US — Brooklyn, NY)
# Curated vintage and used guitars. Heavy on Gibson/Fender/Gretsch vintage.
# All products are truly vintage/used — 100% relevant inventory.
# ─────────────────────────────────────────────────────────────────────────────

RETROFRET_BASE = "https://www.retrofret.com"
RETROFRET_COLLECTION_HANDLES = [
    "electric-guitars",
    "acoustic-guitars",
]


def scrape_retrofret() -> List[Dict]:
    return scrape_shopify_store(RETROFRET_BASE, RETROFRET_COLLECTION_HANDLES, "retrofret")
