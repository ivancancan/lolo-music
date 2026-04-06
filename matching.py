import re
from rapidfuzz import fuzz
from typing import Optional, Tuple, List, Dict, Set


STOPWORDS = {
    "used", "electric", "guitar", "guitars", "with", "w", "and",
    "the", "a", "an", "shop", "limited", "edition",
    "2020", "2021", "2022", "2023", "2024", "2025", "2026",
}

# Guitar type classification — acoustic vs electric.
# If both titles have a detectable type and they differ, it's a hard mismatch.
# Models that are unambiguously acoustic (not electro-acoustic hybrids):
ACOUSTIC_MODELS = {
    "sj-200", "sj200", "j-200", "j200",          # Gibson jumbo acoustics
    "j-45", "j45", "j-50", "j50",
    "l-00", "l00", "l-1", "l-2",
    "advanced jumbo", "aj",
    "hummingbird",
    "dove",
    "gospel",
    "songwriter",
    "d-28", "d-18", "d-45", "d-35",              # Martin dreadnoughts
    "om-28", "om-18", "000-28", "000-18",
    "dreadnought",
    "grand auditorium", "grand concert",
}
# Models that are unambiguously electric (not semi-hollow or acoustic):
ELECTRIC_ONLY_MODELS = {
    "stratocaster", "telecaster", "jazzmaster", "jaguar", "mustang",
    "les paul", "sg", "flying v", "explorer", "firebird",
    "es-335", "es335", "es-339", "es339", "es-330", "es330",
    "es-345", "es345", "es-355", "es355",
}


def detect_guitar_type(title: str) -> str:
    """
    Returns 'acoustic', 'electric', or '' (unknown).
    Used to prevent acoustic/electric mismatches.

    Checks two forms of the normalized text:
    - With separators preserved (hyphens): matches "sj-200", "es-339"
    - Separator-stripped (alphanumeric only): matches "es339", "sj200" even when
      the title writes "ES 339" (space instead of hyphen) — common in Reverb listings
    """
    text       = normalize_title(title)
    text_clean = re.sub(r"[^a-z0-9]", "", text)   # "ES 339" → "es339"

    for model in ACOUSTIC_MODELS:
        model_clean = re.sub(r"[^a-z0-9]", "", model)
        if model in text or model_clean in text_clean:
            return "acoustic"
    for model in ELECTRIC_ONLY_MODELS:
        model_clean = re.sub(r"[^a-z0-9]", "", model)
        if model in text or model_clean in text_clean:
            return "electric"
    return ""


def detect_aging_tier(title: str) -> Optional[int]:
    """
    Detect the aging/finishing tier from a guitar title.
    Returns the tier integer (0–5) or None if no aging tier is declared.

    Examples:
      "Murphy Lab Ultra Heavy Aged" → 5
      "Murphy Lab Heavy Aged"       → 4
      "Murphy Lab Light Aged"       → 2
      "Murphy Lab VOS"              → 0
      "Standard Les Paul"           → None  (no aging declared)
    """
    text = normalize_title(title)
    for phrase, tier in sorted(AGING_TIERS.items(), key=lambda x: len(x[0]), reverse=True):
        if phrase in text:
            return tier
    return None


def detect_brazilian(title: str) -> bool:
    """Returns True if the title explicitly names Brazilian rosewood."""
    text = normalize_title(title)
    return any(kw in text for kw in BRAZILIAN_KEYWORDS)


def detect_flame_top(title: str) -> Optional[str]:
    """
    Returns 'figured', 'plain', or None (unknown) based on top wood description.
    Figured tops command a visual premium; plain tops do not.
    """
    text = normalize_title(title)
    for kw in FIGURED_TOP_KEYWORDS:
        if kw in text:
            return "figured"
    for kw in PLAIN_TOP_KEYWORDS:
        if kw in text:
            return "plain"
    return None


MODEL_KEYWORDS = {
    # These longer phrases MUST come before their shorter substrings — extract_model_family
    # iterates sorted by length descending, so longer names win over shorter ones.
    "les paul sg",      # Gibson Les Paul SG '61/'63 — SG body despite name, ≠ regular Les Paul
    "les paul custom",  # Gibson Les Paul Custom — Black Beauty, 3 humbuckers, ≠ Les Paul Standard/R9
    "sg custom",        # Gibson SG Custom — 3 humbuckers, binding, ≠ SG Special (P-90s)
    "sg special",       # Gibson SG Special — P-90 pickups, no binding, ≠ SG Custom
    "eds-1275",         # Gibson EDS-1275 Doubleneck — 6+12 string, completely ≠ ES-335
    "eds1275",          # alternate spelling without hyphen
    "classic s",        # Suhr Classic S — Strat-style, distinct from Classic T (Tele-style)
    "classic t",        # Suhr Classic T — Tele-style, distinct from Classic S
    "les paul",
    "sg",
    "telecaster",
    "stratocaster",
    "jazzmaster",
    "jaguar",
    "mustang",
    "es-335",
    "es335",
    "es-339",
    "es339",
    "es-345",           # Gibson ES-345 — semi-hollow with Varitone, ≠ ES-335 / Flying V / any solid body
    "es345",
    "es-355",           # Gibson ES-355 — premium semi-hollow, ≠ ES-335 (different binding/hardware tier)
    "es355",
    "es-330",           # Gibson ES-330 — fully hollow body, ≠ ES-335 (different construction)
    "es330",
    "firebird",
    "explorer",
    "flying v",
    "flyingv",
    "precision bass",
    "p bass",
    "pbass",
    "jazz bass",
    "j bass",
    "jbass",
}

SUBMODEL_KEYWORDS = {
    "custom",
    "standard",
    "deluxe",
    "junior",
    "special",
    "classic",
    "studio",
    "traditional",
    "modern",
    "supreme",
    "reissue",
    "custom shop",
    "murphy lab",
    "masterbuilt",
    "vos",
    "wood library",
    "private stock",
    # Dealer-exclusive and boutique tiers
    "made 2 measure",
    "dealer exclusive",
    "limited run",
    "wildwood spec",
    "artist series",
}

# ── Aging tier ────────────────────────────────────────────────────────────────
# Murphy Lab guitars receive different levels of hand-aging, each increasing cost.
# Heavier aging = more labor + more premium. Mismatching tiers = wrong price benchmark.
#   ultra heavy aged  ≈ $7,000-9,000 USD  (highest premium)
#   heavy aged        ≈ $5,500-7,500 USD
#   ultra light aged  ≈ $4,500-6,000 USD
#   light aged        ≈ $4,000-5,500 USD
#   vos (vintage orig spec) ≈ $3,500-5,000 USD  (no visible aging, baseline Custom Shop)
# Sorted longest-first to avoid "light aged" matching before "ultra light aged".
AGING_TIERS: Dict[str, int] = {
    "ultra heavy aged": 5,
    "heavy aged":       4,
    "ultra light aged": 3,
    "light aged":       2,
    "lightly aged":     2,
    "aged":             1,   # generic "aged" — light finish only
    "vos":              0,   # no aging — Custom Shop baseline
}

# ── Premium tonewoods ─────────────────────────────────────────────────────────
# Brazilian rosewood fretboards: protected by CITES, extremely scarce.
# Adds $2,000–5,000+ USD over Indian rosewood equivalent.
# If one title declares Brazilian and the other doesn't, it's a different-price guitar.
BRAZILIAN_KEYWORDS = {"brazilian", "brazilian rosewood", "brw"}

# Figured maple top levels — affects visual premium in MX market.
# Flame/figured tops sell faster at higher prices vs plain tops.
FIGURED_TOP_KEYWORDS = {
    "flame top", "flame maple", "highly figured", "figured maple",
    "4a", "5a", "aaa", "aaaa",   # grade notation
}
PLAIN_TOP_KEYWORDS = {"plain top", "plain maple"}

# Artist/signature models: if GH title contains a known artist name, the US title must also.
# This prevents a Richie Kotzen Signature from matching a Player II Stratocaster.
ARTIST_SIGNATURES = {
    "richie kotzen", "john mayer", "dave murray", "eric clapton",
    "kirk hammett", "jimmy page", "joe bonamassa", "peter frampton",
    "steve vai", "trini lopez", "bb king", "b.b. king",
    "santana", "bonabyrd", "grissom", "dave grissom",
    "richard fortus", "bruno mars", "slash",
}

FINISH_WORDS = {
    "burst",
    "bengal",
    "ebony",
    "black",
    "white",
    "alpine",
    "goldtop",
    "cherry",
    "sunburst",
    "natural",
    "walnut",
    "blue",
    "red",
    "silver",
    "green",
    "pelham",
    "blonde",
    "grey",
    "gray",
    "wine",
    "mocha",
    "amethyst",
}

# Year ranges that indicate vintage vs modern guitars.
# Matching a vintage year against a modern year almost always means a different guitar.
VINTAGE_YEAR_MAX = 1984   # guitars made up to this year are considered vintage
MODERN_YEAR_MIN = 2000    # guitars made from this year onward are considered modern

# ── Model-tier submodels ──────────────────────────────────────────────────────
# When both titles declare a tier submodel and they differ, it's a different guitar.
# "Les Paul Standard" ≠ "Les Paul Studio" — hundreds of USD apart.
# Keep this set to unambiguous, price-differentiating variants only.
TIER_SUBMODELS = {"standard", "studio", "classic", "junior", "deluxe", "special"}

# ── Finishing type ────────────────────────────────────────────────────────────
# Custom Shop guitars come in mutually exclusive finishing levels:
#   NOS / Closet Classic → pristine, no distressing, higher collector value
#   Relic / Heavy Relic  → artificially aged, different aesthetic, different price
# A NOS and a Relic of the same model can differ by $1,000-2,000 USD.
_RELIC_KEYWORDS = {"heavy relic", "journeyman relic", "light relic", "relic"}
_NOS_KEYWORDS   = {"nos", "closet classic"}


def _classify_finish_type(title: str) -> str:
    """Returns 'relic', 'nos', or '' (unknown)."""
    text = normalize_title(title)
    for kw in sorted(_RELIC_KEYWORDS, key=len, reverse=True):   # longest first
        if kw in text:
            return "relic"
    for kw in _NOS_KEYWORDS:
        if kw in text:
            return "nos"
    return ""


# Pre-normalize ARTIST_SIGNATURES so the lookup works correctly after normalize_title().
# normalize_title() strips dots, so "b.b. king" → "b b king", not "bb king".
# We normalize each artist entry once at import time.
_NORM_ARTIST_SIGNATURES: Set[str] = set()   # populated below after normalize_title is defined


def normalize_title(title: str) -> str:
    text = title.lower().strip()
    text = text.replace("'", "'").replace("\u201c", '"').replace("\u201d", '"')
    text = text.replace("_", " ")
    text = re.sub(r"[^a-z0-9\s'/-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(title: str) -> List[str]:
    text = normalize_title(title)
    return [t for t in text.split() if t not in STOPWORDS]


def token_set(title: str) -> Set[str]:
    return set(tokenize(title))


def contains_phrase(text: str, phrase: str) -> bool:
    return phrase in text


def extract_brand(title: str) -> Optional[str]:
    text = normalize_title(title)
    # IMPORTANT: epiphone must come before gibson.
    # Epiphone uses "Inspired by Gibson" in product names, which contains the word "gibson".
    # Checking epiphone first ensures those titles are not misidentified as Gibson.
    for brand in [
        "epiphone",
        "gibson", "fender", "prs", "suhr", "music man", "ernie ball",
        "gretsch", "ibanez", "charvel", "jackson", "esp", "collings",
        "knaggs", "tom anderson", "taylor", "rickenbacker",
    ]:
        if brand in text:
            return brand
    return None


# Now that normalize_title is defined, build the normalized artist signatures set.
# This ensures "b.b. king" (ARTIST_SIGNATURES) → "b b king" (normalized),
# which correctly matches what appears in normalized guitar titles.
_NORM_ARTIST_SIGNATURES.update(normalize_title(a) for a in ARTIST_SIGNATURES)


def extract_model_family(title: str) -> Optional[str]:
    text = normalize_title(title)
    for model in sorted(MODEL_KEYWORDS, key=len, reverse=True):
        if contains_phrase(text, model):
            return model
    return None


def extract_submodels(title: str) -> Set[str]:
    text = normalize_title(title)
    found = set()
    for word in SUBMODEL_KEYWORDS:
        if contains_phrase(text, word):
            found.add(word)
    return found


def extract_finish_tokens(title: str) -> Set[str]:
    tokens = token_set(title)
    return {t for t in tokens if t in FINISH_WORDS}


def extract_year(title: str) -> Optional[int]:
    """
    Extract the production year from a guitar title (1950–2026).

    Handles both full 4-digit years (1959, 2024) and abbreviated 2-digit years
    written as 'YY (e.g., '78 → 1978, '21 → 2021, '59 → 1959).

    Prefers modern production years (≥2000) over vintage/reissue years.
    This handles titles like "Gibson Custom 1959 Les Paul Reissue 2024"
    where 1959 is the reissue model indicator and 2024 is the actual build year.

    Returns the best year found, or None if no year is present.
    """
    text = normalize_title(title)

    # Full 4-digit years
    matches = re.findall(r'\b(19[5-9]\d|20[012]\d)\b', text)
    years = [int(y) for y in matches]

    # Abbreviated years written as 'YY (e.g., '78, '21, '59)
    # After normalize_title, apostrophe-Y becomes 'Y in text (apostrophe is kept as ')
    abbrev = re.findall(r"'(\d{2})\b", text)
    for ab in abbrev:
        n = int(ab)
        if 50 <= n <= 99:
            years.append(1900 + n)   # '78 → 1978, '59 → 1959
        elif 0 <= n <= 30:
            years.append(2000 + n)   # '21 → 2021, '03 → 2003

    if not years:
        return None

    # Prefer production year (≥2000) — vintage years in titles like "1959 Les Paul" are model names
    modern = [y for y in years if y >= 2000]
    if modern:
        return modern[0]
    return years[0]


def extract_reissue_year(title: str) -> Optional[int]:
    """
    Extract the vintage reissue year from a title (1950–1999 only).

    Distinct from extract_year() which prefers modern production years.
    Used to reject cross-reissue matches like '57 R7 vs '68 Custom — same
    production year (2023) but different reissue specs → different guitar.

    Returns the smallest vintage year found, or None.
    """
    text = normalize_title(title)
    vintage = re.findall(r'\b(19[5-9]\d)\b', text)
    years = [int(y) for y in vintage]

    abbrev = re.findall(r"'(\d{2})\b", text)
    for ab in abbrev:
        n = int(ab)
        if 50 <= n <= 99:
            years.append(1900 + n)

    return min(years) if years else None


def fuzzy_score(title_a: str, title_b: str) -> int:
    a = normalize_title(title_a)
    b = normalize_title(title_b)
    return int(fuzz.token_set_ratio(a, b))


def is_hard_match(gh_title: str, us_title: str) -> bool:
    # Doubleneck check: EDS-1275 and other doublenecks are a completely different category.
    # Prevents "EDS-1275 Doubleneck" from matching any single-neck guitar like ES-335.
    _doubleneck_kw = {"doubleneck", "double neck", "double-neck", "eds-1275", "eds1275"}
    gh_dn = any(kw in normalize_title(gh_title) for kw in _doubleneck_kw)
    us_dn = any(kw in normalize_title(us_title) for kw in _doubleneck_kw)
    if gh_dn != us_dn:
        return False

    # Floyd Rose check: Floyd Rose tremolo is a fundamentally different spec from a standard
    # or trem bridge. Buyers are not interchangeable — a Floyd buyer won't accept standard.
    # Price difference can be $300-600 USD on the same base model.
    # Only reject if one declares Floyd and the other doesn't.
    _floyd_kw = {"floyd rose", "floyd", "fr tremolo", "double locking"}
    gh_floyd = any(kw in normalize_title(gh_title) for kw in _floyd_kw)
    us_floyd = any(kw in normalize_title(us_title) for kw in _floyd_kw)
    if gh_floyd != us_floyd:
        return False

    # Quilt Top / figured top check: a "10 Top" or "Quilt Top" PRS carries a significant
    # visual and resale premium over the plain-top version of the same model ($300-800 USD).
    # If GH explicitly lists a quilt/figured top and US doesn't mention it, don't match.
    # Note: only applies when GH (the benchmark) has the premium top — we don't reject when
    # only the US side declares it (buying premium, benchmarking standard = conservative).
    _quilt_kw = {"quilt top", "quilted", "10 top", "10top"}
    gh_quilt = any(kw in normalize_title(gh_title) for kw in _quilt_kw)
    us_quilt = any(kw in normalize_title(us_title) for kw in _quilt_kw)
    if gh_quilt and not us_quilt:
        return False

    # Florentine check: Les Paul Custom Florentine Plus has a carved maple top and specific
    # appointments that make it a distinct model from a standard Les Paul Custom VOS.
    # If GH benchmark declares "Florentine" and US listing doesn't → different guitar.
    gh_florentine = "florentine" in normalize_title(gh_title)
    us_florentine = "florentine" in normalize_title(us_title)
    if gh_florentine and not us_florentine:
        return False

    # Goldtop check: Goldtop is a specific finish (gold metallic) on Les Paul and PRS.
    # Commands a different resale price than burst or other finishes.
    # If GH benchmark is a Goldtop and US listing isn't → finish mismatch, reject.
    gh_goldtop = "goldtop" in normalize_title(gh_title) or "gold top" in normalize_title(gh_title)
    us_goldtop = "goldtop" in normalize_title(us_title) or "gold top" in normalize_title(us_title)
    if gh_goldtop and not us_goldtop:
        return False

    # Guitar type check: acoustic vs electric — never match across types.
    # Extra safety: if GH benchmark is definitively acoustic, US must also be acoustic.
    # An unknown US type (model not in our sets) is not safe to match against a known acoustic.
    gh_type = detect_guitar_type(gh_title)
    us_type = detect_guitar_type(us_title)
    if gh_type and us_type and gh_type != us_type:
        return False
    if gh_type == "acoustic" and us_type != "acoustic":
        return False

    # Brand must match if both titles declare one
    gh_brand = extract_brand(gh_title)
    us_brand = extract_brand(us_title)
    if gh_brand and us_brand and gh_brand != us_brand:
        return False

    # Model family must match if both titles declare one
    gh_family = extract_model_family(gh_title)
    us_family = extract_model_family(us_title)
    if gh_family and us_family and gh_family != us_family:
        return False

    # Aging tier check: Murphy Lab tiers carry $2,000-4,000 price gaps between levels.
    # Only reject if BOTH titles declare an aging tier and they differ by more than 1 step.
    # (If only one side declares it, we allow the match — the other side may not label it.)
    gh_aging = detect_aging_tier(gh_title)
    us_aging = detect_aging_tier(us_title)
    if gh_aging is not None and us_aging is not None:
        if abs(gh_aging - us_aging) > 1:
            return False

    # Brazilian rosewood check: adds $2,000-5,000+ vs Indian rosewood.
    # If one title explicitly declares Brazilian and the other doesn't, it's a different guitar.
    gh_brazilian = detect_brazilian(gh_title)
    us_brazilian = detect_brazilian(us_title)
    if gh_brazilian != us_brazilian:
        return False

    # Submodel checks — compute once, used for both premium tier and model tier.
    gh_sub = extract_submodels(gh_title)
    us_sub = extract_submodels(us_title)

    # Premium tier check: one premium vs one non-premium is a mismatch
    premium_markers = {
        "custom shop", "murphy lab", "masterbuilt", "vos",
        "wood library", "private stock", "made 2 measure",
        "dealer exclusive", "wildwood spec",
    }
    gh_premium = gh_sub & premium_markers
    us_premium = us_sub & premium_markers
    if bool(gh_premium) != bool(us_premium):
        return False

    # Model tier check: "Les Paul Standard" ≠ "Les Paul Studio" — clearly different guitars.
    # If both titles declare a differentiating tier submodel and they don't share one → reject.
    gh_tier = gh_sub & TIER_SUBMODELS
    us_tier = us_sub & TIER_SUBMODELS
    if gh_tier and us_tier and gh_tier.isdisjoint(us_tier):
        return False

    # Finish check: if both declare finish tokens that share nothing, it's a different colorway
    gh_finish = extract_finish_tokens(gh_title)
    us_finish = extract_finish_tokens(us_title)
    if gh_finish and us_finish and gh_finish.isdisjoint(us_finish):
        return False

    # Finishing type check: Relic ≠ NOS — different aesthetic, different price category.
    # A Relic Custom Shop and an NOS Custom Shop of the same model can differ by $1,000-2,000 USD.
    gh_finish_type = _classify_finish_type(gh_title)
    us_finish_type = _classify_finish_type(us_title)
    if gh_finish_type and us_finish_type and gh_finish_type != us_finish_type:
        return False

    # Artist/signature check — bidirectional.
    # If EITHER side has a named artist model, the other side must mention the same artist.
    # Prevents: "B.B. King ES-335" ↔ "Alvin Lee ES-335" (different artists)
    #           "Jimmy Page Telecaster" ↔ "1955 Masterbuilt Relic" (signature vs non-signature)
    #           "Santana Retro" ↔ "Custom 24" (Santana is an artist name on PRS)
    # NOTE: normalize artist names the same way as titles — "b.b. king" → "b b king".
    gh_text = normalize_title(gh_title)
    us_text = normalize_title(us_title)
    gh_artists = {a for a in _NORM_ARTIST_SIGNATURES if a in gh_text}
    us_artists = {a for a in _NORM_ARTIST_SIGNATURES if a in us_text}
    if gh_artists and not us_artists:
        return False
    if us_artists and not gh_artists:
        return False
    # Both have artists but different ones (e.g. B.B. King vs Alvin Lee)
    if gh_artists and us_artists and gh_artists != us_artists:
        return False

    # Year range check: prevent vintage guitars from matching modern ones,
    # and prevent same-model guitars from different years from cross-benchmarking.
    # A 2019 Les Paul Standard can be $500-800 USD cheaper than a 2024 — same model, different price.
    gh_year = extract_year(gh_title)
    us_year = extract_year(us_title)
    if gh_year and us_year:
        # Hard fail: one vintage, one modern
        if (gh_year <= VINTAGE_YEAR_MAX and us_year >= MODERN_YEAR_MIN):
            return False
        if (us_year <= VINTAGE_YEAR_MAX and gh_year >= MODERN_YEAR_MIN):
            return False
        # Modern vs modern: max 2-year gap.
        # Same model shifts spec/price more than 2 years apart (e.g., 2019 vs 2024 Les Paul).
        if gh_year >= MODERN_YEAR_MIN and us_year >= MODERN_YEAR_MIN:
            if abs(gh_year - us_year) > 2:
                return False
        # Vintage vs vintage (reissue model years like 1954, 1959): max 4-year gap.
        # |1959 - 1954| = 5 → different reissue models (R9 vs Goldtop) → reject.
        elif abs(gh_year - us_year) >= 5:
            return False

    # Reissue year check: Custom Shop titles often have BOTH a vintage reissue year
    # (the model spec, e.g., "1957 R7") AND a modern production year (e.g., "2023").
    # extract_year() picks 2023 for both → year check passes even though the reissue
    # specs are totally different (1957 vs 1968 are distinct Les Paul Custom models).
    # Check vintage reissue years separately; same ≥5 gap threshold as vintage guitars.
    gh_reissue = extract_reissue_year(gh_title)
    us_reissue = extract_reissue_year(us_title)
    if gh_reissue and us_reissue and abs(gh_reissue - us_reissue) >= 5:
        return False

    # Fender series-tier checks: upgraded product lines are distinct from their base versions.
    # Mismatching series = wrong price benchmark (~$200-400 gap per upgrade tier).
    #
    # "Player II" / "Player Plus" ≠ "Player"
    # "American Professional II" ≠ "American Professional"  (one of the 5 validation targets)
    # "American Ultra II" ≠ "American Ultra"
    _fender_upgrade_re = re.compile(
        r'\b(player|professional|ultra)\s+(ii|plus)\b', re.I
    )
    gh_fender_upgrade = bool(_fender_upgrade_re.search(gh_title))
    us_fender_upgrade = bool(_fender_upgrade_re.search(us_title))
    if gh_fender_upgrade != us_fender_upgrade:
        return False

    # Anniversary edition check: "70th Anniversary", "40th Anniversary", etc. are
    # specific limited editions with different specs and price points.
    # If one title has an anniversary marker and the other doesn't, it's a mismatch.
    _anniv_re = re.compile(r'\b(\d+(?:st|nd|rd|th)\s+anniversary|anniversary)\b', re.I)
    gh_has_anniv = bool(_anniv_re.search(gh_title))
    us_has_anniv = bool(_anniv_re.search(us_title))
    if gh_has_anniv != us_has_anniv:
        return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Red Flag Detection
# Guitars with structural damage sell below Reverb "Sold" benchmarks.
# ─────────────────────────────────────────────────────────────────────────────

RED_FLAG_KEYWORDS = [
    "headstock repair",
    "repaired headstock",
    "headstock crack",
    "cracked headstock",
    "broken headstock",
    "headstock break",
    "headstock glue",
    "headstock broken",
    "neck crack",
    "neck break",
    "structural repair",
    "body crack",
    "crack in the neck",
    "cracked neck",
    "damaged neck",
    "broken neck",
    "crack in body",
]


def has_red_flags(title: str, description: str = "") -> bool:
    """
    Returns True if the guitar mentions structural damage that would
    cause it to sell below the normal Reverb 'Sold' average.
    """
    combined = normalize_title(title + " " + description)
    return any(kw in combined for kw in RED_FLAG_KEYWORDS)


def build_reverb_sold_query(title: str) -> str:
    """
    Build a clean Reverb search query from a guitar title.
    Uses brand + model_family + key_submodel for accurate sold-price comps.

    When no model family is detected (e.g. PRS Santana, PRS CE 24 — model
    names not in MODEL_KEYWORDS), falls back to extracting up to 2 distinctive
    words from the title so the query is specific instead of just "prs".

    Examples:
        "Gibson Les Paul Custom Murphy Lab Ultra Light Aged 2024"
        → "gibson les paul custom shop murphy lab"

        "PRS Santana Retro 10 Top 2023"
        → "prs santana retro"

        "PRS CE 24 Semi-Hollow"
        → "prs ce 24"
    """
    brand   = extract_brand(title)        or ""
    model   = extract_model_family(title) or ""
    subs    = extract_submodels(title)
    # Include the submodel tier that most affects price
    key_sub = next(
        (s for s in ["custom shop", "murphy lab", "wood library", "private stock", "masterbuilt"]
         if s in subs),
        "",
    )
    parts = [p for p in [brand, model, key_sub] if p]

    # If model family is unknown, extract up to 2 distinctive words from the
    # title so the Reverb query is specific (avoids all PRS guitars sharing "prs")
    if not model:
        norm = normalize_title(title)
        skip = STOPWORDS | {brand}
        extra_words = [w for w in norm.split() if w not in skip and len(w) >= 2
                       and not w.isdigit()]
        extra = " ".join(extra_words[:2])
        if extra:
            parts.append(extra)

    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────

def find_best_match(gh_item: Dict, us_items: List[Dict]) -> Tuple[Optional[Dict], int]:
    best_item = None
    best_score = 0

    gh_year = extract_year(gh_item["title"])

    for us_item in us_items:
        if not is_hard_match(gh_item["title"], us_item["title"]):
            continue

        score = fuzzy_score(gh_item["title"], us_item["title"])

        # Year proximity bonus/penalty.
        # Prefers exact-year matches when multiple candidates pass the hard filter.
        # This matters when a GH 2024 listing could match a 2023 or 2022 US listing.
        us_year = extract_year(us_item["title"])
        if gh_year and us_year and gh_year >= MODERN_YEAR_MIN and us_year >= MODERN_YEAR_MIN:
            year_diff = abs(gh_year - us_year)
            if year_diff == 0:
                score = min(100, score + 4)   # exact year match — strongest signal
            elif year_diff == 1:
                score = min(100, score + 1)   # 1-year gap — minor bonus
            else:
                score = max(0, score - 3)     # 2-year gap — slight penalty (still passes hard filter)

        if score > best_score:
            best_score = score
            best_item = us_item

    return best_item, best_score
