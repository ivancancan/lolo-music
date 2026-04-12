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
    "t3", "t5",   # Taylor electric series — semi-hollow/hollow electrics, ≠ any acoustic Taylor
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
        # text match: use word boundary so "l-2" doesn't match inside longer words
        if re.search(r'\b' + re.escape(model) + r'\b', text):
            return "acoustic"
        # text_clean match: only use for models ≥4 chars to avoid substring collisions
        # (e.g., "l2" matching "serial201550093yourg" → false positive)
        if len(model_clean) >= 4 and model_clean in text_clean:
            return "acoustic"
    for model in ELECTRIC_ONLY_MODELS:
        model_clean = re.sub(r"[^a-z0-9]", "", model)
        if re.search(r'\b' + re.escape(model) + r'\b', text):
            return "electric"
        if len(model_clean) >= 4 and model_clean in text_clean:
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
    "custom 24",        # PRS Custom 24 — set neck, mahogany body, ≠ CE 24 (bolt-on)
    "ce 24",            # PRS CE 24 — bolt-on neck, different construction from Custom 24
    "custom 22",        # PRS Custom 22 — 22-fret variant of Custom 24
    "ce 22",            # PRS CE 22 — bolt-on 22-fret variant
    "silver sky",       # PRS Silver Sky — John Mayer signature, Strat-style, ≠ Custom 24
    "mccarty 594",      # PRS McCarty 594 — vintage-voiced, different from Custom 24/CE 24
    "mccarty",          # PRS McCarty (non-594) — different from Custom 24/CE 24
    "santana retro",    # PRS Santana Retro — artist model, ≠ Custom 24
    "modern eagle",     # PRS Modern Eagle — premium tier, ≠ Custom 24
    "dgt",              # PRS DGT — David Grissom Trem, ≠ Custom 24
    "hollowbody",       # PRS Hollowbody — completely different from solid body models
    "singlecut",        # PRS Singlecut — single-cutaway, ≠ Custom 24 double-cutaway
    # Taylor acoustic models — each is a distinct body shape and price tier
    "grand auditorium",  # Taylor Grand Auditorium (e.g. 814ce)
    "grand symphony",    # Taylor Grand Symphony (e.g. 716ce) — larger body
    "grand concert",     # Taylor Grand Concert (e.g. 512ce) — smaller body
    "grand pacific",     # Taylor Grand Pacific (e.g. 717) — round-shoulder dreadnought
    "grand theater",     # Taylor Grand Theater (e.g. GT 811) — compact body
    "t3",               # Taylor T3 — semi-hollow electric, ≠ any acoustic Taylor
    "t5",               # Taylor T5 — hollow electric, ≠ T3 or acoustics
    # Gibson acoustic models — each is a distinct body/price class, must never cross-match
    "sj-200",           # Gibson SJ-200 — Super Jumbo, ≠ J-45/J-50/Hummingbird/Dove
    "sj200",
    "j-200",            # alternate name for SJ-200
    "j200",
    "j-45",             # Gibson J-45 — slope-shoulder dreadnought, ≠ SJ-200/Hummingbird/J-50
    "j45",
    "j-50",             # Gibson J-50 — natural-top variant of J-45 family
    "j50",
    "hummingbird",      # Gibson Hummingbird — square-shoulder dreadnought, ≠ J-45/SJ-200
    "dove",             # Gibson Dove — square-shoulder with ornamentation, ≠ Hummingbird
    # Suhr models
    "modern plus",      # Suhr Modern Plus — distinct from Modern, Classic S, Classic T
    "modern",           # Suhr Modern — base model, ≠ Modern Plus (different electronics)
    "alt",              # Suhr Alt (T, S) — alternative series
    # Rickenbacker models — each is a distinct body/spec/price tier ($400-1,900 gaps)
    "rickenbacker 660",  # solid body, T-style — most expensive (~$3,500)
    "rickenbacker 620",  # semi-hollow, mahogany body (~$2,800)
    "rickenbacker 360",  # semi-hollow, deluxe binding/hardware (~$2,200) — also 360/12
    "rickenbacker 330",  # semi-hollow, standard spec (~$1,600)
    "rickenbacker 325",  # short-scale, Lennon model — collector's item
    "rickenbacker 4003", # bass — completely separate category from 6-string models
    "rickenbacker 4001", # vintage bass
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
    "figured",   # catches "Figured Blood Moon", "Figured Top" without "Maple"
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
    "burgundy",
    "honey",
    "amber",
    "tobacco",
    "ocean",
    "seafoam",
    "surf",
    "olympic",
    "fiesta",
    "candy",
    "coral",
    "shell",
    "pink",
    "purple",
    "violet",
    "orange",
    "turquoise",
    "teal",
    "ivory",
    "cream",
    "vintage",
    "antique",
    "iced",
    "faded",
    "trans",
    "transparent",
    "satin",
    "mist",
}

# Year ranges that indicate vintage vs modern guitars.
# Matching a vintage year against a modern year almost always means a different guitar.
VINTAGE_YEAR_MAX = 1984   # guitars made up to this year are considered vintage
MODERN_YEAR_MIN = 2000    # guitars made from this year onward are considered modern

# ── Model-tier submodels ──────────────────────────────────────────────────────
# When both titles declare a tier submodel and they differ, it's a different guitar.
# "Les Paul Standard" ≠ "Les Paul Studio" — hundreds of USD apart.
# "Les Paul Custom" ≠ "Les Paul Standard" — different body, binding, fretboard.
# Keep this set to unambiguous, price-differentiating variants only.
TIER_SUBMODELS = {"standard", "studio", "classic", "junior", "deluxe", "special", "custom"}

# ── Accessory / non-instrument title keywords ─────────────────────────────────
# Listings for cases, bags, straps, parts — never instruments. Filter at scrape time
# and inside is_hard_match as a safety net to prevent benchmarking a $800 case
# against a $4,000 guitar (confirmed false positive: Gretsch hardshell case).
ACCESSORY_TITLE_KEYWORDS = {
    "hardshell case", "hard shell case", "gig bag", "case only", "case for",
    "softcase", "soft case", "strap only", "pickguard only", "neck only",
    "body only", "pickups only", "pickup only", "parts only", "tuner only",
    "tremolo only", "bridge only", "hardware only",
}

# ── Music Man model families ──────────────────────────────────────────────────
# Each Music Man artist/model line has distinct construction, pickups, and price.
# Luke (Luke Haller) ≠ Axis (EVH design) ≠ JP (John Petrucci) ≠ Cutlass (vintage S-style).
# Price gaps: Luke ~$2,500 / Axis ~$3,000 / JP ~$3,500+ / St. Vincent ~$2,000.
MUSICMAN_MODEL_FAMILIES = {
    "luke":       ["luke"],
    "axis":       ["axis"],
    "jp":         ["jp "],          # trailing space avoids matching "jpc" or partial words
    "cutlass":    ["cutlass"],
    "st vincent": ["st vincent", "st. vincent"],
    "stingray":   ["stingray"],     # bass
    "sterling":   ["sterling"],     # bass
}

# ── Finishing type ────────────────────────────────────────────────────────────
# Custom Shop guitars come in mutually exclusive finishing levels:
#   NOS / Closet Classic → pristine, no distressing, higher collector value
#   Relic / Heavy Relic  → artificially aged, different aesthetic, different price
# A NOS and a Relic of the same model can differ by $1,000-2,000 USD.
# Relic aging levels — each level is a distinct price tier ($500-1,500 between levels).
# Heavy Relic ($$$) > Journeyman Relic ($$) > Light Relic ($) > Relic (generic)
# Sorted longest-first to avoid "relic" matching before "heavy relic".
_RELIC_TIERS: Dict[str, int] = {
    "heavy relic":      3,
    "journeyman relic": 2,
    "light relic":      1,
    "relic":            0,   # generic "relic" — unspecified level
}
_NOS_KEYWORDS = {"nos", "closet classic"}


def _classify_finish_type(title: str) -> str:
    """Returns 'relic', 'nos', or '' (unknown)."""
    text = normalize_title(title)
    for kw in sorted(_RELIC_TIERS.keys(), key=len, reverse=True):
        if kw in text:
            return "relic"
    for kw in _NOS_KEYWORDS:
        if kw in text:
            return "nos"
    return ""


def _classify_relic_tier(title: str) -> Optional[int]:
    """Returns relic tier (0-3) or None if not a relic."""
    text = normalize_title(title)
    for kw in sorted(_RELIC_TIERS.keys(), key=len, reverse=True):
        if kw in text:
            return _RELIC_TIERS[kw]
    return None


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
        "knaggs", "tom anderson", "martin", "taylor", "rickenbacker",
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


def _extract_spec_terms(description: str) -> str:
    """
    Extract price-relevant spec terms from a product description for fuzzy matching.

    Descriptions are noisy (seller stories, shipping info, return policies).
    We extract only terms that help identify the guitar: brand, model, year,
    finish, wood, pickups, and known keywords from our matching vocabulary.

    Returns a short normalized string of spec-relevant tokens.
    """
    if not description:
        return ""
    text = normalize_title(description)

    spec_tokens: List[str] = []

    # Extract brand if present
    brand = extract_brand(description)
    if brand:
        spec_tokens.append(brand)

    # Extract model family if present
    model = extract_model_family(description)
    if model:
        spec_tokens.append(model)

    # Extract submodels (custom shop, masterbuilt, wood library, etc.)
    for sub in extract_submodels(description):
        spec_tokens.append(sub)

    # Extract finish tokens
    for f in extract_finish_tokens(description):
        spec_tokens.append(f)

    # Extract years
    year = extract_year(description)
    if year:
        spec_tokens.append(str(year))

    # Known spec keywords that affect identity/price
    _spec_kw = {
        "relic", "heavy relic", "light relic", "nos", "closet classic",
        "roasted", "flame", "quilt", "10 top", "figured",
        "hss", "hsh", "sss", "hhh", "p90", "p-90",
        "tremolo", "bigsby", "floyd", "hardtail",
        "satin", "gloss", "nitro", "poly",
    }
    for kw in _spec_kw:
        if kw in text:
            spec_tokens.append(kw)

    return " ".join(dict.fromkeys(spec_tokens))  # dedupe preserving order


def is_hard_match(gh_title: str, us_title: str,
                   gh_desc: str = "", us_desc: str = "") -> bool:
    # Build "effective" text for each side: title + description.
    # Hard-match checks use this so that specs hidden in descriptions are caught.
    # Title-only checks (finish type, artist) still use just the title where noted.
    gh_effective = (gh_title + " " + gh_desc).strip() if gh_desc else gh_title
    us_effective = (us_title + " " + us_desc).strip() if us_desc else us_title

    # Accessory / non-instrument check: cases, bags, parts are never guitars.
    # A Gretsch hardshell case listing ($800) must never match a White Falcon ($4,156).
    # Check BOTH sides — neither the benchmark nor the US listing should be an accessory.
    _us_title_lc = normalize_title(us_effective)
    _gh_title_lc = normalize_title(gh_effective)
    if any(kw in _us_title_lc for kw in ACCESSORY_TITLE_KEYWORDS):
        return False
    if any(kw in _gh_title_lc for kw in ACCESSORY_TITLE_KEYWORDS):
        return False

    # One of a Kind / signed / numbered check: these guitars have non-replicable pricing.
    # "1-of-1", "one of a kind", "signed by", "23/100" → skip on EITHER side.
    _unique_re = re.compile(
        r'one.of.a.kind|1\s*[-/]\s*of\s*[-/]?\s*1'
        r'|\bsigned\s+by\b|\bautograph'
        r'|\b\d+\s*/\s*\d+\b',
        re.I
    )
    if _unique_re.search(us_effective) or _unique_re.search(gh_effective):
        return False

    # Doubleneck check: EDS-1275 and other doublenecks are a completely different category.
    # Prevents "EDS-1275 Doubleneck" from matching any single-neck guitar like ES-335.
    _doubleneck_kw = {"doubleneck", "double neck", "double-neck", "eds-1275", "eds1275"}
    gh_dn = any(kw in normalize_title(gh_effective) for kw in _doubleneck_kw)
    us_dn = any(kw in normalize_title(us_effective) for kw in _doubleneck_kw)
    if gh_dn != us_dn:
        return False

    # Floyd Rose check: Floyd Rose tremolo is a fundamentally different spec from a standard
    # or trem bridge. Buyers are not interchangeable — a Floyd buyer won't accept standard.
    # Price difference can be $300-600 USD on the same base model.
    # Only reject if one declares Floyd and the other doesn't.
    _floyd_kw = {"floyd rose", "floyd", "fr tremolo", "double locking"}
    gh_floyd = any(kw in normalize_title(gh_effective) for kw in _floyd_kw)
    us_floyd = any(kw in normalize_title(us_effective) for kw in _floyd_kw)
    if gh_floyd != us_floyd:
        return False

    # Left-handed check: lefty guitars are a completely different market.
    # Inventory is smaller, resale is harder, prices differ. Never cross-match.
    _lefty_kw = {"left handed", "left-handed", "lefty", "zurdo", "zurda", "lh ", " lh"}
    gh_lefty = any(kw in normalize_title(gh_effective) for kw in _lefty_kw)
    us_lefty = any(kw in normalize_title(us_effective) for kw in _lefty_kw)
    if gh_lefty != us_lefty:
        return False

    # String count check: 7-string, 8-string, 12-string, baritone are different instruments.
    # A 7-string Les Paul is NOT the same market as a 6-string Les Paul.
    _string_re = re.compile(r'\b(7|8|9|12)[\s-]?string|\bbaritone\b|\bbari\b', re.I)
    gh_special_strings = bool(_string_re.search(gh_effective))
    us_special_strings = bool(_string_re.search(us_effective))
    if gh_special_strings != us_special_strings:
        return False
    # If both have special strings, ensure same count (7-string ≠ 8-string)
    if gh_special_strings and us_special_strings:
        gh_counts = set(re.findall(r'\b(\d+)[\s-]?string', gh_effective, re.I))
        us_counts = set(re.findall(r'\b(\d+)[\s-]?string', us_effective, re.I))
        if gh_counts and us_counts and gh_counts.isdisjoint(us_counts):
            return False

    # Pickup configuration check: HSS ≠ SSS ≠ HH ≠ P90.
    # Different pickup layouts produce fundamentally different tones and attract
    # different buyers. A Strat with HSS pickups is $100-300 different from SSS.
    _pickup_configs = {
        "hss", "ssh", "hsh", "hhh", "hh", "ss", "sss",
        "p90", "p-90", "p 90",
    }
    gh_norm = normalize_title(gh_effective)
    us_norm = normalize_title(us_effective)
    gh_pickups = {p for p in _pickup_configs if re.search(r'\b' + re.escape(p) + r'\b', gh_norm)}
    us_pickups = {p for p in _pickup_configs if re.search(r'\b' + re.escape(p) + r'\b', us_norm)}
    # Normalize aliases: "ssh" = "hss" (same config, different naming)
    _pickup_aliases = {"ssh": "hss"}
    gh_pickups = {_pickup_aliases.get(p, p) for p in gh_pickups}
    us_pickups = {_pickup_aliases.get(p, p) for p in us_pickups}
    # P-90 variants
    gh_p90 = bool(gh_pickups & {"p90", "p-90", "p 90"})
    us_p90 = bool(us_pickups & {"p90", "p-90", "p 90"})
    gh_pickups -= {"p-90", "p 90"}  # normalize to just "p90"
    us_pickups -= {"p-90", "p 90"}
    if gh_p90:
        gh_pickups.add("p90")
    if us_p90:
        us_pickups.add("p90")
    if gh_pickups and us_pickups and gh_pickups.isdisjoint(us_pickups):
        return False

    # One-sided P-90 check: if GH benchmark explicitly names P-90 pickups and US listing
    # doesn't mention any pickup config → reject. A Les Paul Standard P-90 ($3,000+) is
    # a specific product different from the humbucker LP Standard ($2,200-2,500).
    # We check effective text (title+desc) for P-90 keywords on both sides.
    _p90_re = re.compile(r'\bp-?90\b', re.I)
    gh_p90_explicit = bool(_p90_re.search(gh_effective))
    us_p90_explicit = bool(_p90_re.search(us_effective))
    if gh_p90_explicit and not us_p90_explicit:
        return False

    # Quilt Top / figured top check: a "10 Top" or "Quilt Top" PRS carries a significant
    # visual and resale premium over the plain-top version of the same model ($300-800 USD).
    # If GH explicitly lists a quilt/figured top and US doesn't mention it, don't match.
    # Note: only applies when GH (the benchmark) has the premium top — we don't reject when
    # only the US side declares it (buying premium, benchmarking standard = conservative).
    _quilt_kw = {"quilt top", "quilted", "10 top", "10top", "figured"}
    gh_quilt = any(kw in normalize_title(gh_effective) for kw in _quilt_kw)
    us_quilt = any(kw in normalize_title(us_effective) for kw in _quilt_kw)
    if gh_quilt and not us_quilt:
        return False

    # Florentine check: Les Paul Custom Florentine Plus has a carved maple top and specific
    # appointments that make it a distinct model from a standard Les Paul Custom VOS.
    # If GH benchmark declares "Florentine" and US listing doesn't → different guitar.
    gh_florentine = "florentine" in normalize_title(gh_effective)
    us_florentine = "florentine" in normalize_title(us_effective)
    if gh_florentine and not us_florentine:
        return False

    # Goldtop check: Goldtop is a specific finish (gold metallic) on Les Paul and PRS.
    # Commands a different resale price than burst or other finishes.
    # If GH benchmark is a Goldtop and US listing isn't → finish mismatch, reject.
    gh_goldtop = "goldtop" in normalize_title(gh_effective) or "gold top" in normalize_title(gh_effective)
    us_goldtop = "goldtop" in normalize_title(us_effective) or "gold top" in normalize_title(us_effective)
    if gh_goldtop and not us_goldtop:
        return False

    # Artisan check: Fender Custom Shop "Artisan" is a premium sub-line with figured
    # exotic tonewoods (roasted rosewood neck, figured rosewood body, etc.).
    # Commands $1,500-2,000 more than a regular Custom Shop Classic/Deluxe.
    # If GH benchmark is an Artisan and US listing isn't → benchmark mismatch.
    gh_artisan = "artisan" in normalize_title(gh_effective)
    us_artisan = "artisan" in normalize_title(us_effective)
    if gh_artisan and not us_artisan:
        return False

    # Guitar type check: acoustic vs electric — never match across types.
    # Extra safety: if GH benchmark is definitively acoustic, US must also be acoustic.
    # An unknown US type (model not in our sets) is not safe to match against a known acoustic.
    gh_type = detect_guitar_type(gh_effective)
    us_type = detect_guitar_type(us_effective)
    if gh_type and us_type and gh_type != us_type:
        return False
    if gh_type == "acoustic" and us_type != "acoustic":
        return False

    # Brand must match if both sides declare one.
    # Check effective text (title + description) so brand in description is caught.
    gh_brand = extract_brand(gh_effective)
    us_brand = extract_brand(us_effective)
    if gh_brand and us_brand and gh_brand != us_brand:
        return False

    # Model family must match if both sides declare one.
    # Check effective text so model names in descriptions are caught (e.g. Reverb title
    # says "Suhr Modern Plus" but description says "CE 24" → mismatch detected).
    gh_family = extract_model_family(gh_effective)
    us_family = extract_model_family(us_effective)
    if gh_family and us_family and gh_family != us_family:
        return False

    # Aging tier check: Murphy Lab tiers carry $2,000-4,000 price gaps between levels.
    # Only reject if BOTH sides declare an aging tier and they differ by more than 1 step.
    gh_aging = detect_aging_tier(gh_effective)
    us_aging = detect_aging_tier(us_effective)
    if gh_aging is not None and us_aging is not None:
        if abs(gh_aging - us_aging) > 1:
            return False

    # Brazilian rosewood check: adds $2,000-5,000+ vs Indian rosewood.
    # If one side explicitly declares Brazilian and the other doesn't, it's a different guitar.
    # Use title-only (not effective/description) — descriptions often mention "Brazilian" contextually
    # (e.g. seller comparing materials) without the guitar actually having Brazilian rosewood.
    gh_brazilian = detect_brazilian(gh_title)
    us_brazilian = detect_brazilian(us_title)
    if gh_brazilian != us_brazilian:
        return False

    # Submodel checks — effective text for general submodels.
    gh_sub = extract_submodels(gh_effective)
    us_sub = extract_submodels(us_effective)

    # Premium tier check: one premium vs one non-premium is a mismatch.
    # IMPORTANT: Use TITLE-ONLY for premium detection — descriptions often mention
    # "Custom Shop" in comparative context ("without going full Custom Shop") which
    # does NOT mean the guitar IS Custom Shop.
    gh_sub_title = extract_submodels(gh_title)
    us_sub_title = extract_submodels(us_title)
    premium_markers = {
        "custom shop", "murphy lab", "masterbuilt", "vos",
        "wood library", "private stock", "made 2 measure",
        "dealer exclusive", "wildwood spec",
    }
    gh_premium = gh_sub_title & premium_markers
    us_premium = us_sub_title & premium_markers
    if bool(gh_premium) != bool(us_premium):
        return False

    # Masterbuilt vs regular Custom Shop: Masterbuilt guitars are hand-built by a single
    # master builder and command 2-3x the price of a regular Custom Shop ($6K-10K+ vs $3K-5K).
    # If one side declares Masterbuilt and the other doesn't, it's a different price tier.
    gh_masterbuilt = "masterbuilt" in gh_sub_title
    us_masterbuilt = "masterbuilt" in us_sub_title
    if gh_masterbuilt != us_masterbuilt:
        return False

    # Model tier check: "Les Paul Standard" ≠ "Les Paul Studio" — clearly different guitars.
    # If both sides declare a differentiating tier submodel and they don't share one → reject.
    gh_tier = gh_sub & TIER_SUBMODELS
    us_tier = us_sub & TIER_SUBMODELS
    if gh_tier and us_tier and gh_tier.isdisjoint(us_tier):
        return False

    # Finish check: only reject when one side has a PREMIUM/DISTINCTIVE finish that the other
    # doesn't share. Generic color names ("tobacco", "cherry", "burst") are just color variants
    # of the same model — rejecting on those causes false negatives (e.g. LP Tobacco Burst vs
    # LP Heritage Cherry). Goldtop is already handled by its own filter above.
    # Only finishes that carry a real price premium (natural, pelham, seafoam, olympic, fiesta)
    # or that are structurally distinct should disqualify.
    _PREMIUM_FINISH = {"natural", "pelham", "seafoam", "surf", "olympic", "fiesta", "blonde"}
    gh_finish = extract_finish_tokens(gh_effective)
    us_finish = extract_finish_tokens(us_effective)
    gh_premium = gh_finish & _PREMIUM_FINISH
    us_premium = us_finish & _PREMIUM_FINISH
    if gh_premium and not (gh_premium & us_finish):
        return False  # GH has premium finish, US has nothing matching it

    # Finishing type check: Relic ≠ NOS — different aesthetic, different price category.
    # A Relic Custom Shop and an NOS Custom Shop of the same model can differ by $1,000-2,000 USD.
    gh_finish_type = _classify_finish_type(gh_effective)
    us_finish_type = _classify_finish_type(us_effective)
    if gh_finish_type and us_finish_type and gh_finish_type != us_finish_type:
        return False

    # Relic sub-level check: Heavy Relic ($$$) vs Journeyman Relic ($$) vs Light Relic ($).
    # These are distinct aging levels with $500-1,500 price gaps between them.
    # If both are relics but different tiers, reject.
    gh_relic_tier = _classify_relic_tier(gh_effective)
    us_relic_tier = _classify_relic_tier(us_effective)
    if (gh_relic_tier is not None and us_relic_tier is not None
            and gh_relic_tier != us_relic_tier
            and gh_relic_tier != 0 and us_relic_tier != 0):
        # Only reject when BOTH specify a concrete tier (not generic "relic" = tier 0).
        # "Heavy Relic" vs "Journeyman Relic" → reject.
        # "Relic" vs "Heavy Relic" → allow (generic could be any level).
        return False

    # Artist/signature check — bidirectional.
    # If EITHER side has a named artist model, the other side must mention the same artist.
    # Use effective text so artist names in descriptions are caught.
    gh_text = normalize_title(gh_effective)
    us_text = normalize_title(us_effective)
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
        # Modern vs modern: max 5-year gap.
        # Used inventory circulates 3-5 years after manufacture. A 2020 LP Standard in
        # excellent condition vs a 2024 benchmark is a real opportunity (~$200-400 diff),
        # not a mismatch. Deal score penalizes year gap; this is not a hard reject.
        if gh_year >= MODERN_YEAR_MIN and us_year >= MODERN_YEAR_MIN:
            if abs(gh_year - us_year) > 5:
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

    # Center Block check: Gretsch (and others) make "Center Block" semi-hollow variants
    # that are fundamentally different from their full-hollow counterparts.
    # G6636T-RF (Center Block) ≠ G6136RF (full hollow Falcon) — different construction,
    # weight, feedback characteristics, and price ($500-1500 difference).
    _center_block_kw = {"center block", "center-block", "centerblock"}
    gh_cb = any(kw in normalize_title(gh_effective) for kw in _center_block_kw)
    us_cb = any(kw in normalize_title(us_effective) for kw in _center_block_kw)
    if gh_cb != us_cb:
        return False

    # Bigsby vs non-Bigsby (V-Stoptail) check: different tailpiece = different guitar.
    # A Bigsby adds vibrato but changes string tension/feel. On Gretsch Falcons,
    # V-Stoptail versions (no Bigsby) command different prices.
    _bigsby_kw = {"bigsby", "b7", "b6", "b3"}
    _stoptail_kw = {"v-stop", "v stop", "stoptail", "stop tail"}
    gh_bigsby = any(kw in normalize_title(gh_effective) for kw in _bigsby_kw)
    us_bigsby = any(kw in normalize_title(us_effective) for kw in _bigsby_kw)
    gh_stop = any(kw in normalize_title(gh_effective) for kw in _stoptail_kw)
    us_stop = any(kw in normalize_title(us_effective) for kw in _stoptail_kw)
    # Reject if one declares Bigsby and the other declares stoptail
    if (gh_bigsby and us_stop) or (us_bigsby and gh_stop):
        return False

    # Les Paul Custom decade-variant check: "70's Custom" / "70s Custom" is a distinct
    # reissue with pancake body, volute, and different specs from a regular LP Custom.
    # Same applies to "54 Custom", "57 Custom", etc. — but those are caught by reissue year.
    # The 70's specifically is sold as "Les Paul Custom 70's" without a specific reissue year.
    _70s_re = re.compile(r"\b70'?s\b", re.I)
    gh_70s = bool(_70s_re.search(gh_effective))
    us_70s = bool(_70s_re.search(us_effective))
    if gh_family == "les paul custom" or us_family == "les paul custom":
        if gh_70s != us_70s:
            return False

    # Taylor cutaway/electronics check: Taylor "XXXce" models have a cutaway body and
    # built-in electronics (pickup + preamp). The non-ce version (e.g. 716 vs 716ce)
    # is a different guitar with different playability and $200-500 price difference.
    # Also catches "e" suffix (electronics only, no cutaway) vs bare model.
    _taylor_ce_re = re.compile(r'\b\d{3}ce\b', re.I)
    _taylor_no_ce_re = re.compile(r'\b\d{3}\b(?!ce)', re.I)
    gh_has_ce = bool(_taylor_ce_re.search(gh_effective))
    us_has_ce = bool(_taylor_ce_re.search(us_effective))
    # Only check if at least one side is a Taylor
    if (extract_brand(gh_effective) == "taylor" or extract_brand(us_effective) == "taylor"):
        if gh_has_ce and not us_has_ce:
            return False
        if us_has_ce and not gh_has_ce:
            return False

    # Anniversary edition check: "70th Anniversary", "40th Anniversary", etc. are
    # specific limited editions with different specs and price points.
    # If one title has an anniversary marker and the other doesn't, it's a mismatch.
    _anniv_re = re.compile(r'\b(\d+(?:st|nd|rd|th)\s+anniversary|anniversary)\b', re.I)
    gh_has_anniv = bool(_anniv_re.search(gh_title))
    us_has_anniv = bool(_anniv_re.search(us_title))
    if gh_has_anniv != us_has_anniv:
        return False

    # Explorer Reverse check: the Reverse Explorer has an inverted body shape and
    # is a limited/collector's item (Guitar of the Month, etc.) — completely different
    # guitar from a standard Explorer or 70's Explorer reissue.
    gh_reverse = "reverse" in normalize_title(gh_effective)
    us_reverse = "reverse" in normalize_title(us_effective)
    if gh_family == "explorer" or us_family == "explorer":
        if gh_reverse != us_reverse:
            return False

    # Taylor model series check: Taylor uses 3-digit model numbers where the
    # hundreds digit indicates the series tier (4xx, 5xx, 7xx, 8xx, 9xx).
    # A 716ce (700-series, rosewood) ≠ 416ce (400-series, ovangkol) — different
    # tonewoods, different price tier (~$500-1500 gap between adjacent series).
    _taylor_series_re = re.compile(r'\b([3-9])\d{2}ce?\b', re.I)
    if extract_brand(gh_effective) == "taylor" or extract_brand(us_effective) == "taylor":
        gh_series = _taylor_series_re.search(gh_effective)
        us_series = _taylor_series_re.search(us_effective)
        if gh_series and us_series:
            if gh_series.group(1) != us_series.group(1):
                return False

    # Body wood mismatch check for custom-order brands (Suhr, Tom Anderson, etc.).
    # Custom builders make the same model name with different body woods (alder vs
    # basswood vs ash vs mahogany) — these are effectively different guitars with
    # different tonal profiles and price points ($200-600 difference).
    _CUSTOM_BRANDS = {"suhr", "tom anderson", "anderson", "tyler", "james tyler",
                      "sadowsky", "nash", "k-line", "collings"}
    _BODY_WOODS = {"alder", "basswood", "ash", "swamp ash", "mahogany", "korina",
                   "poplar", "maple", "limba", "pine", "paulownia", "chambered"}
    _brand = extract_brand(gh_effective)
    if _brand in _CUSTOM_BRANDS:
        gh_woods = {w for w in _BODY_WOODS if w in normalize_title(gh_effective)}
        us_woods = {w for w in _BODY_WOODS if w in normalize_title(us_effective)}
        if gh_woods and us_woods and gh_woods.isdisjoint(us_woods):
            return False

    # Pickup configuration mismatch for custom brands: HSS ≠ SSS ≠ HH.
    # Standard guitars (Gibson HH, Fender SSS) don't usually declare this in titles,
    # but custom builders always do because it's a build choice.
    _PICKUP_CONFIGS = {"hss", "ssh", "hsh", "hhh", "hh", "sss", "ss", "h-s-s",
                       "h-s-h", "s-s-s"}
    if _brand in _CUSTOM_BRANDS:
        gh_config = {c for c in _PICKUP_CONFIGS if c in normalize_title(gh_effective)}
        us_config = {c for c in _PICKUP_CONFIGS if c in normalize_title(us_effective)}
        if gh_config and us_config and gh_config != us_config:
            return False

    # Gibson body family check: LP, SG, ES-335, Flying V, Explorer, Firebird are
    # completely different instruments. A mismatch here means totally wrong guitar.
    # Confirmed false positive: ES-335 Custom MTM matched against LP Custom Alpine White.
    _GIBSON_BODY_FAMILIES = {
        "les paul": {"les paul", "lp"},
        "sg":       {"sg"},
        "es-335":   {"es-335", "es335", "es 335"},
        "es-339":   {"es-339", "es339", "es 339"},
        "es-345":   {"es-345", "es345"},
        "es-355":   {"es-355", "es355"},
        "es-330":   {"es-330", "es330"},
        "flying v": {"flying v", "flying-v", "flyingv"},
        "explorer": {"explorer"},
        "firebird": {"firebird"},
    }
    if gh_brand == "gibson" and us_brand == "gibson":
        def _gibson_body_family(t: str) -> Optional[str]:
            tn = normalize_title(t)
            for family, aliases in _GIBSON_BODY_FAMILIES.items():
                if any(a in tn for a in aliases):
                    return family
            return None
        gbf_a = _gibson_body_family(gh_effective)
        gbf_b = _gibson_body_family(us_effective)
        if gbf_a and gbf_b and gbf_a != gbf_b:
            return False

    # Music Man model family check: Luke ≠ Axis ≠ JP ≠ Cutlass ≠ St. Vincent.
    # Confirmed false positive: EBMM Axis (EVH design) matched against Luke III (Luke Haller).
    # Each family has distinct pickups, construction, and price ($500-1,500 gaps).
    _mm_brands = {"music man", "ernie ball", "ernieball"}
    if any(b in normalize_title(gh_effective) for b in _mm_brands) and \
       any(b in normalize_title(us_effective) for b in _mm_brands):
        def _mm_family(t: str) -> Optional[str]:
            tn = normalize_title(t)
            for family, aliases in MUSICMAN_MODEL_FAMILIES.items():
                if any(a in tn for a in aliases):
                    return family
            return None
        mmf_a = _mm_family(gh_effective)
        mmf_b = _mm_family(us_effective)
        if mmf_a and mmf_b and mmf_a != mmf_b:
            return False
        # If one side declares a family and the other doesn't, reject (unknown ≠ known)
        if (mmf_a and not mmf_b) or (mmf_b and not mmf_a):
            return False

    # PRS SE vs Core check: SE (Korean, ~$500-900) ≠ Core/CE/S2 (USA, ~$1,800-4,000).
    # Confirmed false positive: PRS SE Custom 22 Semi-Hollow ($799) matched against
    # PRS CE-24 Semi-Hollow benchmark ($3,463 USD). ~$2,500 of false margin.
    # The existing S2 filter covers S2 vs Core. This covers SE vs everything else.
    if gh_brand == "prs" and us_brand == "prs":
        us_is_se = bool(re.search(r'\bse\b', normalize_title(us_effective)))
        gh_is_se = bool(re.search(r'\bse\b', normalize_title(gh_effective)))
        if us_is_se != gh_is_se:
            return False

    # Semi-hollow variant check: guitars that exist in both solid and semi-hollow versions
    # are distinct products with different construction, tone, and price (~$300-500 gap).
    # PRS CE-24 Semi-Hollow ≠ PRS CE-24, Gibson ES-335 Thinline ≠ ES-335, etc.
    # Bidirectional: benchmark must match what we're buying.
    _SEMI_HOLLOW_KW = {"semi-hollow", "semi hollow", "semihollow", "thinline"}
    gh_semi = any(kw in normalize_title(gh_effective) for kw in _SEMI_HOLLOW_KW)
    us_semi = any(kw in normalize_title(us_effective) for kw in _SEMI_HOLLOW_KW)
    if gh_semi != us_semi:
        return False

    # PRS S2 vs Core check: S2 line (~$900-1,500 USD) uses different body construction
    # and cheaper components than Core (~$2,500-4,000 USD). Same model name (Custom 24,
    # McCarty) but "S2" prefix makes it a completely different product tier.
    # Without this check, a PRS S2 Custom 24 would match a PRS Custom 24 Core benchmark
    # and produce a false 40%+ margin — potential $2,000+ loss if purchased.
    if gh_brand == "prs" and us_brand == "prs":
        gh_s2 = bool(re.search(r'\bs2\b', normalize_title(gh_effective)))
        us_s2 = bool(re.search(r'\bs2\b', normalize_title(us_effective)))
        if gh_s2 != us_s2:
            return False

    # Gretsch series tier check: G5xxx (Electromatic, ~$500-800) ≠ G6xxx (Professional,
    # $2,000-4,000+). "Rancher" = acoustic → never match against an electric Gretsch.
    if gh_brand == "gretsch" and us_brand == "gretsch":
        def _gretsch_series(t: str) -> Optional[int]:
            m = re.search(r'\bG(\d)', t, re.I)
            return int(m.group(1)) if m else None
        gh_rancher = "rancher" in normalize_title(gh_effective)
        us_rancher = "rancher" in normalize_title(us_effective)
        if gh_rancher != us_rancher:
            return False
        gs_a = _gretsch_series(gh_effective)
        gs_b = _gretsch_series(us_effective)
        if gs_a is not None and gs_b is not None and gs_a != gs_b:
            return False

    # Fender generation filter: Ultra ≠ Professional ≠ Performer ≠ Player ≠ Standard.
    # The existing upgrade check (Player II / Pro II vs base) only catches "II/Plus" suffixes.
    # This check catches cross-generation mismatches (Ultra vs Professional, etc.).
    # Each generation is a distinct product line with different specs and $200-600 gap.
    _FENDER_GENERATIONS = [
        ("ultra",),
        ("professional ii", "pro ii"),
        ("professional", "pro"),
        ("performer",),
        ("player plus",),
        ("player ii",),
        ("player",),
        ("american special",),
        ("american standard",),
        ("standard",),
    ]
    if gh_brand == "fender" and us_brand == "fender":
        def _fender_gen(t: str) -> Optional[str]:
            tn = normalize_title(t)
            for gen_group in _FENDER_GENERATIONS:
                if any(g in tn for g in gen_group):
                    return gen_group[0]
            return None
        gh_gen = _fender_gen(gh_effective)
        us_gen = _fender_gen(us_effective)
        if gh_gen and us_gen and gh_gen != us_gen:
            return False

    # Gibson CS reissue code filter: R7 (1957), R8 (1958), R9 (1959), R0 (1960) are
    # distinct Custom Shop models with $1,000-3,000 price gaps between them.
    # The existing reissue year check uses the raw year; this catches the shorthand codes.
    _GIBSON_REISSUE_CODES: Dict[str, int] = {"r7": 1957, "r8": 1958, "r9": 1959, "r0": 1960}
    if gh_brand == "gibson" and us_brand == "gibson":
        def _gibson_reissue_code(t: str) -> Optional[int]:
            tn = normalize_title(t)
            for code, year in _GIBSON_REISSUE_CODES.items():
                if re.search(r'\b' + code + r'\b', tn):
                    return year
            return None
        gh_rc = _gibson_reissue_code(gh_effective)
        us_rc = _gibson_reissue_code(us_effective)
        # ANY difference in reissue code = different guitar. R8 (1958) ≠ R9 (1959):
        # different neck profile, different collector demand, $500-800 price gap.
        if gh_rc and us_rc and gh_rc != us_rc:
            return False

    # Martin finish filter: Natural vs Sunburst on the same Martin model are different
    # price points — Sunburst commands $300-800 premium on most dreadnoughts and OM models.
    # Only applies when BOTH sides declare a critical finish keyword.
    _MARTIN_CRITICAL_FINISHES = {"sunburst", "amberburst", "natural", "black"}
    if gh_brand == "martin" and us_brand == "martin":
        def _martin_finish(t: str) -> Optional[str]:
            tn = normalize_title(t)
            for f in _MARTIN_CRITICAL_FINISHES:
                if f in tn:
                    return f
            return None
        mf_a = _martin_finish(gh_effective)
        mf_b = _martin_finish(us_effective)
        if mf_a and mf_b and mf_a != mf_b:
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

def _score_candidate(gh_item: Dict, us_item: Dict, gh_year: int | None) -> int | None:
    """Score a single US item against a GH item. Returns score or None if no match."""
    if not is_hard_match(gh_item["title"], us_item["title"],
                         gh_desc=gh_item.get("description", ""),
                         us_desc=us_item.get("description", "")):
        return None

    # Score from titles only
    title_score = fuzzy_score(gh_item["title"], us_item["title"])

    # Score from title + description: when US title is vague but description has
    # the real specs, this recovers matches that title-only scoring would miss.
    us_desc = us_item.get("description", "")
    desc_spec_terms = _extract_spec_terms(us_desc)
    if desc_spec_terms:
        enriched_us = us_item["title"] + " " + desc_spec_terms
        desc_score = fuzzy_score(gh_item["title"], enriched_us)
    else:
        desc_score = title_score

    score = max(title_score, desc_score)

    # Vague-title penalty
    gh_tokens = tokenize(gh_item["title"])
    us_tokens = tokenize(us_item["title"])
    if len(us_tokens) > 0 and len(gh_tokens) > 0:
        token_ratio = len(us_tokens) / len(gh_tokens)
        if token_ratio < 0.50:
            gh_set = set(gh_tokens)
            us_set = set(us_tokens)
            missing_from_title = gh_set - us_set
            if missing_from_title and desc_spec_terms:
                desc_tokens = set(normalize_title(desc_spec_terms).split())
                recovered = missing_from_title & desc_tokens
                recovery_ratio = len(recovered) / len(missing_from_title) if missing_from_title else 0
            else:
                recovery_ratio = 0.0
            if recovery_ratio >= 0.50:
                score = max(0, score - 8)
            else:
                score = max(0, score - 25)

    # Year proximity bonus/penalty
    us_year = extract_year(us_item["title"])
    if gh_year and us_year and gh_year >= MODERN_YEAR_MIN and us_year >= MODERN_YEAR_MIN:
        year_diff = abs(gh_year - us_year)
        if year_diff == 0:
            score = min(100, score + 4)
        elif year_diff == 1:
            score = min(100, score + 1)
        else:
            score = max(0, score - 3)

    return score


def find_best_match(gh_item: Dict, us_items: List[Dict]) -> Tuple[Optional[Dict], int]:
    best_item = None
    best_score = 0
    gh_year = extract_year(gh_item["title"])

    for us_item in us_items:
        score = _score_candidate(gh_item, us_item, gh_year)
        if score is not None and score > best_score:
            best_score = score
            best_item = us_item

    return best_item, best_score


def find_all_matches(gh_item: Dict, us_items: List[Dict],
                     min_score: int = 75) -> List[Tuple[Dict, int]]:
    """Return ALL qualifying US matches for a GH guitar, sorted by price (cheapest first).

    This ensures opportunities from non-Reverb stores surface even when
    Reverb has a higher fuzzy score for the same model. Each store's cheapest
    qualifying match is returned (one per source).
    """
    gh_year = extract_year(gh_item["title"])
    # Collect best match per source
    best_per_source: Dict[str, Tuple[Dict, int]] = {}

    for us_item in us_items:
        score = _score_candidate(gh_item, us_item, gh_year)
        if score is None or score < min_score:
            continue
        source = us_item.get("source", "unknown")
        existing = best_per_source.get(source)
        if existing is None:
            best_per_source[source] = (us_item, score)
        else:
            # Prefer cheaper price within same source; if same price, higher score
            ex_item, ex_score = existing
            if (us_item["price_usd"] < ex_item["price_usd"] or
                    (us_item["price_usd"] == ex_item["price_usd"] and score > ex_score)):
                best_per_source[source] = (us_item, score)

    # Sort by price ascending (cheapest first = best arbitrage)
    results = sorted(best_per_source.values(), key=lambda x: x[0]["price_usd"])
    return results
