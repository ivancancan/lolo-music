import os
import re
import json
import warnings
from datetime import datetime
from dotenv import load_dotenv
from rapidfuzz import fuzz

from scrapers import (
    scrape_guitarshome,
    scrape_daves,
    scrape_wildwood,
    scrape_graysons,
    scrape_twin_town,
    scrape_cme,
    scrape_cream_city,
    scrape_music_go_round,
    scrape_elderly,
    scrape_normans,
    scrape_toneshop,
    scrape_retrofret,
    scrape_ebay,
    scrape_guitar_center,
    scrape_sam_ash,
    scrape_reverb,
    scrape_reverb_query,
    fetch_reverb_sold_avg,
)
from matching import find_best_match, find_all_matches, build_reverb_sold_query, has_red_flags
from pricing import calculate_landed_cost, calculate_net_margin, is_opportunity, estimate_sell_mxn
from catalog import (
    load_sold_catalog,
    load_full_history,
    find_gh_historical_price,
    build_proactive_targets,
    build_liquidity_scores,
    get_liquidity,
    get_fresh_posts,
)
from price_history import PriceHistory
from deal_score import compute_deal_score, verdict_emoji, VERDICT_BUY, VERDICT_REVIEW
from notifier import send_telegram_message, send_telegram_photo
# AI review and self-learning optimizer disabled — manual validation preferred
# from ai_reviewer import review_opportunities, format_ai_reviews
# from optimizer import apply_tuning_to_deal_score, optimize_from_report, load_tuning

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def save_json(filename: str, data) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


SOURCE_LABELS = {
    "reverb":         "Reverb",
    "daves":          "Dave's Guitar",
    "wildwood":       "Wildwood Guitars",
    "graysons":       "Graysons Tune Town",
    "twin_town":      "Twin Town Guitars",
    "normans":        "Norman's Rare Guitars",
    "toneshop":       "Tone Shop Guitars",
    "retrofret":      "Retrofret Vintage",
    "cme":            "Chicago Music Exchange",
    "cream_city":     "Cream City Music",
    "music_go_round": "Music Go Round",
    "elderly":        "Elderly Instruments",
    "ebay":           "eBay",
    "guitar_center":  "Guitar Center",
    "sam_ash":        "Sam Ash",
}

# ── Validation targets (Fase 1 — 5 guitarras de validación inicial) ──────────
# Used to flag opportunities that match Ivan's first-purchase shortlist.
# Fuzzy match against gh_title — if score >= threshold, show ★ GUITARRA OBJETIVO.
VALIDATION_TARGETS = [
    "Fender American Professional II Stratocaster",
    "Gibson Les Paul Standard 50s",
    "PRS CE 24",
    "Gibson SG Standard",
    "Fender American Ultra Telecaster",
]
_VALIDATION_THRESHOLD = 78   # fuzzy threshold to consider a match


def is_validation_target(gh_title: str) -> bool:
    from rapidfuzz import fuzz as _fuzz
    t = gh_title.lower()
    for target in VALIDATION_TARGETS:
        if _fuzz.token_set_ratio(t, target.lower()) >= _VALIDATION_THRESHOLD:
            return True
    return False


def fmt_source(source: str) -> str:
    return SOURCE_LABELS.get(source, source.capitalize())


# ── Quality gates ─────────────────────────────────────────────────────────────
# Hard requirements that must pass for a deal to be shown at all.

MIN_SELL_RATE        = 0.30   # GH must have sold ≥30% of this model historically
MIN_MATCH_BUY_NOW   = 88     # fuzzy match must be ≥88 for BUY NOW verdict
MIN_LIQ_SOLD_COUNT  = 2      # need at least 2 historical sales to trust liquidity


def passes_liquidity_gate(liq: dict | None, proactive: bool = False) -> bool:
    """Return False if this model has proven LOW demand at GH — don't show it."""
    if not liq:
        # No liquidity data at all: for proactive, this is too risky. For reactive
        # (GH actively selling it), allow through with neutral score.
        return not proactive
    if liq.get("sell_rate", 0) < MIN_SELL_RATE:
        return False
    if liq.get("count_sold", 0) < MIN_LIQ_SOLD_COUNT:
        return False
    return True


def on_sale_tag(item: dict) -> str:
    if item.get("on_sale") and item.get("discount_pct", 0) > 0:
        return f"[ON SALE -{item['discount_pct']:.0f}%]"
    return ""


def benchmark_label(item: dict) -> str:
    src = item.get("benchmark_source", "gh_listing")
    if src == "instagram_sold":
        return f"GH vendio (IG) ${item['benchmark_usd']:,.0f} USD ({item.get('benchmark_match','')[:40]})"
    return f"GH lista ${item['benchmark_usd']:,.0f} USD"


# ─────────────────────────────────────────────────────────────────────────────
# Message builder
# ─────────────────────────────────────────────────────────────────────────────

def _card(i: int, item: dict, full: bool = True) -> str:
    sale      = on_sale_tag(item)
    sale_str  = f"  {sale}\n" if sale else ""
    orig      = item.get("original_price_usd")
    orig_str  = f" (orig ${orig:,.0f})" if orig else ""
    if item.get("very_fresh"):
        mode_tag = "[FRESH] "
    elif item.get("fresh"):
        mode_tag = "[RECIENTE] "
    elif item.get("proactive"):
        mode_tag = "[PROACTIVO] "
    else:
        mode_tag = ""
    # "All Original" warning: GH benchmark is for an all-original vintage guitar,
    # but US listing doesn't mention it → benchmark may be inflated 30-50%.
    _all_orig_kw = {"all original", "all orig", "100% original", "all-original"}
    _bench_title = (item.get("benchmark_match") or item.get("gh_title") or "").lower()
    _us_title_lc = item.get("us_title", "").lower()
    _gh_all_orig = any(kw in _bench_title for kw in _all_orig_kw)
    _us_all_orig = any(kw in _us_title_lc for kw in _all_orig_kw)
    all_orig_str = (
        "   ⚠ BENCHMARK 'ALL ORIGINAL' — verificar originalidad del listing US\n"
        if _gh_all_orig and not _us_all_orig else ""
    )
    capped_str = (
        "   ⚠ BENCHMARK AJUSTADO — precio GH muy arriba del mercado US, margen recalculado con Reverb\n"
        if item.get("benchmark_capped") else ""
    )

    reverb_avg = item.get("reverb_sold_avg_usd")
    reverb_str = ""
    if reverb_avg:
        benchmark_usd = item.get("benchmark_usd", 0)
        if benchmark_usd and reverb_avg > benchmark_usd * 1.05:
            reverb_warn = " ⚠ REVERB > GH PRECIO — comps US mas caros, verificar mismo spec"
        elif benchmark_usd and reverb_avg < benchmark_usd * 0.60:
            reverb_warn = " ⚠ REVERB MUY BAJO — benchmark puede ser optimista"
        else:
            reverb_warn = ""
        reverb_str = f"   Reverb Sold ref: ${reverb_avg:,.0f} USD{reverb_warn}\n"
    liq        = item.get("liquidity")
    liq_str    = ""
    if liq:
        avg_d = liq['avg_days_to_sell']
        # avg_days < 3 means all samples were "effectively sold" (>60 days old) — display as "< 60 días"
        avg_d_str = "< 60" if avg_d < 3 else f"~{avg_d:.0f}"
        liq_str = (
            f"   Liquidez: {avg_d_str} dias | "
            f"{liq['sell_rate']*100:.0f}% vendidas "
            f"[{liq['count_sold']}/{liq['count_total']}]\n"
        )

    ds          = item.get("deal_score", {})
    ds_total    = ds.get("total", 0)
    ds_verdict  = ds.get("verdict", "")
    ds_flags    = ds.get("flags", [])
    # DOM note: if days_on_market is unknown, score could be up to 10 pts higher once DB accumulates
    dom         = item.get("days_on_market")
    dom_pending = " (+hasta 10 pts cuando acumule historial)" if dom is None else ""
    ds_str      = f"   {verdict_emoji(ds_verdict)}  ({ds_total}/100){dom_pending}\n" if ds_verdict else ""
    # Validation target flag
    target_str  = "   ★ GUITARRA OBJETIVO (Fase 1)\n" if is_validation_target(item.get("gh_title", "")) else ""
    flags_str   = f"   {' | '.join(ds_flags)}\n" if ds_flags else ""
    dom_str     = f" | {dom}d en venta" if dom else ""
    drop_str    = " | PRECIO BAJO" if item.get("had_price_drop") else ""
    offer_str   = ""
    if dom and dom >= 45:
        offer_price = round(item["us_price_usd"] * 0.90)
        offer_str = f"   OFERTA SUGERIDA: ${offer_price:,.0f} USD ({dom}d en venta)\n"

    if not full:
        liq_tag = f" [{liq['avg_days_to_sell']:.0f}d]" if liq else ""
        return (
            f"{i}. {mode_tag}{item['gh_title']}{(' ' + sale) if sale else ''}\n"
            f"   {fmt_source(item['us_source'])} | "
            f"Margen: {item['margin']*100:.1f}% | Match: {item['score']}"
            f"{liq_tag} | Deal: {ds_total}/100\n"
        )

    sell_mxn     = item.get("suggested_sell_mxn", 0)
    sell_mxn_str = f"   Venta MX: ${sell_mxn:,.0f} MXN\n" if sell_mxn else ""

    return (
        f"{i}. {mode_tag}{item['gh_title']}\n"
        f"{ds_str}"
        f"{target_str}"
        f"{flags_str}"
        f"{sale_str}"
        f"   Fuente: {fmt_source(item['us_source'])} [{item.get('us_condition','')}]{dom_str}{drop_str}\n"
        f"   Listing: {item['us_title']}\n"
        f"   Compra: ${item['us_price_usd']:,.0f} USD{orig_str}\n"
        f"   Costo aterrizaje: ${item['landed_cost_usd']:,.0f} USD\n"
        f"   Benchmark: {benchmark_label(item)}\n"
        f"{all_orig_str}"
        f"{capped_str}"
        f"{sell_mxn_str}"
        f"{reverb_str}"
        f"{liq_str}"
        f"   Margen: {item['margin']*100:.1f}% | Match: {item['score']} | Deal: {ds_total}/100\n"
        f"{offer_str}"
        f"   GH URL: {item.get('gh_url','N/A')}\n"
        f"   US URL: {item['us_url']}\n"
    )


def _card_caption(i: int, item: dict) -> str:
    """
    Short caption for Telegram photo messages (≤1024 chars).
    Contains the essential deal info without the full guide text.
    """
    ds       = item.get("deal_score", {})
    ds_total = ds.get("total", 0)
    verdict  = ds.get("verdict", "")
    flags    = ds.get("flags", [])

    if item.get("very_fresh"):
        mode_tag = "[FRESH] "
    elif item.get("fresh"):
        mode_tag = "[RECIENTE] "
    elif item.get("proactive"):
        mode_tag = "[PROACTIVO] "
    else:
        mode_tag = ""

    dom     = item.get("days_on_market")
    dom_str = f" | {dom}d en venta" if dom else ""

    reverb_avg = item.get("reverb_sold_avg_usd")
    reverb_str = f"Reverb ref: ${reverb_avg:,.0f} USD\n" if reverb_avg else ""

    liq     = item.get("liquidity")
    liq_str = ""
    if liq:
        avg_d     = liq["avg_days_to_sell"]
        avg_d_str = "< 60" if avg_d < 3 else f"~{avg_d:.0f}"
        liq_str   = f"Liquidez: {avg_d_str}d | {liq['sell_rate']*100:.0f}% vendidas\n"

    sell_mxn     = item.get("suggested_sell_mxn", 0)
    sell_mxn_str = f"Venta MX: ${sell_mxn:,.0f} MXN\n" if sell_mxn else ""

    offer_str = ""
    if dom and dom >= 45:
        offer_str = f"OFERTA SUGERIDA: ${round(item['us_price_usd'] * 0.90):,} USD\n"

    flags_str  = f"{' | '.join(flags)}\n" if flags else ""
    target_str = "★ GUITARRA OBJETIVO (Fase 1)\n" if is_validation_target(item.get("gh_title", "")) else ""
    verdict_str = f"{verdict_emoji(verdict)}  ({ds_total}/100)\n" if verdict else ""

    cap = (
        f"{i}. {mode_tag}{item['gh_title']}\n"
        f"{verdict_str}"
        f"{target_str}"
        f"{flags_str}"
        f"Fuente: {fmt_source(item['us_source'])} [{item.get('us_condition','')}]{dom_str}\n"
        f"Compra: ${item['us_price_usd']:,.0f} USD | Landed: ${item['landed_cost_usd']:,.0f} USD\n"
        f"Benchmark: {benchmark_label(item)}\n"
        f"{sell_mxn_str}"
        f"{reverb_str}"
        f"{liq_str}"
        f"Margen: {item['margin']*100:.1f}% | Match: {item['score']} | Deal: {ds_total}/100\n"
        f"{offer_str}"
        f"US: {item['us_url']}"
    )
    # Hard truncate at Telegram's 1024-char caption limit
    if len(cap) > 1024:
        cap = cap[:1021] + "..."
    return cap


def build_header(opportunities, near_misses, ranked_matches,
                 gh_count: int, us_counts: dict) -> str:
    """Summary header + guide sent as the first text message of the report."""
    total_us  = sum(us_counts.values())
    count_str = ", ".join(f"{fmt_source(k)}: {v}" for k, v in us_counts.items())

    lines = [
        "LOLO MUSIC — Reporte de Arbitraje\n",
        f"Guitar's Home: {gh_count} guitarras activas",
        f"Fuentes US ({total_us}): {count_str}",
        f"Matches evaluados: {len(ranked_matches)}",
        f"Near misses (margen 20-30%): {len(near_misses)}",
        f"Oportunidades (margen 30%+): {len(opportunities)}\n",
        "Veredictos:",
        "  *** COMPRA AHORA *** = Deal score >= 75",
        "  >> REVISAR          = Deal score 50-74",
        "  -- ignorar          = Deal score < 50",
        "\nREGLA: Margen bruto >= 30% para proceder.",
    ]
    return "\n".join(lines)


def build_message(opportunities, near_misses, ranked_matches,
                  gh_count: int, us_counts: dict) -> str:
    """Legacy full-text report (used as fallback when no photo sending)."""
    total_us  = sum(us_counts.values())
    count_str = ", ".join(f"{fmt_source(k)}: {v}" for k, v in us_counts.items())

    lines = [
        "LOLO MUSIC — Reporte de Arbitraje\n",
        f"Guitar's Home: {gh_count} guitarras activas",
        f"Fuentes US ({total_us}): {count_str}",
        f"Matches evaluados: {len(ranked_matches)}",
        f"Near misses (margen 20-30%): {len(near_misses)}",
        f"Oportunidades (margen 30%+): {len(opportunities)}\n",
        "Veredictos:",
        "  *** COMPRA AHORA *** = Deal score >= 75, accion inmediata",
        "  >> REVISAR          = Deal score 50-74, evalua antes de comprar",
        "  -- ignorar          = Deal score < 50",
        "\nREGLA: Margen bruto >= 30% para proceder.\n",
    ]

    if opportunities:
        lines.append("OPORTUNIDADES:\n")
        for i, item in enumerate(opportunities, 1):
            lines.append(_card(i, item, full=True))
    elif near_misses:
        lines.append("Sin oportunidades claras. Near misses (20%+):\n")
        for i, item in enumerate(near_misses, 1):
            lines.append(_card(i, item, full=True))
    elif ranked_matches:
        lines.append("Sin 20%+. Top matches:\n")
        for i, item in enumerate(ranked_matches[:10], 1):
            lines.append(_card(i, item, full=False))
    else:
        lines.append("No se encontraron matches.")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Core matching logic (shared by reactive + proactive modes)
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_match(
    gh_title: str,
    gh_url: str,
    gh_price_mxn: float,
    us_item: dict,
    score: int,
    usd_mxn: float,
    logistics_usd: float,
    min_margin: float,
    min_match_score: int,
    price_ratio_min: float,
    price_ratio_max: float,
    sold_catalog: list,
    reverb_sold_cache: dict,
    gh_reverb_shop: str,
    proactive: bool = False,
    drop_urls: set = None,
    ph: "PriceHistory" = None,
) -> dict | None:
    """
    Evaluate a single GH guitar vs a US listing.
    Returns a match_data dict if it passes all filters, else None.
    """
    if score < min_match_score:
        return None

    us_price_usd = us_item["price_usd"]

    # Price ratio sanity check
    gh_usd_equiv = gh_price_mxn / usd_mxn if gh_price_mxn else us_price_usd * 1.3
    ratio = us_price_usd / gh_usd_equiv
    if not (price_ratio_min <= ratio <= price_ratio_max):
        return None

    landed_cost = calculate_landed_cost(us_price_usd, logistics_usd)

    # ── Benchmark selection (priority order) ─────────────────────────────────
    # 1. GH current listing price  (most reliable)
    # 2. GH Instagram sold price   (historical, good signal)
    # 3. Reverb sold avg            (reference only, not used for margin)
    benchmark_usd    = None
    benchmark_source = None
    benchmark_match  = ""

    if gh_price_mxn:
        benchmark_usd    = gh_price_mxn / usd_mxn
        benchmark_source = "gh_listing"
    else:
        # Try Instagram sold catalog
        hist = find_gh_historical_price(gh_title, sold_catalog, usd_mxn)
        if hist:
            benchmark_usd    = hist["price_usd"]
            benchmark_source = "instagram_sold"
            benchmark_match  = hist["match_title"]

    if not benchmark_usd:
        return None

    ML_COMMISSION = 0.17   # MercadoLibre 17%

    net_profit  = benchmark_usd - landed_cost
    margin      = net_profit / landed_cost if landed_cost > 0 else -1.0
    # Real margin after MercadoLibre 17% — this is the actual profit Ivan keeps
    net_after_ml = (benchmark_usd * (1 - ML_COMMISSION) - landed_cost) / landed_cost if landed_cost > 0 else -1.0
    opportunity = net_profit > 0 and margin >= min_margin

    # Reverb sold avg — secondary context only
    reverb_query = build_reverb_sold_query(gh_title)
    if reverb_query not in reverb_sold_cache:
        reverb_sold_cache[reverb_query] = fetch_reverb_sold_avg(
            reverb_query, max_results=20, gh_reverb_shop=gh_reverb_shop
        )
    reverb_avg = reverb_sold_cache[reverb_query]

    # All Original adjustment: if the GH benchmark is for an "all original" vintage guitar
    # but the US listing doesn't claim the same, cap the effective benchmark at Reverb sold avg.
    # Rationale: all-original premiums are 30–50% above non-original; we can't assume the US
    # listing shares that premium → use Reverb (market price for non-original) as ceiling.
    _all_orig_kw = {"all original", "all orig", "100% original", "all-original"}
    _bench_title_lc = (benchmark_match or gh_title).lower()
    _us_title_lc = us_item.get("title", "").lower()
    _gh_all_orig = any(kw in _bench_title_lc for kw in _all_orig_kw)
    _us_all_orig = any(kw in _us_title_lc for kw in _all_orig_kw)
    if _gh_all_orig and not _us_all_orig and reverb_avg and reverb_avg < benchmark_usd:
        benchmark_usd = reverb_avg
        net_profit    = benchmark_usd - landed_cost
        margin        = net_profit / landed_cost if landed_cost > 0 else -1.0
        net_after_ml  = (benchmark_usd * (1 - ML_COMMISSION) - landed_cost) / landed_cost if landed_cost > 0 else -1.0
        opportunity   = net_profit > 0 and margin >= min_margin

    # ── Reverb divergence cap ────────────────────────────────────────────────
    # If Reverb sold avg is known and significantly lower than GH benchmark,
    # the GH price is likely inflated (GH charges MX premium that US market
    # doesn't support). Cap the effective benchmark to avoid phantom margins.
    benchmark_capped = False
    if reverb_avg and benchmark_usd and reverb_avg < benchmark_usd * 0.70:
        capped = reverb_avg * 1.15
        if capped < benchmark_usd:
            benchmark_usd    = capped
            benchmark_capped = True
            net_profit  = benchmark_usd - landed_cost
            margin      = net_profit / landed_cost if landed_cost > 0 else -1.0
            net_after_ml = (benchmark_usd * (1 - ML_COMMISSION) - landed_cost) / landed_cost if landed_cost > 0 else -1.0
            opportunity = net_profit > 0 and margin >= min_margin

    # Days on market from price history DB
    us_url = us_item["url"]
    days_on_market = ph.get_days_on_market(us_url) if ph else None
    had_price_drop = bool(drop_urls and us_url in drop_urls)

    # Suggested MX sell price in MXN for the Telegram report
    # Priority: GH active listing price → Instagram benchmark × FX → 37% ROI estimate
    if gh_price_mxn:
        suggested_sell_mxn_val = int(gh_price_mxn)
    elif benchmark_source == "instagram_sold" and benchmark_usd:
        suggested_sell_mxn_val = int(round(benchmark_usd * usd_mxn / 1_000) * 1_000)
    else:
        suggested_sell_mxn_val = estimate_sell_mxn(landed_cost, usd_mxn)

    return {
        "gh_title":            gh_title,
        "gh_price_mxn":        gh_price_mxn,
        "gh_url":              gh_url,
        "us_title":            us_item["title"],
        "us_price_usd":        us_price_usd,
        "original_price_usd":  us_item.get("original_price_usd"),
        "on_sale":             us_item.get("on_sale", False),
        "discount_pct":        us_item.get("discount_pct", 0.0),
        "us_url":              us_url,
        "image_url":           us_item.get("image_url", ""),
        "us_source":           us_item.get("source", "unknown"),
        "us_condition":        us_item.get("condition", ""),
        "benchmark_usd":       round(benchmark_usd, 2),
        "benchmark_source":    benchmark_source,
        "benchmark_match":     benchmark_match,
        "suggested_sell_mxn":  suggested_sell_mxn_val,
        "reverb_sold_avg_usd": reverb_avg,
        "landed_cost_usd":     landed_cost,
        "margin":              margin,
        "net_margin_ml":       net_after_ml,
        "score":               score,
        "opportunity":         opportunity,
        "proactive":           proactive,
        "days_on_market":      days_on_market,
        "had_price_drop":      had_price_drop,
        "benchmark_capped":    benchmark_capped,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()

    bot_token       = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id         = os.getenv("TELEGRAM_CHAT_ID",   "").strip()
    usd_mxn         = float(os.getenv("USD_MXN",          "19.5"))
    logistics_usd   = float(os.getenv("LOGISTICS_USD",    "150.0"))
    min_margin      = float(os.getenv("MIN_MARGIN",        "0.30"))
    min_match_score = int(os.getenv("MIN_MATCH_SCORE",    "75"))
    gh_reverb_shop  = os.getenv("GH_REVERB_SHOP", "").strip()
    price_ratio_min = float(os.getenv("PRICE_RATIO_MIN",  "0.45"))
    price_ratio_max = float(os.getenv("PRICE_RATIO_MAX",  "2.00"))

    if not bot_token or not chat_id:
        raise ValueError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID en .env")

    # ── Load Instagram sold catalog + liquidity scores ────────────────────────
    sold_catalog = load_sold_catalog()
    full_history = load_full_history()
    print(f"Catalogo Instagram: {len(sold_catalog)} vendidas / {len(full_history)} total")

    liquidity_scores  = build_liquidity_scores(full_history)
    proactive_targets = build_proactive_targets(full_history, usd_mxn=usd_mxn)

    # Remove unique/signed/numbered targets — their IG price isn't replicable
    _unique_re = re.compile(
        r'one.of.a.kind|único|unico|1\s*of\s*1'
        r'|\bfirmad[oa]\b|\bsigned\s+by\b|\bautograph'
        r'|\b\d+\s*/\s*\d+\b|\b#\s*\d+\s+of\s+\d+'
        r'|\bprototipo\b|\bprototype\b',
        re.I
    )
    proactive_targets = [t for t in proactive_targets if not _unique_re.search(t["title"])]
    print(f"Targets proactivos: {len(proactive_targets)} | Modelos con score: {len(liquidity_scores)}")

    # ── Scrape US sources ─────────────────────────────────────────────────────
    print("\nScraping Dave's Guitar...")
    daves_items = scrape_daves()

    print("\nScraping Wildwood Guitars...")
    wildwood_items = scrape_wildwood()

    print("\nScraping Graysons Tune Town...")
    graysons_items = scrape_graysons()

    print("\nScraping Twin Town Guitars...")
    twin_town_items = scrape_twin_town()

    print("\nScraping Chicago Music Exchange...")
    cme_items = scrape_cme()

    print("\nScraping Cream City Music...")
    cream_items = scrape_cream_city()

    print("\nScraping Music Go Round...")
    mgr_items = scrape_music_go_round()

    print("\nScraping Elderly Instruments...")
    elderly_items = scrape_elderly()

    print("\nScraping Norman's Rare Guitars...")
    normans_items = scrape_normans()

    print("\nScraping Tone Shop Guitars...")
    toneshop_items = scrape_toneshop()

    print("\nScraping Retrofret Vintage Instruments...")
    retrofret_items = scrape_retrofret()

    # eBay, Guitar Center, Sam Ash blocked by bot detection — disabled
    ebay_items = []
    gc_items   = []
    samash_items = []

    # ── Scrape Guitar's Home (reactive mode) — must run BEFORE Reverb
    # so we can build dynamic queries from GH titles ──────────────────────
    print("\nScraping Guitar's Home...")
    gh_items = scrape_guitarshome()
    print(f"Guitar's Home: {len(gh_items)} guitarras")

    # Track GH listings across runs — detect real sell events (disappeared = sold).
    # This builds actual days-on-market data instead of Instagram approximations.
    newly_sold = ph.update_gh_listings(gh_items)
    if newly_sold:
        print(f"GH vendidas desde ultimo run: {len(newly_sold)}")
        for s in newly_sold:
            print(f"  VENDIDA en {s['days_listed']}d: {s['title'][:60]}")

    # Build dynamic Reverb queries from GH titles + proactive targets.
    # This surfaces Reverb listings that the static query list would miss
    # (e.g. "suhr modern plus" when only "suhr modern electric" is in the static list).
    dynamic_queries = []
    for gh_item in gh_items:
        q = build_reverb_sold_query(gh_item["title"])
        if q:
            dynamic_queries.append(q)
    for target in proactive_targets:
        q = build_reverb_sold_query(target["title"])
        if q:
            dynamic_queries.append(q)
    # Dedupe and limit to avoid excessive API calls (1 page each for dynamic queries)
    dynamic_queries = list(dict.fromkeys(dynamic_queries))  # dedupe preserving order
    print(f"\nReverb: {len(dynamic_queries)} dynamic queries from GH + proactive targets")

    print("\nScraping Reverb...")
    reverb_items = scrape_reverb(extra_queries=dynamic_queries)

    us_items = (
        daves_items + wildwood_items + graysons_items + twin_town_items +
        cme_items + cream_items + mgr_items + elderly_items +
        normans_items + toneshop_items + retrofret_items + reverb_items
    )

    us_counts = {
        "daves":          len(daves_items),
        "wildwood":       len(wildwood_items),
        "graysons":       len(graysons_items),
        "twin_town":      len(twin_town_items),
        "cme":            len(cme_items),
        "cream_city":     len(cream_items),
        "music_go_round": len(mgr_items),
        "elderly":        len(elderly_items),
        "normans":        len(normans_items),
        "toneshop":       len(toneshop_items),
        "retrofret":      len(retrofret_items),
        "reverb":         len(reverb_items),
    }

    print(f"\nTotal US items: {len(us_items)}")
    for src, cnt in us_counts.items():
        print(f"  {fmt_source(src)}: {cnt}")

    # ── Price history DB — record all US prices, detect drops ────────────────
    ph = PriceHistory()
    price_drops = ph.record_batch(us_items)
    drop_urls   = {d["url"] for d in price_drops}

    if price_drops:
        print(f"\nPrice drops detectados: {len(price_drops)}")
        for d in price_drops[:5]:
            print(f"  {d['title'][:50]} ${d['old_price']:,.0f} → ${d['new_price']:,.0f} (-{d['drop_pct']}%)")

    db_stats = ph.summary()
    print(f"Price DB: {db_stats['unique_listings']} listings, {db_stats['total_observations']} observaciones")

    reverb_sold_cache: dict = {}
    ranked_matches: list  = []
    opportunities:  list  = []
    near_misses:    list  = []
    buy_now:        list  = []

    # ── MODE A: REACTIVE — match GH current listings vs US sources ────────────
    # Track US URLs already used — one US listing can only be one opportunity
    seen_us_urls: set = set()

    print("\n--- Modo Reactivo (GH listings actuales) ---")
    for gh_item in gh_items:
        if _unique_re.search(gh_item["title"]):
            continue
        all_matches = find_all_matches(gh_item, us_items, min_score=min_match_score)
        if not all_matches:
            continue

        for us_match, score in all_matches:
            if us_match["url"] in seen_us_urls:
                continue

            result = evaluate_match(
                gh_title        = gh_item["title"],
                gh_url          = gh_item["url"],
                gh_price_mxn    = gh_item["price_mxn"],
                us_item         = us_match,
                score           = score,
                usd_mxn         = usd_mxn,
                logistics_usd   = logistics_usd,
                min_margin      = min_margin,
                min_match_score = min_match_score,
                price_ratio_min = price_ratio_min,
                price_ratio_max = price_ratio_max,
                sold_catalog    = sold_catalog,
                reverb_sold_cache = reverb_sold_cache,
                gh_reverb_shop  = gh_reverb_shop,
                proactive       = False,
                drop_urls       = drop_urls,
                ph              = ph,
            )
            if result:
                seen_us_urls.add(us_match["url"])
                liq = get_liquidity(result["gh_title"], liquidity_scores, ph=ph)
                result["liquidity"] = liq

                # Liquidity gate: skip if proven low demand
                if not passes_liquidity_gate(liq, proactive=False):
                    continue

                ds = compute_deal_score(
                    margin             = result["margin"],
                    match_score        = result["score"],
                    liquidity          = liq,
                    on_sale            = result["on_sale"],
                    had_price_drop     = result["had_price_drop"],
                    days_on_market     = result["days_on_market"],
                    source             = result["us_source"],
                    condition          = result.get("us_condition", ""),
                    aging_tier         = us_match.get("aging_tier"),
                    flame_top          = us_match.get("flame_top"),
                    has_brazilian      = us_match.get("has_brazilian", False),
                    has_mods           = us_match.get("has_mods", False),
                    no_coa             = us_match.get("no_coa", False),
                    has_ohsc           = us_match.get("has_ohsc", False),
                    is_weight_relieved = us_match.get("is_weight_relieved", False),
                    offers_enabled     = us_match.get("offers_enabled", False),
                    drop_velocity      = ph.get_drop_velocity(us_match.get("url", "")),
                    benchmark_source   = result.get("benchmark_source", ""),
                    benchmark_capped   = result.get("benchmark_capped", False),
                )

                # Match confidence gate: low match can never be BUY NOW
                verdict = ds["verdict"]
                if verdict == VERDICT_BUY and result["score"] < MIN_MATCH_BUY_NOW:
                    ds["verdict"] = VERDICT_REVIEW
                    verdict = VERDICT_REVIEW

                result["deal_score"] = ds
                ranked_matches.append(result)
                if verdict == VERDICT_BUY:
                    buy_now.append(result)
                    opportunities.append(result)
                elif result["opportunity"]:
                    opportunities.append(result)
                elif result["margin"] >= 0.20:
                    near_misses.append(result)

    print(f"Reactive: {len(ranked_matches)} matches, {len(opportunities)} oportunidades")

    # ── MODE A.5: FRESH — recent Instagram posts (< 48hrs) as high-confidence targets ─
    # A fresh GH Instagram post means: GH has the guitar NOW, price is current,
    # demand is proven. We treat these like reactive matches but with lower margin
    # threshold (25% instead of 30%) because the benchmark is very reliable.
    fresh_posts = get_fresh_posts(full_history, hours=48)
    # Also include posts up to 7 days old — GH posts ~1/day, a week covers latest inventory
    recent_posts = get_fresh_posts(full_history, hours=168)
    # Use the wider window but tag only < 48hrs as "FRESH"
    fresh_titles = {p["title"] for p in fresh_posts}

    if recent_posts and us_items:
        print(f"\n--- Modo Fresh ({len(fresh_posts)} posts < 48hrs, {len(recent_posts)} < 7 dias) ---")
        seen_us_urls_fresh = {m["us_url"] for m in ranked_matches}
        fresh_min_margin = 0.25  # lower threshold for fresh intelligence

        for post in recent_posts:
            title = post["title"]
            price_mxn = post.get("price_mxn") or 0
            price_usd_raw = post.get("price_usd")
            is_very_fresh = title in fresh_titles

            # Skip if already matched in reactive mode (GH web listing = same guitar)
            # We check by fuzzy matching the fresh title against existing matches
            already_covered = False
            for existing in ranked_matches:
                if fuzz.token_set_ratio(
                    title.lower(), existing["gh_title"].lower()
                ) >= 85:
                    already_covered = True
                    break
            if already_covered:
                continue

            # Create synthetic GH item from Instagram post
            if price_mxn:
                gh_price = price_mxn
            elif price_usd_raw:
                gh_price = price_usd_raw * usd_mxn
            else:
                continue

            synthetic_gh = {"title": title, "price_mxn": gh_price, "url": ""}
            all_matches = find_all_matches(synthetic_gh, us_items, min_score=min_match_score)
            if not all_matches:
                continue

            for us_match, score in all_matches:
                if us_match["url"] in seen_us_urls_fresh:
                    continue

                result = evaluate_match(
                    gh_title        = title,
                    gh_url          = post.get("url", ""),
                    gh_price_mxn    = 0,
                    us_item         = us_match,
                    score           = score,
                    usd_mxn         = usd_mxn,
                    logistics_usd   = logistics_usd,
                    min_margin      = fresh_min_margin,
                    min_match_score = min_match_score,
                    price_ratio_min = price_ratio_min,
                    price_ratio_max = price_ratio_max,
                    sold_catalog    = full_history,
                    reverb_sold_cache = reverb_sold_cache,
                    gh_reverb_shop  = gh_reverb_shop,
                    proactive       = True,
                    drop_urls       = drop_urls,
                    ph              = ph,
                )
                if result:
                    result["fresh"] = True
                    result["very_fresh"] = is_very_fresh
                    seen_us_urls_fresh.add(us_match["url"])
                    seen_us_urls.add(us_match["url"])
                    liq = get_liquidity(result["gh_title"], liquidity_scores, ph=ph)
                    result["liquidity"] = liq

                    if not passes_liquidity_gate(liq, proactive=True):
                        continue

                    ds = compute_deal_score(
                        margin             = result["margin"],
                        match_score        = result["score"],
                        liquidity          = liq,
                        on_sale            = result["on_sale"],
                        had_price_drop     = result["had_price_drop"],
                        days_on_market     = result["days_on_market"],
                        source             = result["us_source"],
                        condition          = result.get("us_condition", ""),
                        aging_tier         = us_match.get("aging_tier"),
                        flame_top          = us_match.get("flame_top"),
                        has_brazilian      = us_match.get("has_brazilian", False),
                        has_mods           = us_match.get("has_mods", False),
                        no_coa             = us_match.get("no_coa", False),
                        has_ohsc           = us_match.get("has_ohsc", False),
                        is_weight_relieved = us_match.get("is_weight_relieved", False),
                        offers_enabled     = us_match.get("offers_enabled", False),
                        drop_velocity      = ph.get_drop_velocity(us_match.get("url", "")),
                        fresh_post         = is_very_fresh,
                        benchmark_source   = result.get("benchmark_source", ""),
                        benchmark_capped   = result.get("benchmark_capped", False),
                    )

                    verdict = ds["verdict"]
                    if verdict == VERDICT_BUY and result["score"] < MIN_MATCH_BUY_NOW:
                        ds["verdict"] = VERDICT_REVIEW
                        verdict = VERDICT_REVIEW

                    result["deal_score"] = ds
                    ranked_matches.append(result)
                    if verdict == VERDICT_BUY:
                        buy_now.append(result)
                        opportunities.append(result)
                    elif result["opportunity"]:
                        opportunities.append(result)
                    elif result["margin"] >= 0.20:
                        near_misses.append(result)

        fresh_matches = sum(1 for m in ranked_matches if m.get("fresh"))
        print(f"Fresh: {fresh_matches} matches from recent IG posts")

    # ── MODE B: PROACTIVE — search US sources for GH's historical best sellers ─
    # Uses Instagram sold catalog to find guitars GH sells well even if not
    # currently listed on their website.
    if proactive_targets and us_items:
        print(f"\n--- Modo Proactivo ({len(proactive_targets)} targets del catalogo IG) ---")
        seen_us_urls = {m["us_url"] for m in ranked_matches}

        for target in proactive_targets:
            gh_title = target["title"]
            gh_price_mxn = target["avg_price_usd"] * usd_mxn

            synthetic_gh = {"title": gh_title, "price_mxn": gh_price_mxn, "url": ""}
            all_matches = find_all_matches(synthetic_gh, us_items, min_score=min_match_score)
            if not all_matches:
                continue

            for us_match, score in all_matches:
                if us_match["url"] in seen_us_urls:
                    continue

                result = evaluate_match(
                    gh_title        = gh_title,
                    gh_url          = "",
                    gh_price_mxn    = 0,
                    us_item         = us_match,
                    score           = score,
                    usd_mxn         = usd_mxn,
                    logistics_usd   = logistics_usd,
                    min_margin      = min_margin,
                    min_match_score = min_match_score,
                    price_ratio_min = price_ratio_min,
                    price_ratio_max = price_ratio_max,
                    sold_catalog    = full_history,
                    reverb_sold_cache = reverb_sold_cache,
                    gh_reverb_shop  = gh_reverb_shop,
                    proactive       = True,
                    drop_urls       = drop_urls,
                    ph              = ph,
                )
                if result:
                    liq = get_liquidity(result["gh_title"], liquidity_scores, ph=ph)
                    result["liquidity"] = liq

                    if not passes_liquidity_gate(liq, proactive=True):
                        continue

                    ds = compute_deal_score(
                        margin             = result["margin"],
                        match_score        = result["score"],
                        liquidity          = liq,
                        on_sale            = result["on_sale"],
                        had_price_drop     = result["had_price_drop"],
                        days_on_market     = result["days_on_market"],
                        source             = result["us_source"],
                        condition          = result.get("us_condition", ""),
                        aging_tier         = us_match.get("aging_tier"),
                        flame_top          = us_match.get("flame_top"),
                        has_brazilian      = us_match.get("has_brazilian", False),
                        has_mods           = us_match.get("has_mods", False),
                        no_coa             = us_match.get("no_coa", False),
                        has_ohsc           = us_match.get("has_ohsc", False),
                        is_weight_relieved = us_match.get("is_weight_relieved", False),
                        offers_enabled     = us_match.get("offers_enabled", False),
                        drop_velocity      = ph.get_drop_velocity(us_match.get("url", "")),
                        benchmark_source   = result.get("benchmark_source", ""),
                        benchmark_capped   = result.get("benchmark_capped", False),
                    )

                    verdict = ds["verdict"]
                    if verdict == VERDICT_BUY and result["score"] < MIN_MATCH_BUY_NOW:
                        ds["verdict"] = VERDICT_REVIEW
                        verdict = VERDICT_REVIEW

                    result["deal_score"] = ds
                    seen_us_urls.add(us_match["url"])
                    ranked_matches.append(result)
                    if result["opportunity"]:
                        opportunities.append(result)
                    elif result["margin"] >= 0.20:
                        near_misses.append(result)

        print(f"Proactive added: {sum(1 for m in ranked_matches if m.get('proactive'))} matches")

    # ── Sort results by deal score ────────────────────────────────────────────
    def _ds(x):
        return x.get("deal_score", {}).get("total", 0)

    ranked_matches.sort(key=lambda x: (_ds(x), x["margin"]), reverse=True)
    opportunities.sort(key=lambda x: (_ds(x), x["margin"]), reverse=True)
    near_misses.sort(key=lambda x: (_ds(x), x["margin"]), reverse=True)
    buy_now.sort(key=lambda x: _ds(x), reverse=True)

    # ── Save snapshots ────────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    save_json(f"gh_items_{ts}.json",       gh_items)
    save_json(f"us_daves_{ts}.json",        daves_items)
    save_json(f"us_wildwood_{ts}.json",     wildwood_items)
    save_json(f"us_graysons_{ts}.json",     graysons_items)
    save_json(f"us_twin_town_{ts}.json",    twin_town_items)
    save_json(f"us_cme_{ts}.json",          cme_items)
    save_json(f"us_cream_{ts}.json",        cream_items)
    save_json(f"us_mgr_{ts}.json",          mgr_items)
    save_json(f"us_elderly_{ts}.json",      elderly_items)
    save_json(f"us_ebay_{ts}.json",         ebay_items)
    save_json(f"us_gc_{ts}.json",           gc_items)
    save_json(f"us_samash_{ts}.json",       samash_items)
    save_json(f"us_reverb_{ts}.json",       reverb_items)
    save_json(f"ranked_matches_{ts}.json",  ranked_matches)
    save_json(f"near_misses_{ts}.json",     near_misses)
    save_json(f"opportunities_{ts}.json",   opportunities)

    # ── Console summary ───────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"RESULTADOS")
    print(f"{'='*50}")
    print(f"Guitar's Home:      {len(gh_items)}")
    for src, cnt in us_counts.items():
        print(f"  {fmt_source(src)}: {cnt}")
    print(f"Ranked matches:     {len(ranked_matches)}")
    print(f"Near misses (20%+): {len(near_misses)}")
    print(f"Oportunidades:      {len(opportunities)}")

    if opportunities:
        print("\nTop oportunidades:")
        for item in opportunities[:5]:
            sale = on_sale_tag(item)
            mode = "[P] " if item.get("proactive") else ""
            cond = item.get("us_condition", "")
            cond_str = f" [{cond}]" if cond else ""
            dom = item.get("days_on_market")
            offer_str = ""
            if dom and dom >= 45:
                offer_price = round(item["us_price_usd"] * 0.90)
                offer_str = f"\n    OFERTA SUGERIDA: ${offer_price:,.0f} ({dom}d en venta — vendedor motivado)"
            print(
                f"  {mode}{item['gh_title']}{(' ' + sale) if sale else ''}\n"
                f"    Compra: {fmt_source(item['us_source'])}{cond_str} "
                f"${item['us_price_usd']:,.0f} | "
                f"Venta: {benchmark_label(item)}\n"
                f"    Landed: ${item['landed_cost_usd']:,.0f} | "
                f"Margen: {item['margin']*100:.1f}%"
                f"{offer_str}\n"
                f"    US:  {item['us_url']}\n"
                f"    GH:  {item.get('gh_url', 'N/A')}"
            )

    # ── Telegram: BUY NOW urgent alerts (separate message, sent first) ───────
    # Dedup: only send alerts for deals not previously alerted at same verdict level.
    # A deal upgrading from REVIEW → BUY NOW gets a new alert.
    ph.cleanup_old_alerts(days=30)
    sent_count = 0
    if buy_now:
        for item in buy_now[:3]:
            us_url = item["us_url"]
            if ph.was_alerted(us_url, "BUY NOW"):
                continue
            ds = item.get("deal_score", {})
            cond = item.get("us_condition", "")
            cond_str = f" [{cond}]" if cond else ""
            sale_str = ""
            if item.get("on_sale") and item.get("discount_pct", 0) > 0:
                sale_str = f"\nOFERTA: -{item['discount_pct']:.0f}% (era ${item.get('original_price_usd', 0):,.0f})"
            offer_str = ""
            if item.get("days_on_market", 0) >= 45:
                suggested_offer = int(item["us_price_usd"] * 0.90)
                offer_str = f"\nOferta sugerida: ${suggested_offer:,} USD (90%)"
            gh_url = item.get("gh_url", "")
            gh_link = f"\nGH: {gh_url}" if gh_url else ""
            caption = (
                f"*** COMPRA AHORA ***\n\n"
                f"{item['gh_title']}\n"
                f"Score: {ds.get('total',0)}/100 | Margen: {item['margin']*100:.1f}%{cond_str}\n"
                f"Compra: {fmt_source(item['us_source'])} ${item['us_price_usd']:,.0f} USD\n"
                f"Venta MX: ${item.get('suggested_sell_mxn', 0):,} MXN\n"
                f"Benchmark: {benchmark_label(item)}"
                f"{sale_str}{offer_str}\n"
                f"{' | '.join(ds.get('flags',[]))}\n"
                f"URL: {item['us_url']}{gh_link}"
            )
            img = item.get("image_url", "")
            if img:
                send_telegram_photo(bot_token=bot_token, chat_id=chat_id,
                                    photo_url=img, caption=caption)
            else:
                send_telegram_message(bot_token=bot_token, chat_id=chat_id,
                                      text=caption)
            ph.record_alert(us_url, ds.get("total", 0), "BUY NOW")
            sent_count += 1
        if sent_count:
            print(f"ALERTA BUY NOW enviada: {sent_count} deals nuevos")
        elif buy_now:
            print(f"BUY NOW: {len(buy_now)} deals (ya alertados previamente)")

    # ── Telegram: price drop alerts ───────────────────────────────────────────
    if price_drops:
        drop_lines = ["PRICE DROPS DETECTADOS\n"]
        for d in price_drops[:5]:
            drop_lines.append(
                f"  {d['title'][:50]}\n"
                f"  ${d['old_price']:,.0f} -> ${d['new_price']:,.0f} (-{d['drop_pct']}%)\n"
                f"  {fmt_source(d['source'])} | {d['url']}\n"
            )
        send_telegram_message(bot_token=bot_token, chat_id=chat_id, text="\n".join(drop_lines))

    # ── Telegram: full report ─────────────────────────────────────────────────
    # Header (summary stats) as text first
    header = build_header(opportunities, near_misses, ranked_matches,
                          len(gh_items), us_counts)
    send_telegram_message(bot_token=bot_token, chat_id=chat_id, text=header)

    # Actionable items: each gets a photo preview + caption
    actionable = opportunities or near_misses
    if actionable:
        section_label = "OPORTUNIDADES:" if opportunities else "Near misses (20%+):"
        send_telegram_message(bot_token=bot_token, chat_id=chat_id, text=section_label)
        for i, item in enumerate(actionable, 1):
            caption = _card_caption(i, item)
            img     = item.get("image_url", "")
            if img:
                send_telegram_photo(bot_token=bot_token, chat_id=chat_id,
                                    photo_url=img, caption=caption)
            else:
                send_telegram_message(bot_token=bot_token, chat_id=chat_id, text=caption)
    elif ranked_matches:
        # No actionable items — send top matches as plain text (informativo)
        top_lines = ["Sin 20%+. Top matches:\n"]
        for i, item in enumerate(ranked_matches[:10], 1):
            top_lines.append(_card(i, item, full=False))
        send_telegram_message(bot_token=bot_token, chat_id=chat_id,
                              text="\n".join(top_lines))
    else:
        send_telegram_message(bot_token=bot_token, chat_id=chat_id,
                              text="No se encontraron matches.")

    print("\nNotificacion enviada.")

    # ── Close price history DB ────────────────────────────────────────────────
    ph.close()


if __name__ == "__main__":
    load_dotenv(dotenv_path=".env")
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.getenv("TELEGRAM_CHAT_ID",   "").strip()
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        if bot_token and chat_id:
            try:
                send_telegram_message(bot_token, chat_id, f"Lolo Music ERROR\n\n{e}")
            except Exception:
                pass
