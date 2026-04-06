"""
deal_score.py
─────────────
Converts raw match data into a single 0-100 deal score with a clear
BUY / REVIEW / PASS verdict.

Scoring breakdown (total = 100 points):

  40 pts — Margin
            Full 40 if margin ≥ 40%
            Scaled linearly between 20% (0 pts) and 40% (40 pts)

  20 pts — Liquidity (how fast this model sells at GH)
            Full 20 if avg days to sell ≤ 15
            Scaled down to 0 if ≥ 90 days or no data (neutral 10 pts)

  15 pts — Match confidence
            Full 15 if fuzzy score = 100
            Scaled linearly from score 75 (0 pts) to 100 (15 pts)

  10 pts — On sale / price drop bonus
            +10 if item is on sale OR had a recent price drop in DB
            +5  if only one of the above

  10 pts — Days on market (seller motivation)
            +10 if listing has been on market ≥ 14 days (motivated seller)
            +5  if 7-13 days
             0  if <7 days or unknown

   5 pts — Source trust
            +5 for established shops (Dave's, CME, Cream City, Elderly)
            +3 for eBay / Guitar Center / Sam Ash
            +1 for Reverb (individual sellers, more variable)

Verdict:
  ≥ 75 → BUY NOW    — send urgent Telegram alert
  50-74 → REVIEW    — include in normal report
  < 50 → PASS       — log only, no notification
"""

from typing import Optional

# Condition score multipliers — imported from scrapers at runtime to avoid circular import
_CONDITION_SCORE = {
    "Mint":      1.00,
    "Excellent": 0.95,
    "VG+":       0.85,
    "VG":        0.75,
    "Good+":     0.60,
    "Good":      0.50,
    "Fair":      0.30,
    "Poor":      0.10,
}


# ── Weights ───────────────────────────────────────────────────────────────────

WEIGHT_MARGIN      = 40
WEIGHT_LIQUIDITY   = 20
WEIGHT_MATCH       = 15
WEIGHT_SALE        = 10
WEIGHT_DOM         = 10   # days on market
WEIGHT_SOURCE      = 5

MARGIN_MIN  = 0.20   # below this = 0 pts
MARGIN_MAX  = 0.40   # above this = full pts

LIQ_FAST    = 15     # days — full liquidity points
LIQ_SLOW    = 90     # days — zero liquidity points

MATCH_MIN   = 75
MATCH_MAX   = 100

SOURCE_TRUST = {
    "daves":          5,
    "cme":            5,
    "cream_city":     5,
    "elderly":        5,
    "wildwood":       5,   # boutique dealer, Wildwood Spec exclusives
    "graysons":       5,   # boutique dealer, Custom Shop Showroom
    "twin_town":      4,   # Minneapolis boutique, good used inventory
    "guitar_center":  3,
    "ebay":           3,
    "sam_ash":        3,
    "music_go_round": 3,
    "reverb":         1,
}

VERDICT_BUY    = "BUY NOW"
VERDICT_REVIEW = "REVIEW"
VERDICT_PASS   = "PASS"

BUY_THRESHOLD    = 75
REVIEW_THRESHOLD = 50


# ── Scoring ───────────────────────────────────────────────────────────────────

def _score_margin(margin: float) -> float:
    if margin <= MARGIN_MIN:
        return 0.0
    if margin >= MARGIN_MAX:
        return float(WEIGHT_MARGIN)
    pct = (margin - MARGIN_MIN) / (MARGIN_MAX - MARGIN_MIN)
    return round(pct * WEIGHT_MARGIN, 1)


def _score_liquidity(liquidity: Optional[dict]) -> float:
    if not liquidity:
        return WEIGHT_LIQUIDITY * 0.5   # neutral if no data yet

    avg_days   = liquidity.get("avg_days_to_sell", LIQ_SLOW)
    sell_rate  = liquidity.get("sell_rate", 0.5)

    if avg_days <= LIQ_FAST:
        speed = 1.0
    elif avg_days >= LIQ_SLOW:
        speed = 0.0
    else:
        speed = 1.0 - (avg_days - LIQ_FAST) / (LIQ_SLOW - LIQ_FAST)

    # Blend speed and sell rate
    combined = (speed * 0.6) + (sell_rate * 0.4)
    return round(combined * WEIGHT_LIQUIDITY, 1)


def _score_match(match_score: int) -> float:
    if match_score >= MATCH_MAX:
        return float(WEIGHT_MATCH)
    if match_score <= MATCH_MIN:
        return 0.0
    pct = (match_score - MATCH_MIN) / (MATCH_MAX - MATCH_MIN)
    return round(pct * WEIGHT_MATCH, 1)


def _score_sale(on_sale: bool, had_price_drop: bool) -> float:
    if on_sale and had_price_drop:
        return float(WEIGHT_SALE)
    if on_sale or had_price_drop:
        return WEIGHT_SALE * 0.5
    return 0.0


def _score_days_on_market(days_on_market: Optional[int]) -> float:
    if days_on_market is None:
        return 0.0
    if days_on_market >= 14:
        return float(WEIGHT_DOM)
    if days_on_market >= 7:
        return WEIGHT_DOM * 0.5
    return 0.0


def _score_source(source: str) -> float:
    return float(SOURCE_TRUST.get(source, 1))


def _score_spec(
    aging_tier: Optional[int] = None,
    flame_top: Optional[str] = None,
    has_brazilian: bool = False,
    has_mods: bool = False,
    no_coa: bool = False,
) -> float:
    """
    Spec bonus/penalty based on physical attributes that drive MX resale price.

    Bonuses (guitar is worth more in MX than the average of its model):
      Brazilian rosewood fretboard: +8  (CITES-protected, extreme scarcity)
      Heavy Aged / Ultra Heavy Aged: +5 (most desirable Murphy Lab tier)
      Light/Ultra Light Aged: +2        (above VOS baseline)
      Flame/figured top: +3             (visual premium, sells faster)

    Penalties (guitar is worth less than its model average):
      Modifications (pickups replaced, refret, etc.): -8
        Collectors pay for originality — mods destroy value in this segment.
      Missing/No COA: -10
        Without the certificate, a Custom Shop loses 15-20% of value in MX.

    Returns a value typically between -15 and +10.
    Total deal score is still capped at 0-100 after this is applied.
    """
    score = 0.0

    # Aging tier premium (Murphy Lab levels)
    if aging_tier is not None:
        if aging_tier >= 4:   # Heavy Aged / Ultra Heavy Aged
            score += 5
        elif aging_tier >= 2: # Light Aged / Ultra Light Aged
            score += 2
        # tier 0-1 (VOS / generic aged) → no bonus, it's the Custom Shop baseline

    # Figured top visual premium
    if flame_top == "figured":
        score += 3

    # Brazilian rosewood — most extreme price driver
    if has_brazilian:
        score += 8

    # Modification penalty — destroys collectible value
    if has_mods:
        score -= 8

    # Missing COA penalty — 15-20% value hit in MX collector market
    if no_coa:
        score -= 10

    return score


# ── Main function ─────────────────────────────────────────────────────────────

def compute_deal_score(
    margin: float,
    match_score: int,
    liquidity: Optional[dict] = None,
    on_sale: bool = False,
    had_price_drop: bool = False,
    days_on_market: Optional[int] = None,
    source: str = "reverb",
    condition: str = "",
    # Spec attributes — affect MX resale value beyond the base model price
    aging_tier: Optional[int] = None,   # 0-5 from detect_aging_tier()
    flame_top: Optional[str] = None,    # 'figured', 'plain', or None
    has_brazilian: bool = False,        # Brazilian rosewood fretboard
    has_mods: bool = False,             # pickups replaced, refret, hardware swap
    no_coa: bool = False,               # certificate of authenticity missing
) -> dict:
    """
    Compute a 0-100 deal score and return a full breakdown.

    Args:
        margin:          Net margin as decimal (e.g. 0.35 = 35%)
        match_score:     Fuzzy match score 0-100
        liquidity:       Dict from catalog.get_liquidity() or None
        on_sale:         True if the listing has a sale/discount price
        had_price_drop:  True if price_history detected a recent drop
        days_on_market:  Days since listing was first seen in DB
        source:          Source store key (daves, cme, reverb, etc.)

    Returns:
        {
          "total":          int,      # 0-100
          "verdict":        str,      # BUY NOW / REVIEW / PASS
          "breakdown": {
            "margin":       float,
            "liquidity":    float,
            "match":        float,
            "sale":         float,
            "dom":          float,
            "source":       float,
          },
          "flags": list[str]          # human-readable notes
        }
    """
    s_margin  = _score_margin(margin)
    s_liq     = _score_liquidity(liquidity)
    s_match   = _score_match(match_score)
    s_sale    = _score_sale(on_sale, had_price_drop)
    s_dom     = _score_days_on_market(days_on_market)
    s_source  = _score_source(source)
    s_spec    = _score_spec(aging_tier, flame_top, has_brazilian, has_mods, no_coa)

    total = int(round(s_margin + s_liq + s_match + s_sale + s_dom + s_source + s_spec))
    total = max(0, min(100, total))

    # Condition penalty: deduct points for below-excellent condition
    cond_score = _CONDITION_SCORE.get(condition, None)
    if cond_score is not None and cond_score < _CONDITION_SCORE["Excellent"]:
        penalty = int(round((1.0 - cond_score) * 20))   # max -20 pts for "Poor"
        total = max(0, total - penalty)

    if total >= BUY_THRESHOLD:
        verdict = VERDICT_BUY
    elif total >= REVIEW_THRESHOLD:
        verdict = VERDICT_REVIEW
    else:
        verdict = VERDICT_PASS

    # Build human-readable flags
    flags = []
    if margin >= 0.40:
        flags.append(f"Margen excelente {margin*100:.0f}%")
    elif margin >= 0.30:
        flags.append(f"Margen solido {margin*100:.0f}%")
    if on_sale:
        flags.append("EN OFERTA")
    if had_price_drop:
        flags.append("PRECIO BAJO RECIENTEMENTE")
    if days_on_market and days_on_market >= 14:
        flags.append(f"Lleva {days_on_market} dias en venta (vendedor motivado)")
    if liquidity and liquidity.get("avg_days_to_sell", 999) <= 20:
        flags.append(f"Vende rapido (~{liquidity['avg_days_to_sell']:.0f} dias en GH)")
    if liquidity and liquidity.get("sell_rate", 0) >= 0.80:
        flags.append(f"Alta demanda en MX ({liquidity['sell_rate']*100:.0f}% vendidas)")
    if has_brazilian:
        flags.append("BRAZILIAN ROSEWOOD (prima maxima)")
    if aging_tier is not None and aging_tier >= 4:
        labels = {4: "Heavy Aged", 5: "Ultra Heavy Aged"}
        flags.append(f"{labels.get(aging_tier, 'Heavy Aged')} (+prima Murphy Lab)")
    if flame_top == "figured":
        flags.append("Flame/Figured Top (+visual premium)")
    if no_coa:
        flags.append("SIN COA — valor reducido 15-20%")
    if has_mods:
        flags.append("MODIFICADA — coleccionistas pagan menos")
    if condition:
        flags.append(f"Condicion: {condition}")
    if days_on_market and days_on_market >= 45:
        flags.append(f"OFERTA SUGERIDA: ver precio negociado")

    return {
        "total":   total,
        "verdict": verdict,
        "breakdown": {
            "margin":    s_margin,
            "liquidity": s_liq,
            "match":     s_match,
            "sale":      s_sale,
            "dom":       s_dom,
            "source":    s_source,
            "spec":      s_spec,
        },
        "flags": flags,
    }


def verdict_emoji(verdict: str) -> str:
    return {
        VERDICT_BUY:    "*** COMPRA AHORA ***",
        VERDICT_REVIEW: ">> REVISAR",
        VERDICT_PASS:   "-- ignorar",
    }.get(verdict, verdict)


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_cases = [
        dict(margin=0.38, match_score=94, on_sale=True,  had_price_drop=True,  days_on_market=21, source="daves",
             liquidity={"avg_days_to_sell": 12, "sell_rate": 0.85}),
        dict(margin=0.28, match_score=82, on_sale=False, had_price_drop=False, days_on_market=3,  source="reverb",
             liquidity={"avg_days_to_sell": 45, "sell_rate": 0.60}),
        dict(margin=0.18, match_score=76, on_sale=False, had_price_drop=False, days_on_market=None, source="ebay",
             liquidity=None),
    ]

    for t in test_cases:
        result = compute_deal_score(**t)
        print(f"{verdict_emoji(result['verdict'])}  Score: {result['total']}/100")
        print(f"  Margin: {t['margin']*100:.0f}% | Match: {t['match_score']} | "
              f"DOM: {t.get('days_on_market','?')}d | Source: {t['source']}")
        print(f"  Breakdown: {result['breakdown']}")
        if result["flags"]:
            print(f"  Flags: {', '.join(result['flags'])}")
        print()
