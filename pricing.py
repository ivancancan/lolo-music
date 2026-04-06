# ─────────────────────────────────────────────────────────────────────────────
# Lolo Music — Pricing Engine
#
# Business rules (validated against 3 real transactions):
#   Case 1: Buy $301,431 MXN → Sell $435,999 MXN  (ROI 44.6%)
#   Case 2: Buy $334,000 MXN → Sell $435,999 MXN  (ROI 30.5%)
#   Case 3: Buy  $97,136 MXN → Sell $132,999 MXN  (ROI 36.9%)
#
#  • Landed cost  = purchase price + flat logistics fee
#  • Benchmark    = GH listing price in MXN → converted to USD  (PRIMARY)
#                   GH Instagram history price                   (FALLBACK)
#  • Margin       = (benchmark_usd − landed_cost) / landed_cost
#  • Opportunity  = margin ≥ MIN_MARGIN  (default 30%)
#
# Reverb sold avg is shown as *context only* — never used to compute margin.
# ─────────────────────────────────────────────────────────────────────────────

LOGISTICS_USD: float = 150.0   # Flat shipping + insurance + handling (US → MX)
ROI_TARGET:    float = 0.37    # Validated average from real cases (30-45% range)


def calculate_landed_cost(
    purchase_usd: float,
    logistics_usd: float = LOGISTICS_USD,
) -> float:
    """
    Total acquisition cost in USD.
    Formula: purchase_price + logistics_fee
    No IVA or import duties applied (excluded per business rules).
    """
    return purchase_usd + logistics_usd


def calculate_net_margin(
    gh_benchmark_usd: float,
    landed_cost_usd: float,
) -> float:
    """
    Net margin using GH price as benchmark (not Reverb).
    margin = (gh_benchmark − landed_cost) / landed_cost
    """
    if landed_cost_usd <= 0:
        return -1.0
    return (gh_benchmark_usd - landed_cost_usd) / landed_cost_usd


def is_opportunity(
    gh_benchmark_usd: float,
    landed_cost_usd: float,
    min_margin: float = 0.30,
) -> bool:
    """
    True when:
      1. gh_benchmark − landed_cost > 0   (positive net profit)
      2. net margin ≥ min_margin           (default: 30%)
    """
    net_profit = gh_benchmark_usd - landed_cost_usd
    if net_profit <= 0:
        return False
    return (net_profit / landed_cost_usd) >= min_margin


def estimate_sell_mxn(
    landed_cost_usd: float,
    usd_mxn: float,
    roi: float = ROI_TARGET,
) -> int:
    """
    For proactive mode: estimate MX sell price when no GH listing/history exists.
    Rounds to nearest $1,000 MXN (typical GH pricing pattern).

    Based on real cases: avg ROI = 37%, range 30-45%.
    Example: landed $5,850 USD × 17.2 FX × 1.37 → ~$137,700 MXN → $138,000
    """
    raw = landed_cost_usd * (1 + roi) * usd_mxn
    return int(round(raw / 1_000) * 1_000)
