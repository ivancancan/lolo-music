"""
optimizer.py
────────────
Self-learning optimization engine for the guitar arbitrage pipeline.

After each pipeline run, this module:
  1. Sends the full report + algorithm state to Claude
  2. Claude analyzes as a guitar expert: which matches are wrong? which thresholds need tuning?
  3. Claude returns structured JSON with parameter adjustments
  4. Adjustments are saved to tuning.json — loaded by next pipeline run
  5. History is kept in tuning_history.json for audit trail + rollback

Tunable parameters (safe to adjust automatically):
  - deal_score weights and thresholds
  - finish words to add as premium
  - model keywords to add for better family detection
  - new red flag keywords
  - filter keywords for accessory/non-guitar detection
  - margin/liquidity/match thresholds

NOT tunable (too risky for auto-adjustment):
  - core matching logic (is_hard_match structure)
  - scraper URLs or parsing logic
  - database schema
  - notification logic

Requires ANTHROPIC_API_KEY in .env.
"""

import os
import json
from datetime import datetime
from typing import Optional


TUNING_FILE = "tuning.json"
TUNING_HISTORY_FILE = "tuning_history.json"

# Default tuning state — these are the baseline values that the pipeline
# starts with. The optimizer can adjust any of these.
DEFAULT_TUNING = {
    "version": 1,
    "last_updated": None,
    "iterations": 0,

    # ── Deal score thresholds ─────────────────────────────────────────────
    "buy_threshold": 78,
    "review_threshold": 50,
    "min_match_buy_now": 88,
    "min_sell_rate": 0.30,
    "min_liq_sold_count": 2,

    # ── Margin thresholds ─────────────────────────────────────────────────
    "margin_min": 0.20,
    "margin_max": 0.40,

    # ── Reverb divergence cap ─────────────────────────────────────────────
    "reverb_divergence_ratio": 0.70,   # cap if reverb < this * benchmark
    "reverb_cap_premium": 1.15,         # allow 15% MX premium over Reverb

    # ── Benchmark confidence penalties ────────────────────────────────────
    "instagram_bench_penalty": -3.0,
    "capped_bench_penalty": -8.0,

    # ── Score weights ─────────────────────────────────────────────────────
    "weight_margin": 40,
    "weight_liquidity": 20,
    "weight_match": 15,
    "weight_sale": 10,
    "weight_dom": 10,
    "weight_source": 5,

    # ── New filter keywords learned from bad matches ──────────────────────
    "learned_premium_finishes": [],      # finishes that should block cross-matching
    "learned_model_keywords": {},        # model → [keywords] to add to MODEL_KEYWORDS
    "learned_red_flags": [],             # new red flag phrases to filter
    "learned_accessory_keywords": [],    # new non-guitar items to filter

    # ── Optimization notes ────────────────────────────────────────────────
    "notes": [],   # human-readable log of what changed and why
}


def load_tuning() -> dict:
    """Load current tuning parameters, or return defaults."""
    if os.path.exists(TUNING_FILE):
        try:
            with open(TUNING_FILE, "r") as f:
                tuning = json.load(f)
            # Merge with defaults to pick up new keys added in code updates
            merged = {**DEFAULT_TUNING, **tuning}
            return merged
        except (json.JSONDecodeError, IOError):
            pass
    return DEFAULT_TUNING.copy()


def save_tuning(tuning: dict) -> None:
    """Save tuning parameters to disk."""
    with open(TUNING_FILE, "w") as f:
        json.dump(tuning, f, indent=2, ensure_ascii=False)


def _append_history(tuning: dict, changes: dict, reasoning: str) -> None:
    """Append an entry to the tuning history for audit trail."""
    history = []
    if os.path.exists(TUNING_HISTORY_FILE):
        try:
            with open(TUNING_HISTORY_FILE, "r") as f:
                history = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    history.append({
        "timestamp": datetime.now().isoformat(),
        "iteration": tuning.get("iterations", 0),
        "changes": changes,
        "reasoning": reasoning,
    })

    # Keep last 50 entries
    history = history[-50:]

    with open(TUNING_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def optimize_from_report(
    report_text: str,
    opportunities: list,
    near_misses: list,
    current_tuning: dict,
) -> Optional[dict]:
    """
    Send the pipeline output to Claude for analysis and parameter optimization.

    Returns updated tuning dict, or None if optimization failed/skipped.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None

    try:
        import anthropic
    except ImportError:
        print("[Optimizer] anthropic package not installed")
        return None

    client = anthropic.Anthropic(api_key=api_key)

    # Build the optimization prompt
    prompt = _build_optimization_prompt(report_text, opportunities, near_misses, current_tuning)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        response_text = response.content[0].text

        # Parse the JSON response
        updated_tuning = _parse_optimization_response(response_text, current_tuning)
        if updated_tuning:
            updated_tuning["iterations"] = current_tuning.get("iterations", 0) + 1
            updated_tuning["last_updated"] = datetime.now().isoformat()

            # Extract reasoning for history
            reasoning = _extract_reasoning(response_text)
            changes = _diff_tuning(current_tuning, updated_tuning)

            if changes:
                save_tuning(updated_tuning)
                _append_history(updated_tuning, changes, reasoning)
                print(f"[Optimizer] Iteration {updated_tuning['iterations']}: {len(changes)} parameters adjusted")
                for key, (old, new) in changes.items():
                    print(f"  {key}: {old} → {new}")
                return updated_tuning
            else:
                print("[Optimizer] No changes recommended — algorithm is performing well")
                return None

    except Exception as e:
        print(f"[Optimizer] Error: {e}")
        return None


def _build_optimization_prompt(
    report_text: str,
    opportunities: list,
    near_misses: list,
    tuning: dict,
) -> str:
    """Build the prompt that asks Claude to optimize the algorithm."""

    # Summarize opportunities for context
    opp_summary = ""
    for i, opp in enumerate(opportunities[:10], 1):
        opp_summary += (
            f"  {i}. {opp.get('gh_title', '')[:60]}\n"
            f"     US: {opp.get('us_title', '')[:60]}\n"
            f"     Compra: ${opp.get('us_price_usd', 0):,.0f} | Benchmark: ${opp.get('benchmark_usd', 0):,.0f} "
            f"| Reverb: ${opp.get('reverb_sold_avg_usd', 0):,.0f}\n"
            f"     Margen: {opp.get('margin', 0)*100:.1f}% | Match: {opp.get('score', 0)} "
            f"| Deal: {opp.get('deal_score', {}).get('total', 0)}\n"
            f"     Fuente: {opp.get('us_source', '')} | Cond: {opp.get('us_condition', '')}\n"
            f"     Bench src: {opp.get('benchmark_source', '')} | Capped: {opp.get('benchmark_capped', False)}\n"
            f"     Liquidez: {opp.get('liquidity', {})}\n\n"
        )

    nm_summary = ""
    for i, nm in enumerate(near_misses[:5], 1):
        nm_summary += (
            f"  {i}. {nm.get('gh_title', '')[:60]} → {nm.get('us_title', '')[:60]}\n"
            f"     Margen: {nm.get('margin', 0)*100:.1f}% | Match: {nm.get('score', 0)}\n\n"
        )

    tuning_json = json.dumps(tuning, indent=2, ensure_ascii=False)

    return f"""Eres un experto en algoritmos de arbitraje de guitarras. Tu trabajo es optimizar los parametros
del sistema de matching y scoring para que SOLO muestre oportunidades LEGITIMAS de compra.

El sistema compra guitarras usadas en EE.UU. y las vende en Mexico con 30%+ de margen.
Guitar's Home (GH) es la referencia de precios en Mexico.

== REPORTE ACTUAL DEL PIPELINE ==
{report_text[:4000]}

== OPORTUNIDADES DETALLADAS ==
{opp_summary}

== NEAR MISSES (margen 20-30%) ==
{nm_summary}

== PARAMETROS ACTUALES (tuning.json) ==
{tuning_json}

== TU ANALISIS Y AJUSTES ==

Analiza cada oportunidad como un experto de guitarras:
1. ¿El match GH vs US es realmente la MISMA guitarra? (misma generacion, specs, tier)
2. ¿El benchmark es confiable? (¿Reverb sold diverge mucho de GH?)
3. ¿La liquidez justifica la compra? (¿GH realmente vende este modelo?)
4. ¿El margen es real o inflado por benchmark optimista?

Basado en tu analisis, responde con un JSON de ajustes. SOLO ajusta parametros que necesiten cambio.
Si el algoritmo esta funcionando bien, devuelve {{}}.

REGLAS:
- Nunca subas buy_threshold arriba de 85 ni lo bajes de 70
- Nunca bajes min_match_buy_now debajo de 82
- Nunca subas margin_max arriba de 0.50
- Los cambios deben ser incrementales (max ±20% por iteracion)
- Incluye "reasoning" explicando POR QUE cada cambio

Responde SOLO con JSON valido en este formato:
```json
{{
  "reasoning": "Explicacion de los cambios...",
  "adjustments": {{
    "parametro_a_cambiar": nuevo_valor,
    "learned_premium_finishes": ["finish1", "finish2"],
    "learned_model_keywords": {{"model_family": ["keyword1"]}},
    "learned_red_flags": ["nueva frase de red flag"],
    "notes": ["nota sobre el cambio"]
  }}
}}
```
"""


def _parse_optimization_response(response_text: str, current_tuning: dict) -> Optional[dict]:
    """Parse Claude's optimization response and apply safe adjustments."""
    import re

    # Extract JSON from response (might be wrapped in markdown code blocks)
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        # Try to find raw JSON
        json_match = re.search(r'\{[^{}]*"adjustments"[^{}]*\{.*?\}[^{}]*\}', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
        else:
            print("[Optimizer] Could not extract JSON from response")
            return None

    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError:
        print("[Optimizer] Invalid JSON in response")
        return None

    adjustments = parsed.get("adjustments", {})
    if not adjustments:
        return None

    # Apply adjustments with safety bounds
    updated = current_tuning.copy()
    SAFETY_BOUNDS = {
        "buy_threshold":        (70, 85),
        "review_threshold":     (40, 60),
        "min_match_buy_now":    (82, 95),
        "min_sell_rate":        (0.15, 0.50),
        "min_liq_sold_count":   (1, 5),
        "margin_min":           (0.15, 0.30),
        "margin_max":           (0.30, 0.50),
        "reverb_divergence_ratio": (0.50, 0.85),
        "reverb_cap_premium":   (1.05, 1.30),
        "instagram_bench_penalty": (-8.0, 0.0),
        "capped_bench_penalty":    (-15.0, -3.0),
        "weight_margin":        (30, 50),
        "weight_liquidity":     (10, 30),
        "weight_match":         (10, 25),
        "weight_sale":          (5, 15),
        "weight_dom":           (5, 15),
        "weight_source":        (2, 10),
    }

    for key, value in adjustments.items():
        if key in ("reasoning", "notes"):
            continue

        if key in SAFETY_BOUNDS and isinstance(value, (int, float)):
            lo, hi = SAFETY_BOUNDS[key]
            value = max(lo, min(hi, value))

            # Max ±20% change per iteration
            old_val = current_tuning.get(key, value)
            if old_val != 0:
                max_delta = abs(old_val) * 0.20
                if abs(value - old_val) > max_delta:
                    value = old_val + (max_delta if value > old_val else -max_delta)

            updated[key] = round(value, 4) if isinstance(value, float) else value

        elif key == "learned_premium_finishes" and isinstance(value, list):
            existing = set(updated.get("learned_premium_finishes", []))
            existing.update(v.lower().strip() for v in value if isinstance(v, str))
            updated["learned_premium_finishes"] = sorted(existing)

        elif key == "learned_model_keywords" and isinstance(value, dict):
            existing = updated.get("learned_model_keywords", {})
            for model, keywords in value.items():
                if isinstance(keywords, list):
                    old = set(existing.get(model, []))
                    old.update(k.lower().strip() for k in keywords if isinstance(k, str))
                    existing[model] = sorted(old)
            updated["learned_model_keywords"] = existing

        elif key == "learned_red_flags" and isinstance(value, list):
            existing = set(updated.get("learned_red_flags", []))
            existing.update(v.lower().strip() for v in value if isinstance(v, str))
            updated["learned_red_flags"] = sorted(existing)

        elif key == "learned_accessory_keywords" and isinstance(value, list):
            existing = set(updated.get("learned_accessory_keywords", []))
            existing.update(v.lower().strip() for v in value if isinstance(v, str))
            updated["learned_accessory_keywords"] = sorted(existing)

        elif key == "notes" and isinstance(value, list):
            existing = updated.get("notes", [])
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            for note in value:
                existing.append(f"[{ts}] {note}")
            # Keep last 20 notes
            updated["notes"] = existing[-20:]

    return updated


def _extract_reasoning(response_text: str) -> str:
    """Extract the reasoning field from the response."""
    import re
    try:
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if json_match:
            parsed = json.loads(json_match.group(1))
            return parsed.get("reasoning", response_text[:500])
    except Exception:
        pass
    return response_text[:500]


def _diff_tuning(old: dict, new: dict) -> dict:
    """Return a dict of changed keys: key → (old_value, new_value)."""
    changes = {}
    skip_keys = {"version", "last_updated", "iterations", "notes"}
    for key in new:
        if key in skip_keys:
            continue
        old_val = old.get(key)
        new_val = new.get(key)
        if old_val != new_val:
            changes[key] = (old_val, new_val)
    return changes


def apply_tuning_to_deal_score():
    """
    Apply tuning.json overrides to deal_score module at import time.
    Call this at the start of main.py before scoring.
    """
    tuning = load_tuning()
    if tuning.get("iterations", 0) == 0:
        return tuning  # no optimizations yet, use defaults

    import deal_score
    import main as main_module

    # Apply score weights
    if "weight_margin" in tuning:
        deal_score.WEIGHT_MARGIN = tuning["weight_margin"]
    if "weight_liquidity" in tuning:
        deal_score.WEIGHT_LIQUIDITY = tuning["weight_liquidity"]
    if "weight_match" in tuning:
        deal_score.WEIGHT_MATCH = tuning["weight_match"]
    if "weight_sale" in tuning:
        deal_score.WEIGHT_SALE = tuning["weight_sale"]
    if "weight_dom" in tuning:
        deal_score.WEIGHT_DOM = tuning["weight_dom"]
    if "weight_source" in tuning:
        deal_score.WEIGHT_SOURCE = tuning["weight_source"]

    # Apply thresholds
    if "buy_threshold" in tuning:
        deal_score.BUY_THRESHOLD = tuning["buy_threshold"]
    if "review_threshold" in tuning:
        deal_score.REVIEW_THRESHOLD = tuning["review_threshold"]
    if "margin_min" in tuning:
        deal_score.MARGIN_MIN = tuning["margin_min"]
    if "margin_max" in tuning:
        deal_score.MARGIN_MAX = tuning["margin_max"]

    # Apply main.py gates
    if "min_match_buy_now" in tuning:
        main_module.MIN_MATCH_BUY_NOW = tuning["min_match_buy_now"]
    if "min_sell_rate" in tuning:
        main_module.MIN_SELL_RATE = tuning["min_sell_rate"]
    if "min_liq_sold_count" in tuning:
        main_module.MIN_LIQ_SOLD_COUNT = tuning["min_liq_sold_count"]

    # Apply learned red flags to matching
    learned_flags = tuning.get("learned_red_flags", [])
    if learned_flags:
        import matching
        for flag in learned_flags:
            if flag not in matching.RED_FLAG_KEYWORDS:
                matching.RED_FLAG_KEYWORDS.append(flag)

    # Apply learned model keywords
    learned_models = tuning.get("learned_model_keywords", {})
    if learned_models:
        import matching
        for family, keywords in learned_models.items():
            for kw in keywords:
                if kw not in matching.MODEL_KEYWORDS:
                    matching.MODEL_KEYWORDS[kw] = family

    print(f"[Tuning] Applied iteration {tuning['iterations']} optimizations")
    return tuning
