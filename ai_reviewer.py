"""
ai_reviewer.py
──────────────
AI-powered expert guitar review layer.

After the pipeline scores deals, this module sends the top opportunities to
Claude for expert validation. Claude reads the full listing descriptions,
specs, and match context to provide:

  1. Match confidence — is this REALLY the same guitar?
  2. Condition assessment — does the description match the price?
  3. Risk flags — anything the algorithm can't catch
  4. Final verdict — COMPRAR / NEGOCIAR $X / PASAR

Requires ANTHROPIC_API_KEY in .env. If missing, silently skips.
"""

import os
import json
from typing import Optional


def review_opportunities(
    opportunities: list,
    us_descriptions: dict,
    usd_mxn: float,
    max_reviews: int = 8,
) -> list:
    """
    Send top opportunities to Claude for expert review.

    Args:
        opportunities: list of match dicts from the pipeline
        us_descriptions: dict mapping US URL → full description text
        usd_mxn: current exchange rate
        max_reviews: max deals to review (API cost control)

    Returns:
        list of dicts: [{"url": ..., "review": ..., "verdict": ..., "offer_price": ...}]
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return []

    try:
        import anthropic
    except ImportError:
        print("[AI Review] anthropic package not installed. Run: pip install anthropic")
        return []

    client = anthropic.Anthropic(api_key=api_key)
    reviews = []

    # Only review top deals (sorted by deal score descending)
    to_review = opportunities[:max_reviews]

    for opp in to_review:
        us_url = opp.get("us_url", "")
        us_desc = us_descriptions.get(us_url, "No description available")
        # Truncate very long descriptions
        if len(us_desc) > 3000:
            us_desc = us_desc[:3000] + "..."

        prompt = _build_review_prompt(opp, us_desc, usd_mxn)

        try:
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            review_text = response.content[0].text
            parsed = _parse_review(review_text)
            parsed["url"] = us_url
            parsed["raw"] = review_text
            reviews.append(parsed)
        except Exception as e:
            print(f"[AI Review] Error reviewing {us_url[:60]}: {e}")
            continue

    return reviews


def _build_review_prompt(opp: dict, us_description: str, usd_mxn: float) -> str:
    """Build the expert review prompt for a single opportunity."""
    margin_pct = opp.get("margin", 0) * 100
    benchmark = opp.get("benchmark_usd", 0)
    bench_src = opp.get("benchmark_source", "unknown")
    reverb_avg = opp.get("reverb_sold_avg_usd", 0)
    match_score = opp.get("score", 0)
    condition = opp.get("us_condition", "unknown")
    capped = opp.get("benchmark_capped", False)

    return f"""Eres un experto en compra-venta de guitarras usadas con 20 años de experiencia.
Tu trabajo: evaluar si esta oportunidad de arbitraje (comprar en EE.UU., vender en México) es REAL y segura.

== GUITARRA EN VENTA (EE.UU.) ==
Título: {opp.get('us_title', '')}
Precio: ${opp.get('us_price_usd', 0):,.0f} USD
Condición reportada: {condition}
Fuente: {opp.get('us_source', '')}
Descripción completa del vendedor:
{us_description}

== BENCHMARK (precio de venta en México) ==
Guitarra GH: {opp.get('gh_title', '')}
Benchmark: ${benchmark:,.0f} USD (fuente: {bench_src})
{"⚠ BENCHMARK AJUSTADO por divergencia con Reverb" if capped else ""}
Reverb sold avg: ${reverb_avg:,.0f} USD
Tipo de cambio: {usd_mxn:.2f} MXN/USD
Margen calculado: {margin_pct:.1f}%
Match score: {match_score}/100

== TU ANÁLISIS (responde en español, máximo 200 palabras) ==
1. MATCH: ¿Es realmente la MISMA guitarra? ¿Mismo modelo, specs, generación? (SÍ/NO/PARCIAL)
2. CONDICIÓN: ¿La descripción respalda el estado reportado? ¿Hay banderas rojas ocultas?
3. PRECIO: ¿El precio de compra es justo para esta condición? ¿Hay espacio para negociar?
4. RIESGO: ¿Algo que el algoritmo no puede detectar? (mods no declaradas, piezas reemplazadas, etc.)
5. VEREDICTO FINAL: COMPRAR / NEGOCIAR $X USD / PASAR
   Si NEGOCIAR, indica el precio máximo que pagarías.
"""


def _parse_review(text: str) -> dict:
    """Extract structured data from the AI review text."""
    text_lower = text.lower()

    # Extract verdict
    if "comprar" in text_lower and "negociar" not in text_lower and "pasar" not in text_lower:
        verdict = "COMPRAR"
    elif "negociar" in text_lower:
        verdict = "NEGOCIAR"
    elif "pasar" in text_lower:
        verdict = "PASAR"
    else:
        verdict = "REVISAR"

    # Try to extract offer price from "NEGOCIAR $X" pattern
    offer_price = None
    import re
    price_match = re.search(r'negociar.*?\$\s*([\d,]+)', text_lower)
    if price_match:
        try:
            offer_price = int(price_match.group(1).replace(",", ""))
        except ValueError:
            pass

    # Extract match assessment
    if "match: sí" in text_lower or "match: si" in text_lower:
        match_confirmed = True
    elif "match: no" in text_lower:
        match_confirmed = False
    else:
        match_confirmed = None

    return {
        "review": text,
        "verdict": verdict,
        "offer_price": offer_price,
        "match_confirmed": match_confirmed,
    }


def format_ai_reviews(reviews: list) -> str:
    """Format AI reviews for Telegram message."""
    if not reviews:
        return ""

    lines = ["\n\n---- ANALISIS EXPERTO IA ----"]
    for r in reviews:
        url = r.get("url", "")
        verdict = r.get("verdict", "?")
        offer = r.get("offer_price")
        review = r.get("review", "")

        # Truncate review for Telegram
        if len(review) > 600:
            review = review[:600] + "..."

        lines.append(f"\n{verdict}")
        if offer:
            lines.append(f"Precio sugerido: ${offer:,} USD")
        lines.append(review)
        lines.append(f"URL: {url}")

    return "\n".join(lines)
