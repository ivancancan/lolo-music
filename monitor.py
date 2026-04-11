"""
monitor.py
──────────
Two independent utilities:

1. FX Rate Monitor
   Fetches the current USD/MXN rate and alerts via Telegram if it moves
   more than ALERT_THRESHOLD_PCT in 24 hours.
   Updates .env USD_MXN so main.py always uses a fresh rate.

2. Inventory Tracker
   Tracks guitars you've purchased but not yet sold.
   Stored in inventory.json — edit manually or via CLI.
   Alerts via Telegram if a guitar has been held > HOLD_ALERT_DAYS.

Usage:
    python monitor.py fx               # check FX rate only
    python monitor.py inventory        # check inventory only
    python monitor.py                  # run both

Inventory management:
    python monitor.py add              # add a guitar interactively
    python monitor.py sell <id>        # mark guitar as sold
    python monitor.py list             # print current inventory
"""

import os
import csv
import glob
import json
import sys
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv, set_key

import requests
from rapidfuzz import fuzz as _fuzz

load_dotenv(dotenv_path=".env")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

ENV_FILE           = ".env"
FX_CACHE_FILE      = "fx_cache.json"
INVENTORY_FILE     = "inventory.json"

ALERT_THRESHOLD_PCT = 2.0    # alert if rate moves >2% vs yesterday
HOLD_ALERT_DAYS     = 45     # alert if guitar held >45 days without selling
FX_API_URL          = "https://open.er-api.com/v6/latest/USD"  # free, no key

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "").strip()


# ─────────────────────────────────────────────────────────────────────────────
# Telegram helper
# ─────────────────────────────────────────────────────────────────────────────

def _telegram(text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# 1. FX Rate Monitor
# ─────────────────────────────────────────────────────────────────────────────

def _load_fx_cache() -> dict:
    if Path(FX_CACHE_FILE).exists():
        with open(FX_CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_fx_cache(data: dict) -> None:
    with open(FX_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def fetch_usd_mxn() -> float | None:
    try:
        r = requests.get(FX_API_URL, timeout=10)
        r.raise_for_status()
        data = r.json()
        return float(data["rates"]["MXN"])
    except Exception as e:
        print(f"  [FX] Error fetching rate: {e}")
        return None


def check_fx_rate() -> None:
    print("\n=== FX Rate Monitor ===")

    current_rate = fetch_usd_mxn()
    if not current_rate:
        print("  [FX] No se pudo obtener la tasa actual.")
        return

    cache    = _load_fx_cache()
    today    = str(date.today())
    prev_rate = cache.get("rate")
    prev_date = cache.get("date", "")

    print(f"  USD/MXN hoy:  {current_rate:.4f}")
    if prev_rate:
        print(f"  USD/MXN ayer ({prev_date}): {prev_rate:.4f}")
        change_pct = (current_rate - prev_rate) / prev_rate * 100
        print(f"  Cambio: {change_pct:+.2f}%")

        if abs(change_pct) >= ALERT_THRESHOLD_PCT:
            direction = "SUBE" if change_pct > 0 else "BAJA"
            if change_pct < 0:
                # Peso appreciates = USD buys less MXN = your landed cost in MXN terms is cheaper
                # = all your margins improve simultaneously = buy window
                example_guitar_mxn = 55000  # LP Standard benchmark MXN
                old_usd_cost = example_guitar_mxn / prev_rate
                new_usd_cost = example_guitar_mxn / current_rate
                margin_boost = (old_usd_cost - new_usd_cost) / old_usd_cost * 100
                context = (
                    f"El peso se APRECIA — VENTANA DE COMPRA ABIERTA.\n"
                    f"Tu landed cost baja en MXN equivalente.\n"
                    f"Ejemplo LP Standard ($55,000 MXN benchmark):\n"
                    f"  Antes: USD {old_usd_cost:,.0f} landed\n"
                    f"  Ahora: USD {new_usd_cost:,.0f} landed\n"
                    f"  Mejora de margen: +{margin_boost:.1f}%\n\n"
                    f"Corre main.py ahora — los margenes mejoraron para todas las guitarras."
                )
            else:
                context = (
                    f"El peso se DEPRECIA — tus costos suben en MXN.\n"
                    f"Revisa margenes antes de comprar.\n"
                    f"Espera a que el tipo de cambio se estabilice."
                )
            msg = (
                f"ALERTA FX - USD/MXN\n\n"
                f"Tasa {direction} {abs(change_pct):.2f}%\n"
                f"Ayer: ${prev_rate:.4f}\n"
                f"Hoy:  ${current_rate:.4f}\n\n"
                + context
            )
            _telegram(msg)
            print(f"  ALERTA enviada: {direction} {abs(change_pct):.2f}%")

    # Update .env with fresh rate
    if prev_date != today:
        try:
            set_key(ENV_FILE, "USD_MXN", f"{current_rate:.4f}")
            print(f"  .env actualizado: USD_MXN={current_rate:.4f}")
        except Exception as e:
            print(f"  [FX] No se pudo actualizar .env: {e}")

        _save_fx_cache({"rate": current_rate, "date": today})


# ─────────────────────────────────────────────────────────────────────────────
# 2. Inventory Tracker
# ─────────────────────────────────────────────────────────────────────────────

def _load_inventory() -> list[dict]:
    if Path(INVENTORY_FILE).exists():
        with open(INVENTORY_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []


def _save_inventory(inventory: list[dict]) -> None:
    with open(INVENTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(inventory, f, ensure_ascii=False, indent=2)


def _next_id(inventory: list[dict]) -> int:
    if not inventory:
        return 1
    return max(item["id"] for item in inventory) + 1


def add_guitar() -> None:
    print("\n=== Agregar guitarra al inventario ===")
    inventory = _load_inventory()

    title       = input("Nombre de la guitarra: ").strip()
    source      = input("Tienda donde la compraste (daves/cme/reverb/ebay/etc): ").strip()
    buy_price   = float(input("Precio de compra (USD): ").strip())
    logistics   = float(input("Costo logistica (USD) [default 150]: ").strip() or "150")
    target_sell = input("Precio objetivo de venta (MXN): ").strip()
    us_url      = input("URL del listing original (opcional): ").strip()
    notes       = input("Notas (opcional): ").strip()

    item = {
        "id":           _next_id(inventory),
        "title":        title,
        "source":       source,
        "buy_price_usd": buy_price,
        "logistics_usd": logistics,
        "landed_cost":  round(buy_price + logistics, 2),
        "target_sell_mxn": float(target_sell) if target_sell else None,
        "us_url":       us_url,
        "notes":        notes,
        "date_bought":  str(date.today()),
        "date_sold":    None,
        "sold":         False,
        "sell_price_mxn": None,
    }

    inventory.append(item)
    _save_inventory(inventory)
    print(f"\nAgregado: [{item['id']}] {item['title']}")
    print(f"  Costo aterrizaje: ${item['landed_cost']:,.2f} USD")


def mark_sold(guitar_id: int) -> None:
    inventory = _load_inventory()
    for item in inventory:
        if item["id"] == guitar_id:
            sell_price = float(input(f"Precio de venta de '{item['title']}' (MXN): ").strip())
            item["sold"]          = True
            item["date_sold"]     = str(date.today())
            item["sell_price_mxn"] = sell_price

            usd_mxn = float(os.getenv("USD_MXN", "19.5"))
            sell_usd = sell_price / usd_mxn
            profit   = sell_usd - item["landed_cost"]
            margin   = profit / item["landed_cost"] * 100

            days_held = (
                datetime.strptime(item["date_sold"], "%Y-%m-%d") -
                datetime.strptime(item["date_bought"], "%Y-%m-%d")
            ).days

            print(f"\nVENDIDA: {item['title']}")
            print(f"  Comprada: ${item['landed_cost']:,.0f} USD | Vendida: ${sell_usd:,.0f} USD")
            print(f"  Utilidad: ${profit:,.0f} USD ({margin:.1f}%) | Dias en inventario: {days_held}")

            _save_inventory(inventory)
            return

    print(f"No se encontro guitarra con ID {guitar_id}")


def list_inventory() -> None:
    inventory = _load_inventory()
    active    = [i for i in inventory if not i["sold"]]
    sold      = [i for i in inventory if i["sold"]]

    usd_mxn   = float(os.getenv("USD_MXN", "19.5"))
    today     = date.today()

    print(f"\n=== Inventario Activo ({len(active)} guitarras) ===")
    total_invested = 0.0

    for item in active:
        days_held = (today - datetime.strptime(item["date_bought"], "%Y-%m-%d").date()).days
        target    = item.get("target_sell_mxn")
        target_str = f"  Target: ${target:,.0f} MXN (${target/usd_mxn:,.0f} USD)" if target else ""
        warn = "  *** MAS DE 45 DIAS ***" if days_held > HOLD_ALERT_DAYS else ""

        print(
            f"  [{item['id']}] {item['title']}\n"
            f"    Comprada: {item['date_bought']} | Dias: {days_held}{warn}\n"
            f"    Costo: ${item['landed_cost']:,.0f} USD | Fuente: {item['source']}\n"
            f"{target_str}"
        )
        total_invested += item["landed_cost"]

    print(f"\n  Capital invertido: ${total_invested:,.0f} USD")

    if sold:
        print(f"\n=== Historial Ventas ({len(sold)} guitarras) ===")
        total_profit = 0.0
        for item in sold:
            sell_usd = (item.get("sell_price_mxn") or 0) / usd_mxn
            profit   = sell_usd - item["landed_cost"]
            margin   = profit / item["landed_cost"] * 100 if item["landed_cost"] else 0
            days_held = (
                datetime.strptime(item["date_sold"], "%Y-%m-%d") -
                datetime.strptime(item["date_bought"], "%Y-%m-%d")
            ).days
            print(
                f"  [{item['id']}] {item['title']}\n"
                f"    Costo: ${item['landed_cost']:,.0f} | Venta: ${sell_usd:,.0f} | "
                f"Utilidad: ${profit:,.0f} ({margin:.1f}%) | {days_held} dias\n"
            )
            total_profit += profit
        print(f"  Utilidad total realizada: ${total_profit:,.0f} USD")


def check_inventory_alerts() -> None:
    inventory = _load_inventory()
    active    = [i for i in inventory if not i["sold"]]
    today     = date.today()
    alerts    = []

    for item in active:
        try:
            bought = datetime.strptime(item["date_bought"], "%Y-%m-%d").date()
        except Exception:
            continue
        days_held = (today - bought).days
        if days_held > HOLD_ALERT_DAYS:
            alerts.append(f"  [{item['id']}] {item['title']} — {days_held} dias sin vender")

    if alerts:
        msg = f"INVENTARIO — Guitarras sin vender >45 dias\n\n" + "\n".join(alerts)
        _telegram(msg)
        print("\n".join(alerts))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Manual Guitar Evaluator
# ─────────────────────────────────────────────────────────────────────────────

VALIDATION_TARGETS = [
    "Fender American Professional II Stratocaster",
    "Gibson Les Paul Standard 50s",
    "PRS CE 24",
    "Gibson SG Standard",
    "Fender American Ultra Telecaster",
]

_ALL_ORIG_KW = {"all original", "all orig", "100% original", "all-original"}


def _load_gh_active() -> list[dict]:
    """Load latest GH active listings from cached JSON."""
    files = sorted(glob.glob("gh_items_*.json"))
    if not files:
        return []
    with open(files[-1], encoding="utf-8") as f:
        return json.load(f)


def _load_gh_history() -> list[dict]:
    """Load GH Instagram history CSV."""
    rows = []
    if not Path("gh_instagram_history.csv").exists():
        return rows
    with open("gh_instagram_history.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def _best_gh_match(us_title: str, usd_mxn: float):
    """
    Find best GH benchmark for a given US title.
    Returns (gh_title, benchmark_usd, sell_mxn, source_label) or None.
    """
    best_score = 0
    best = None

    # 1. Active GH listings (highest priority)
    for item in _load_gh_active():
        gh_title = item.get("title", "")
        score = _fuzz.token_set_ratio(us_title.lower(), gh_title.lower())
        if score > best_score and score >= 72:
            price_mxn = item.get("price_mxn") or 0
            if price_mxn:
                best_score = score
                best = (gh_title, float(price_mxn) / usd_mxn, float(price_mxn), score, "GH activo")

    # 2. Instagram history fallback
    for row in _load_gh_history():
        gh_title = row.get("title", "")
        score = _fuzz.token_set_ratio(us_title.lower(), gh_title.lower())
        mxn_str = row.get("price_mxn", "").strip()
        if score > best_score and score >= 72 and mxn_str:
            try:
                mxn = float(mxn_str)
                best_score = score
                best = (gh_title, mxn / usd_mxn, mxn, score, "IG historico")
            except ValueError:
                pass

    return best


def eval_guitar() -> None:
    """Evaluate a guitar found during manual browsing."""
    print("\n=== Evaluar Guitarra (busqueda manual) ===\n")

    us_title  = input("Titulo del listing US: ").strip()
    price_str = input("Precio USD: ").strip()
    condition = input("Condicion [Excellent/VG+/VG] (default Excellent): ").strip() or "Excellent"
    source    = input("Fuente (reverb/daves/cme/etc): ").strip() or "manual"
    url       = input("URL (opcional): ").strip()

    try:
        price_usd = float(price_str.replace(",", "").replace("$", ""))
    except ValueError:
        print("Precio invalido.")
        return

    logistics = float(os.getenv("LOGISTICS_USD", "150"))
    usd_mxn   = float(os.getenv("USD_MXN", "17.88"))
    min_margin = float(os.getenv("MIN_MARGIN", "0.30"))

    landed = price_usd + logistics

    print(f"\n--- Buscando benchmark en GH para: '{us_title}' ---\n")

    match = _best_gh_match(us_title, usd_mxn)

    print("=" * 60)
    print(f"EVALUACION: {us_title}")
    print("=" * 60)
    print(f"  Fuente:    {source} [{condition}]")
    print(f"  Compra:    ${price_usd:,.0f} USD")
    print(f"  Landed:    ${landed:,.0f} USD  (+ ${logistics:.0f} logistica)")

    if not match:
        print("\n  [!] Sin benchmark GH — modelo no encontrado en historial ni listing activo.")
        needed_mxn = landed * 1.30 * usd_mxn
        print(f"      Para 30% necesitas vender en: ${landed*1.30:,.0f} USD = ${needed_mxn:,.0f} MXN")
        print(f"\n  URL: {url}" if url else "")
        return

    gh_title, benchmark_usd, sell_mxn, match_score, bench_source = match
    margin = (benchmark_usd - landed) / landed

    # Warnings
    gh_all_orig = any(kw in gh_title.lower() for kw in _ALL_ORIG_KW)
    us_all_orig = any(kw in us_title.lower() for kw in _ALL_ORIG_KW)
    warn_all_orig = gh_all_orig and not us_all_orig

    warn_reverb_high = benchmark_usd and margin < 0 and False  # placeholder

    # Validation target check
    is_target = any(
        _fuzz.token_set_ratio(us_title.lower(), t.lower()) >= 78
        for t in VALIDATION_TARGETS
    )

    # Verdict
    if margin >= 0.45:
        margin_label = "Margen EXCELENTE"
    elif margin >= 0.30:
        margin_label = "Margen SOLIDO"
    elif margin >= 0.20:
        margin_label = "Near miss (margen insuficiente)"
    else:
        margin_label = "Margen INSUFICIENTE"

    if margin >= 0.30:
        verdict = "OPORTUNIDAD" if margin >= 0.45 else "REVISAR"
    else:
        verdict = "PASS"

    print(f"\n  Benchmark: {gh_title[:55]}")
    print(f"             ${benchmark_usd:,.0f} USD = ${sell_mxn:,.0f} MXN  [{bench_source} | match {match_score}]")
    if warn_all_orig:
        print(f"  ⚠ BENCHMARK 'ALL ORIGINAL' — verificar originalidad del listing US")
    print(f"\n  Margen bruto: {margin*100:.1f}%  — {margin_label}")
    print(f"  Veredicto:    {verdict}")
    if is_target:
        print(f"  ★ GUITARRA OBJETIVO (Fase 1)")

    if margin < min_margin:
        needed_usd = landed * (1 + min_margin)
        needed_mxn = needed_usd * usd_mxn
        print(f"\n  Para llegar a 30% necesitas vender en: ${needed_usd:,.0f} USD = ${needed_mxn:,.0f} MXN")
        gh_gap = needed_usd - benchmark_usd
        if gh_gap > 0:
            print(f"  GH benchmark esta ${gh_gap:,.0f} USD por debajo de lo necesario.")

    print(f"\n  URL: {url}" if url else "")
    print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    if not args or "fx" in args:
        check_fx_rate()

    if not args or "inventory" in args:
        check_inventory_alerts()

    if "list" in args:
        list_inventory()

    if "add" in args:
        add_guitar()

    if "sell" in args:
        idx = args.index("sell")
        if idx + 1 < len(args):
            mark_sold(int(args[idx + 1]))
        else:
            print("Uso: python monitor.py sell <id>")

    if "eval" in args:
        eval_guitar()
