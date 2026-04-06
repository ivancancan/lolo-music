"""
instagram_fetch.py
──────────────────
Scrapes @guitarshome Instagram posts using a real browser (Playwright).
Extracts caption, date, and URL from each post without needing login.

Usage:
    python instagram_fetch.py               # scrape all posts
    python instagram_fetch.py --limit 100   # stop after 100 posts
    python instagram_fetch.py --no-resume   # re-scrape everything

Output: ig_posts/guitarshome/<shortcode>.json
Then run: python instagram_parse.py → generates CSV
"""

import os
import re
import json
import time
import argparse
from dotenv import load_dotenv

load_dotenv()

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    raise SystemExit("Run: pip install playwright && python -m playwright install chromium")

PROFILE_URL  = "https://www.instagram.com/guitarshome/"
OUTPUT_DIR   = os.path.join("ig_posts", "guitarshome")
LINKS_CACHE  = os.path.join("ig_posts", "guitarshome_links.json")
SCROLL_PAUSE = 2.5   # seconds between scrolls


def load_done() -> set:
    done = set()
    if os.path.isdir(OUTPUT_DIR):
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(".json"):
                done.add(f[:-5])
    return done


def save_post(shortcode: str, data: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(OUTPUT_DIR, f"{shortcode}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def scrape(limit: int = 0, resume: bool = True) -> None:
    done = load_done() if resume else set()
    print(f"Posts ya descargados: {len(done)}")

    def make_browser(p):
        return p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-extensions",
                "--single-process",
                "--no-zygote",
            ],
        )

    def make_context(browser, session_id, csrf_token, ds_user_id):
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        if session_id:
            cookies = []
            for name, value in [
                ("sessionid",  session_id),
                ("csrftoken",  csrf_token),
                ("ds_user_id", ds_user_id),
            ]:
                if value:
                    cookies.append({
                        "name": name, "value": value,
                        "domain": ".instagram.com", "path": "/",
                        "httpOnly": name == "sessionid", "secure": True,
                    })
            ctx.add_cookies(cookies)
        return ctx

    with sync_playwright() as p:
        browser = make_browser(p)

        # Inject Instagram session cookies so captions are visible
        session_id = os.getenv("INSTAGRAM_SESSION_ID", "").strip()
        csrf_token = os.getenv("INSTAGRAM_CSRF_TOKEN",  "").strip()
        ds_user_id = os.getenv("INSTAGRAM_DS_USER_ID",  "").strip()

        ctx = make_context(browser, session_id, csrf_token, ds_user_id)
        if session_id:
            print("Cookies de sesion inyectadas")
        else:
            print("Sin sesion — los captions pueden estar ocultos")

        page = ctx.new_page()
        page_crashed = False

        def on_crash():
            nonlocal page_crashed
            page_crashed = True

        page.on("crash", lambda: on_crash())

        # ── Open profile ──────────────────────────────────────────────────────
        print(f"Abriendo {PROFILE_URL} ...")
        page.goto(PROFILE_URL, wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)

        # Dismiss cookie/login dialogs if present
        for selector in [
            "button:has-text('Allow all cookies')",
            "button:has-text('Aceptar todas')",
            "button:has-text('Accept All')",
            "[aria-label='Close']",
            "button:has-text('Not Now')",
            "button:has-text('Ahora no')",
        ]:
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=1500):
                    btn.click()
                    time.sleep(1)
            except Exception:
                pass

        # ── Collect post links (use cache if available) ───────────────────────
        post_urls: list[str] = []

        if os.path.exists(LINKS_CACHE) and not limit:
            with open(LINKS_CACHE, encoding="utf-8") as f:
                post_urls = json.load(f)
            print(f"Links cargados desde cache: {len(post_urls)}")
        else:
            seen_urls: set[str] = set()
            no_new_count = 0

            print("Recolectando links de posts (scrolling)...")
            while True:
                anchors = page.locator("a[href*='/p/']").all()
                new_found = 0
                for a in anchors:
                    try:
                        href = a.get_attribute("href")
                        if href and "/p/" in href:
                            m = re.search(r"/p/([A-Za-z0-9_-]+)/", href)
                            if m:
                                sc = m.group(1)
                                if sc not in seen_urls:
                                    seen_urls.add(sc)
                                    post_urls.append(f"https://www.instagram.com/p/{sc}/")
                                    new_found += 1
                    except Exception:
                        pass

                print(f"  Links encontrados: {len(post_urls)}", end="\r")

                if limit and len(post_urls) >= limit:
                    post_urls = post_urls[:limit]
                    break

                if new_found == 0:
                    no_new_count += 1
                    if no_new_count >= 4:
                        break
                else:
                    no_new_count = 0

                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(SCROLL_PAUSE)

            print(f"\nTotal links: {len(post_urls)}")

            # Save links cache so restarts don't need to re-scroll
            if not limit:
                os.makedirs(os.path.dirname(LINKS_CACHE), exist_ok=True)
                with open(LINKS_CACHE, "w", encoding="utf-8") as f:
                    json.dump(post_urls, f)
                print(f"Links guardados en cache: {LINKS_CACHE}")

        # ── Visit each post ───────────────────────────────────────────────────
        import random

        saved = 0
        skipped = 0
        empty_streak = 0          # consecutive posts with no caption/date
        EMPTY_STREAK_PAUSE = 3    # pause after this many blanks in a row
        RATE_LIMIT_SLEEP   = 600  # 10 min — Instagram necesita mas tiempo para recuperarse
        BROWSER_RESTART_EVERY = 200  # reiniciar browser cada N posts para liberar memoria

        def restart_browser():
            nonlocal browser, ctx, page, page_crashed
            print("  [RESTART] Reiniciando browser para liberar memoria...")
            try:
                browser.close()
            except Exception:
                pass
            browser = make_browser(p)
            ctx = make_context(browser, session_id, csrf_token, ds_user_id)
            page = ctx.new_page()
            page_crashed = False
            page.on("crash", lambda: on_crash())

        posts_since_restart = 0

        for i, url in enumerate(post_urls, 1):
            sc_match = re.search(r"/p/([A-Za-z0-9_-]+)/", url)
            if not sc_match:
                continue
            shortcode = sc_match.group(1)

            if resume and shortcode in done:
                skipped += 1
                continue

            # Reiniciar browser periódicamente para evitar memory leak
            if posts_since_restart > 0 and posts_since_restart % BROWSER_RESTART_EVERY == 0:
                restart_browser()

            # Recuperar de crash si ocurrió en la iteración anterior
            if page_crashed:
                print(f"  [CRASH RECOVERY] Recreando pagina en post {i}...")
                try:
                    page.close()
                except Exception:
                    pass
                try:
                    page = ctx.new_page()
                    page_crashed = False
                    page.on("crash", lambda: on_crash())
                except Exception:
                    restart_browser()
                time.sleep(5)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)

                # Wait for time element — if it never appears, Instagram is blocking us
                date_str = ""
                try:
                    page.wait_for_selector("time", timeout=6000)
                    time_el = page.locator("time").first
                    date_str = time_el.get_attribute("datetime") or ""
                except Exception:
                    pass

                # Caption
                caption = ""
                for sel in [
                    "span.x193iq5w",
                    "article h1",
                    "ul li span span",
                    "article div span span",
                ]:
                    try:
                        els = page.locator(sel).all()
                        for el in els:
                            txt = el.inner_text(timeout=800).strip()
                            if len(txt) > 15:
                                caption = txt
                                break
                        if caption:
                            break
                    except Exception:
                        pass

                post_data = {
                    "shortcode": shortcode,
                    "date":      date_str[:10] if date_str else "",
                    "caption":   caption,
                    "url":       url,
                }
                save_post(shortcode, post_data)
                saved += 1
                posts_since_restart += 1

                preview = caption[:60].replace("\n", " ")
                print(f"  [{i}/{len(post_urls)}] {date_str[:10]}  guitarshome   {preview}...")

                # Track empty results to detect rate limiting
                if not caption and not date_str:
                    empty_streak += 1
                    if empty_streak >= EMPTY_STREAK_PAUSE:
                        print(f"\n  [RATE LIMIT] {empty_streak} posts vacios seguidos — esperando {RATE_LIMIT_SLEEP}s...")
                        time.sleep(RATE_LIMIT_SLEEP)
                        empty_streak = 0
                else:
                    empty_streak = 0

            except PWTimeout:
                print(f"  [{i}] TIMEOUT: {url}")
                empty_streak += 1
            except Exception as e:
                err_str = str(e)
                print(f"  [{i}] ERROR: {err_str[:120]}")
                # Si el error es de página crasheada, forzar recuperación
                if "crash" in err_str.lower() or "target closed" in err_str.lower():
                    page_crashed = True

            # Random delay 6-12s — mas lento pero menos bloqueos
            time.sleep(6 + random.randint(0, 6))

        try:
            browser.close()
        except Exception:
            pass

    print(f"\nListo. Guardados: {saved}  Saltados (ya existian): {skipped}")
    print(f"Carpeta: {OUTPUT_DIR}/")
    print("Siguiente paso: python3 instagram_parse.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",     type=int, default=0,   help="Max posts (0 = todos)")
    parser.add_argument("--no-resume", dest="resume", action="store_false",
                        help="Re-descargar posts ya guardados")
    args = parser.parse_args()
    scrape(limit=args.limit, resume=args.resume)
