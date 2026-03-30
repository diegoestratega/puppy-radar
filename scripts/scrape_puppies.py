#!/usr/bin/env python3
import json, re, time, random
from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

FP_BASE = "https://954puppies.com"
FP_BREED_SLUGS = [
    "bichonpoo","cavapoo","cockapoo","cotonpoo","havanese","havapoo",
    "havamalt","lhasa-poo","maltese","maltipoo","mini-goldendoodle",
    "mini-schnauzer","mini-schnoodle","morkie","pompoo","shih-tzu",
    "shihpoo","shorkie","teddy-bear","toy-poodle","yorkiechon",
    "yorkiepoo","yorkshire-terrier",
]
FP_BREED_SLUGS_SET = set(FP_BREED_SLUGS)


def is_valid(url: str) -> bool:
    path = urlparse(url).path.lower().rstrip("/")
    segs = [s for s in path.split("/") if s]
    if len(segs) != 3 or segs[0] != "puppies":
        return False
    if segs[1] not in FP_BREED_SLUGS_SET:
        return False
    return bool(re.match(r'^[a-z0-9-]{4,}$', segs[2]))


all_urls = set()

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )

    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="America/New_York",
    )
    context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    # Visit home page first to establish a real-looking session
    home = context.new_page()
    try:
        home.goto(FP_BASE, wait_until="domcontentloaded", timeout=20_000)
        home.wait_for_timeout(2000)
        print(f"Home page loaded: {home.title()}")
    except Exception as e:
        print(f"Home page warn: {e}")
    finally:
        home.close()

    for slug in FP_BREED_SLUGS:
        page = context.new_page()
        try:
            # KEY FIX: domcontentloaded instead of networkidle
            # networkidle never fires on sites with WebSockets and silently
            # times out after 30s returning 0 URLs every single time.
            page.goto(
                f"{FP_BASE}/puppies-for-sale/{slug}",
                wait_until="domcontentloaded",
                timeout=30_000,
            )

            # Give React time to hydrate and render puppy cards
            page.wait_for_timeout(3500)

            # Try to wait for an actual puppy link to appear
            try:
                page.wait_for_function(
                    f"() => document.querySelectorAll('a[href*=\"/puppies/{slug}/\"]').length > 0",
                    timeout=10_000,
                )
            except Exception:
                pass  # No puppies in stock for this breed — that's fine

            # Collect all hrefs from the fully-rendered DOM
            hrefs = page.eval_on_selector_all(
                "a[href]",
                "els => [...new Set(els.map(e => e.href))]",
            )

            found = [u for u in hrefs if is_valid(u)]
            all_urls.update(found)

            title = page.title()
            print(f"  {slug}: {len(found)} puppies  ({len(hrefs)} total links)  [{title[:60]}]")

        except Exception as e:
            print(f"  {slug}: ERROR — {e}")
        finally:
            page.close()

        time.sleep(random.uniform(1.0, 2.0))

    context.close()
    browser.close()

out = Path("static/fp_cache.json")
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps({
    "urls":       list(all_urls),
    "scraped_at": time.time(),
    "count":      len(all_urls),
}, indent=2))

print(f"\n✅  Done — {len(all_urls)} total puppy URLs → static/fp_cache.json")