#!/usr/bin/env python3
import json, re, time
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
    browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
    for slug in FP_BREED_SLUGS:
        page = browser.new_page()
        try:
            page.goto(
                f"{FP_BASE}/puppies-for-sale/{slug}",
                wait_until="networkidle",
                timeout=30000,
            )
            page.wait_for_timeout(2000)
            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            found = [u for u in hrefs if is_valid(u)]
            all_urls.update(found)
            print(f"  {slug}: {len(found)} puppies")
        except Exception as e:
            print(f"  {slug}: ERROR - {e}")
        finally:
            page.close()
    browser.close()

out = Path("static/fp_cache.json")
out.write_text(json.dumps({
    "urls": list(all_urls),
    "scraped_at": time.time(),
    "count": len(all_urls),
}, indent=2))
print(f"\nDone. {len(all_urls)} total URLs saved to static/fp_cache.json")