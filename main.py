import asyncio
import concurrent.futures
import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from playwright.sync_api import sync_playwright
from pydantic import BaseModel

load_dotenv()

app = FastAPI()

BASE_DIR   = Path(__file__).parent
STATE_FILE = BASE_DIR / "state.json"
STATIC_DIR = BASE_DIR / "static"

ZIP_CODE     = "33179"
RADIUS_MILES = 30

RG_KEY  = os.getenv("RESCUEGROUPS_API_KEY", "")
RG_BASE = "https://api.rescuegroups.org/v5"

BREEDS = [
    "All", "Poodle", "Maltese", "Shih Tzu", "Yorkshire Terrier",
    "Bichon Frise", "Havanese", "Miniature Schnauzer", "Maltipoo",
    "Cavapoo", "Morkie", "Shihpoo", "Cockapoo", "Teddy Bear",
]

FP_BREED_SLUGS = [
    "bichonpoo", "cavapoo", "cockapoo", "cotonpoo",
    "havanese", "havapoo", "havamalt", "lhasa-poo",
    "maltese", "maltipoo", "mini-goldendoodle", "mini-schnauzer",
    "mini-schnoodle", "morkie", "pompoo", "shih-tzu",
    "shihpoo", "shorkie", "teddy-bear", "toy-poodle",
    "yorkiechon", "yorkiepoo", "yorkshire-terrier",
]
FP_BREED_SLUGS_SET = set(FP_BREED_SLUGS)
FP_BASE            = "https://954puppies.com"

_fp_url_cache: dict = {"urls": [], "cached_at": 0.0}
FP_CACHE_TTL        = 1800  # 30 minutes

_thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

RG_ALLOWED_AGE_GROUPS  = {"baby", "young"}
RG_ALLOWED_SIZE_GROUPS = {"small"}

RG_UNAVAILABLE_STATUSES = {
    "adopted", "hold", "on hold", "not available",
    "inactive", "deleted", "transfer", "euthanized",
}

RG_LOW_SHED_KEYWORDS = {
    "poodle", "maltese", "bichon", "yorkshire", "yorkie", "havanese",
    "schnauzer", "maltipoo", "shih tzu", "shih-tzu", "morkie", "cockapoo",
    "cavapoo", "teddy bear", "shihpoo", "shorkie", "yorkiepoo", "pompoo",
    "cavachon", "poochon", "bichonpoo", "cotonpoo", "schnoodle",
    "mini goldendoodle", "lhasa poo", "lhasapoo", "havapoo", "havamalt",
}

SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

MDAS_URLS = [
    "https://www.miamidade.gov/animals/",
    "https://adopt.miamidade.gov/",
]

_FP_SOLD_RE = re.compile(
    r"(?:"
    r"(?:has been|is now|is)\s+(?:adopted|sold|reserved)|"
    r"status[\s:>\"\\-]{0,6}(?:adopted|sold|reserved)|"
    r"(?:this puppy|this dog)[^.]{0,60}(?:adopted|sold|reserved|gone)|"
    r"no longer available|"
    r"gone to (?:a |their |his |her )?(?:new |forever )?home|"
    r"found (?:a |their |his |her )?forever home|"
    r'"availability"\s*:\s*"(?!InStock)'
    r")",
    re.I | re.DOTALL,
)

_FP_HREF_JS = """
() => {
    const SOLD = /\\b(adopted|sold|reserved)\\b/i;
    return [...new Set(
        Array.from(document.querySelectorAll('a[href]'))
            .filter(a => {
                const card = a.closest(
                    'article,[class*="card"],[class*="item"],li,[class*="puppy"]'
                ) || a.parentElement;
                return card ? !SOLD.test(card.textContent) : true;
            })
            .map(a => a.href)
    )];
}
"""


# ═══════════════════════════════════════════════════════════════════════════
# TTLCache — simple in-memory cache with per-key TTL
# ═══════════════════════════════════════════════════════════════════════════

class TTLCache:
    """
    Thread-safe-enough (GIL-protected) in-memory cache.
    Each key stores (value, expires_at).
    """
    def __init__(self, default_ttl: float):
        self.default_ttl = default_ttl
        self._store: dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry and time.time() < entry[1]:
            return entry[0]
        return None

    def set(self, key: str, value: Any, ttl: float | None = None):
        self._store[key] = (value, time.time() + (ttl or self.default_ttl))

    def clear(self, key: str | None = None):
        if key:
            self._store.pop(key, None)
        else:
            self._store.clear()

    def ttl_remaining(self, key: str) -> float:
        entry = self._store.get(key)
        if not entry:
            return 0.0
        return max(0.0, entry[1] - time.time())

    def has(self, key: str) -> bool:
        return self.get(key) is not None


# ── Per-source result caches ──────────────────────────────────────────────
# RescueGroups: cache ALL animals (breed filter applied in Python = no extra calls)
_rg_raw_cache   = TTLCache(300)    # 5 min  — key: "all"
# 954 Puppies:  cache each detail page individually
_fp_page_cache  = TTLCache(1800)   # 30 min — key: puppy URL
# Miami-Dade:   cache raw results
_mdas_raw_cache = TTLCache(600)    # 10 min — key: "all"


# ── Helpers ───────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_state(data: dict):
    STATE_FILE.write_text(json.dumps(data, indent=2))


def rg_headers() -> dict:
    h = {"Content-Type": "application/vnd.api+json"}
    if RG_KEY:
        h["Authorization"] = RG_KEY
    return h


def photo_str(obj) -> str:
    if not obj:
        return ""
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return (
            obj.get("large") or obj.get("url") or
            obj.get("medium") or obj.get("small") or
            obj.get("original") or ""
        )
    return ""


def matches_breed(name: str, primary: str, secondary: str, filt: str) -> bool:
    if not filt or filt.lower() == "all":
        return True
    b = filt.lower()
    return (
        b in (primary   or "").lower() or
        b in (secondary or "").lower() or
        b in (name      or "").lower()
    )


def _make_absolute(url: str, base: str) -> str:
    if not url or url.startswith("http"):
        return url or ""
    p    = urlparse(base)
    root = f"{p.scheme}://{p.netloc}"
    return root + url if url.startswith("/") else f"{root}/{url.lstrip('/')}"


def _extract_sex(text: str) -> str:
    t = text.lower()
    if "female" in t:
        return "Female"
    if " male" in t or t.startswith("male"):
        return "Male"
    return ""


def _age_from_birthdate(text: str) -> str:
    for pat, builder in [
        (r"(\d{1,2})/(\d{1,2})/(\d{4})",
         lambda m: date(int(m.group(3)), int(m.group(1)), int(m.group(2)))),
        (r"(\d{4})-(\d{2})-(\d{2})",
         lambda m: date(int(m.group(1)), int(m.group(2)), int(m.group(3)))),
    ]:
        m = re.search(pat, text)
        if m:
            try:
                birth  = builder(m)
                today  = date.today()
                months = max(0, (today.year - birth.year) * 12 + (today.month - birth.month))
                if months == 0:
                    weeks = max(0, (today - birth).days // 7)
                    return f"{weeks} week{'s' if weeks != 1 else ''} old"
                return f"{months} month{'s' if months != 1 else ''} old"
            except Exception:
                pass
    return ""


def _is_low_shed(text: str) -> bool:
    return any(k in text.lower() for k in RG_LOW_SHED_KEYWORDS)


def _rg_has_years(s: str) -> bool:
    m = re.search(r"(\d+)\s*y(?:ear|r)s?", s, re.I)
    return bool(m) and int(m.group(1)) > 0


def _is_valid_fp_url(url: str) -> bool:
    path = urlparse(url).path.lower().rstrip("/")
    segs = [s for s in path.split("/") if s]
    if len(segs) != 3 or segs[0] != "puppies":
        return False
    if segs[1] not in FP_BREED_SLUGS_SET:
        return False
    return bool(re.match(r"^\d{5,}$", segs[2]))


def _fp_is_sold(soup: BeautifulSoup, page_text: str) -> bool:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld    = json.loads(script.string or "")
            avail = (ld.get("offers") or {}).get("availability", "")
            if avail and "InStock" not in avail:
                return True
        except Exception:
            pass
    og = soup.find("meta", property="og:title")
    if og and re.search(r"\b(adopted|sold|reserved)\b", og.get("content", ""), re.I):
        return True
    title_el = soup.find("title")
    if title_el and re.search(r"\b(adopted|sold|reserved)\b", title_el.get_text(), re.I):
        return True
    for el in soup.find_all(class_=re.compile(r"status|badge|chip|tag|label|pill|ribbon", re.I)):
        if re.match(r"^(adopted|sold|reserved|unavailable|gone)$",
                    el.get_text(strip=True).lower()):
            return True
    if _FP_SOLD_RE.search(page_text):
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# RescueGroups — raw cache (breed-agnostic)
# ═══════════════════════════════════════════════════════════════════════════

def _rg_photo_map(animals: list, included: list) -> dict[str, str]:
    idx: dict[str, str] = {}
    for inc in included:
        if inc.get("type") == "pictures":
            u = photo_str(inc.get("attributes", {}))
            if u:
                idx[str(inc.get("id", ""))] = u
    m: dict[str, str] = {}
    for a in animals:
        aid  = str(a.get("id", ""))
        pics = a.get("relationships", {}).get("pictures", {}).get("data", [])
        for p in (pics if isinstance(pics, list) else []):
            pid = str(p.get("id", ""))
            if pid in idx:
                m[aid] = idx[pid]
                break
        if aid not in m:
            thumb = a.get("attributes", {}).get("pictureThumbnailUrl")
            if thumb:
                url_str = photo_str(thumb)
                if url_str:
                    m[aid] = url_str
    return m


def _rg_passes_base(a: dict) -> bool:
    """Pre-breed-filter checks (age, size, shed, status)."""
    attrs  = a.get("attributes", {})
    if (attrs.get("ageGroup")  or "").lower() not in RG_ALLOWED_AGE_GROUPS:  return False
    if (attrs.get("sizeGroup") or "").lower() not in RG_ALLOWED_SIZE_GROUPS: return False
    if (attrs.get("statusName") or "").lower().strip() in RG_UNAVAILABLE_STATUSES: return False
    age_s = (attrs.get("ageString") or "").lower()
    if _rg_has_years(age_s): return False
    mo = re.search(r"(\d+)\s*m(?:onth|o)s?", age_s, re.I)
    if mo and int(mo.group(1)) >= 6: return False
    breed = " ".join(filter(None, [
        attrs.get("breedPrimary") or "", attrs.get("breedSecondary") or ""
    ])).lower()
    return _is_low_shed(breed)


async def _rg_page(client: httpx.AsyncClient, page: int) -> dict:
    r = await client.get(
        f"{RG_BASE}/public/animals/search/available/dogs/",
        headers=rg_headers(),
        params={
            "limit": 25, "page": page, "include": "pictures",
            "fields[animals]": (
                "name,breedPrimary,breedSecondary,ageGroup,ageString,"
                "sizeGroup,sex,locationCity,locationState,locationDistance,"
                "updatedDate,urlSingleAdbk,pictureThumbnailUrl,statusName"
            ),
            "filters[postalcode]": ZIP_CODE,
            "filters[distance]":   RADIUS_MILES,
        },
    )
    r.raise_for_status()
    return r.json()


async def _fetch_rg_all() -> list:
    """
    Fetch ALL passing RescueGroups animals (no breed filter).
    Result cached for 5 minutes — one API burst per cache cycle.
    """
    cached = _rg_raw_cache.get("all")
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            pages = await asyncio.gather(
                *[_rg_page(client, p) for p in range(1, 11)],
                return_exceptions=True,
            )
    except Exception:
        return []

    all_a, all_i = [], []
    for pg in pages:
        if isinstance(pg, Exception):
            continue
        all_a.extend(pg.get("data",     []))
        all_i.extend(pg.get("included", []))

    pm  = _rg_photo_map(all_a, all_i)
    out = []
    for a in all_a:
        if not _rg_passes_base(a):
            continue
        at  = a.get("attributes", {})
        lid = str(a.get("id", ""))
        out.append({
            "id":              f"rg_{lid}",
            "name":            at.get("name", "Unknown"),
            "breed":           at.get("breedPrimary", ""),
            "breed_secondary": at.get("breedSecondary", ""),
            "age_text":        at.get("ageString", ""),
            "age_group":       at.get("ageGroup", ""),
            "sex":             at.get("sex", ""),
            "distance_miles":  at.get("locationDistance"),
            "city":            at.get("locationCity", ""),
            "state":           at.get("locationState", ""),
            "photo_url":       photo_str(pm.get(lid, "")),
            "source":          "RescueGroups",
            "source_type":     "rescue",
            "price":           "",
            "posted_at":       at.get("updatedDate", ""),
            "url":             at.get("urlSingleAdbk") or "",
            "low_shed":        True,
        })

    _rg_raw_cache.set("all", out)
    return out


async def fetch_rescuegroups(bf: str) -> list:
    if not RG_KEY:
        return []
    all_animals = await _fetch_rg_all()
    if not bf or bf.lower() == "all":
        return all_animals
    return [
        a for a in all_animals
        if matches_breed(a["name"], a["breed"], a["breed_secondary"], bf)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# 954 Puppies — per-page detail cache
# ═══════════════════════════════════════════════════════════════════════════

def _fp_sync_scrape_all_breeds() -> list[str]:
    all_urls: set[str] = set()
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            for slug in FP_BREED_SLUGS:
                page = browser.new_page(user_agent=SCRAPER_HEADERS["User-Agent"])
                try:
                    page.goto(
                        f"{FP_BASE}/puppies-for-sale/{slug}",
                        wait_until="domcontentloaded",
                        timeout=25_000,
                    )
                    page.wait_for_timeout(2_500)
                    try:
                        page.wait_for_function(
                            f'document.querySelectorAll(\'a[href*="/puppies/{slug}/\"]\').length > 0',
                            timeout=7_000,
                        )
                    except Exception:
                        pass
                    hrefs: list[str] = page.evaluate(_FP_HREF_JS)
                    for u in hrefs:
                        if _is_valid_fp_url(u):
                            all_urls.add(u)
                except Exception:
                    pass
                finally:
                    page.close()
            browser.close()
    except Exception:
        pass
    return list(all_urls)


def _fp_sync_scrape_one_breed(breed_slug: str) -> list[str]:
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            page = browser.new_page(user_agent=SCRAPER_HEADERS["User-Agent"])
            page.goto(
                f"{FP_BASE}/puppies-for-sale/{breed_slug}",
                wait_until="domcontentloaded",
                timeout=20_000,
            )
            page.wait_for_timeout(2_500)
            try:
                page.wait_for_function(
                    f'document.querySelectorAll(\'a[href*="/puppies/{breed_slug}/\"]\').length > 0',
                    timeout=7_000,
                )
            except Exception:
                pass
            hrefs: list[str] = page.evaluate(_FP_HREF_JS)
            browser.close()
            return [u for u in hrefs if _is_valid_fp_url(u)]
    except Exception:
        return []


async def _fp_refresh_cache() -> list[str]:
    global _fp_url_cache
    loop     = asyncio.get_event_loop()
    url_list = await loop.run_in_executor(_thread_pool, _fp_sync_scrape_all_breeds)
    _fp_url_cache = {"urls": url_list, "cached_at": time.time()}
    return url_list


async def _fp_get_all_urls() -> list[str]:
    if _fp_url_cache["urls"] and (time.time() - _fp_url_cache["cached_at"]) < FP_CACHE_TTL:
        return _fp_url_cache["urls"]
    return await _fp_refresh_cache()


@app.on_event("startup")
async def _on_startup():
    asyncio.create_task(_fp_refresh_cache())


async def _fp_parse_detail(
    client: httpx.AsyncClient, url: str
) -> dict | None:
    """
    Fetch and parse one 954puppies detail page.
    Result (including None for invalid/sold) is cached for 30 minutes.
    """
    cached = _fp_page_cache.get(url)
    if cached is not None:
        return cached  # may be the sentinel False (sold/invalid)

    try:
        r = await client.get(url, timeout=12.0)
        if r.status_code != 200 or not re.search(r"\$[\d,]+", r.text):
            _fp_page_cache.set(url, False)
            return None
        html = r.text
    except Exception:
        return None  # network errors: don't cache so we retry next time

    soup = BeautifulSoup(html, "html.parser")
    txt  = soup.get_text(" ", strip=True)

    if _fp_is_sold(soup, txt):
        _fp_page_cache.set(url, False)
        return None

    name = ""
    og   = soup.find("meta", property="og:title")
    if og:
        cand = re.split(r"\s*[-–|·]\s*", og.get("content", ""))[0].strip()
        if 1 < len(cand) <= 25:
            name = cand
    if not name:
        h1 = soup.find("h1")
        if h1:
            cand = h1.get_text(strip=True)
            if 1 < len(cand) <= 25 and "puppies" not in cand.lower():
                name = cand
    if not name or len(name) < 2:
        _fp_page_cache.set(url, False)
        return None

    segs       = urlparse(url).path.lower().strip("/").split("/")
    breed_slug = segs[1] if len(segs) >= 2 else ""
    breed_name = breed_slug.replace("-", " ").title()

    photo  = ""
    og_img = soup.find("meta", property="og:image")
    if og_img:
        photo = og_img.get("content", "")
    if not photo:
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if src and "placeholder" not in src.lower():
                photo = _make_absolute(src, url)
                break

    age   = _age_from_birthdate(txt) or "Puppy"
    sex   = _extract_sex(txt)
    pm    = re.search(r"\$[\d,]+", txt)
    price = pm.group(0) if pm else ""
    uid   = re.sub(r"[^a-z0-9]", "_", urlparse(url).path.lower())[-40:]

    result = {
        "id":              f"fp_{uid}",
        "name":            name,
        "breed":           breed_name,
        "breed_secondary": "",
        "age_text":        age,
        "age_group":       "baby",
        "sex":             sex,
        "distance_miles":  None,
        "city":            "Ft. Lauderdale",
        "state":           "FL",
        "photo_url":       photo,
        "source":          "954 Puppies",
        "source_type":     "store",
        "price":           price,
        "posted_at":       "",
        "url":             url,
        "low_shed":        True,
    }
    _fp_page_cache.set(url, result)
    return result


async def _fetch_fp_all() -> list:
    """
    Return all valid 954 puppies (no breed filter).
    Detail pages are individually cached — repeat calls are instant.
    """
    urls = await _fp_get_all_urls()
    if not urls:
        return []
    async with httpx.AsyncClient(
        timeout=30.0, follow_redirects=True, headers=SCRAPER_HEADERS
    ) as client:
        results = []
        for i in range(0, min(len(urls), 100), 8):
            for r in await asyncio.gather(
                *[_fp_parse_detail(client, u) for u in urls[i:i+8]],
                return_exceptions=True,
            ):
                if isinstance(r, dict) and r:
                    results.append(r)
        return results


async def fetch_954_puppies(bf: str) -> list:
    all_puppies = await _fetch_fp_all()
    if not bf or bf.lower() == "all":
        return all_puppies
    return [
        p for p in all_puppies
        if matches_breed(p["name"], p["breed"], p["breed_secondary"], bf)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Miami-Dade — raw cache
# ═══════════════════════════════════════════════════════════════════════════

def _parse_mdas(html: str, base: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    cards = (
        soup.find_all("div", class_=re.compile(r"animal|pet|dog|result|card|listing", re.I)) or
        soup.find_all("article") or
        [t for t in soup.find_all("div")
         if t.find("img") and t.find(["h2", "h3", "h4"])
         and len(t.get_text(strip=True)) > 15]
    )
    for card in cards:
        txt     = card.get_text(" ", strip=True)
        name_el = card.find(["h2", "h3", "h4"]) or card.find(class_=re.compile(r"name", re.I))
        name    = name_el.get_text(strip=True) if name_el else ""
        if not name or len(name) < 2 or len(name) > 40 or name in seen:
            continue
        seen.add(name)
        bel   = card.find(class_=re.compile(r"breed|species", re.I))
        breed = bel.get_text(strip=True) if bel else ""
        am    = re.search(r"\b(\d+\s*(?:month|year|week|mo|yr|wk)s?\s*old)\b", txt, re.I)
        age_s = am.group(1) if am else ""
        if _rg_has_years(age_s):
            continue
        if not _is_low_shed(f"{name} {breed}"):
            continue
        img = card.find("img")
        ph  = _make_absolute(
            photo_str(img.get("src") or img.get("data-src") or ""), base
        ) if img else ""
        lk  = card.find("a", href=True)
        out.append({
            "id":              f"mdas_{re.sub(r'[^a-z0-9]', '_', name.lower())}",
            "name":            name,
            "breed":           breed,
            "breed_secondary": "",
            "age_text":        age_s or "Puppy",
            "age_group":       "baby",
            "sex":             _extract_sex(txt),
            "distance_miles":  None,
            "city":            "Miami",
            "state":           "FL",
            "photo_url":       ph,
            "source":          "Miami-Dade Animal Services",
            "source_type":     "rescue",
            "price":           "",
            "posted_at":       "",
            "url":             _make_absolute(lk["href"], base) if lk else base,
            "low_shed":        True,
        })
    return out[:50]


async def _fetch_mdas_all() -> list:
    cached = _mdas_raw_cache.get("all")
    if cached is not None:
        return cached
    async with httpx.AsyncClient(
        timeout=25.0, follow_redirects=True, headers=SCRAPER_HEADERS
    ) as client:
        for url in MDAS_URLS:
            try:
                r = await client.get(url)
                if r.status_code == 200 and len(r.text) > 500:
                    res = _parse_mdas(r.text, str(r.url))
                    if res:
                        _mdas_raw_cache.set("all", res)
                        return res
            except Exception:
                continue
    _mdas_raw_cache.set("all", [])
    return []


async def fetch_miami_dade(bf: str) -> list:
    all_mdas = await _fetch_mdas_all()
    if not bf or bf.lower() == "all":
        return all_mdas
    return [
        m for m in all_mdas
        if matches_breed(m["name"], m["breed"], m["breed_secondary"], bf)
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Models & routes
# ═══════════════════════════════════════════════════════════════════════════

class StateUpdate(BaseModel):
    listing_id: str
    status: str
    note: str = ""


@app.get("/api/breeds")
def get_breeds():
    return {"breeds": BREEDS}


@app.get("/api/cache-status")
def cache_status():
    """Show TTL remaining and entry counts for all caches."""
    fp_pages_cached = sum(
        1 for k, (v, exp) in _fp_page_cache._store.items()
        if v is not False and time.time() < exp
    )
    return {
        "rescuegroups": {
            "cached":        _rg_raw_cache.has("all"),
            "ttl_remaining": round(_rg_raw_cache.ttl_remaining("all")),
            "entries":       len(_rg_raw_cache.get("all") or []),
        },
        "954_puppies_urls": {
            "cached":        bool(_fp_url_cache["urls"]),
            "ttl_remaining": round(max(0.0, FP_CACHE_TTL - (time.time() - _fp_url_cache["cached_at"]))),
            "entries":       len(_fp_url_cache["urls"]),
        },
        "954_puppies_pages": {
            "cached_pages":  fp_pages_cached,
            "ttl_remaining": "per-page (30 min each)",
        },
        "miami_dade": {
            "cached":        _mdas_raw_cache.has("all"),
            "ttl_remaining": round(_mdas_raw_cache.ttl_remaining("all")),
            "entries":       len(_mdas_raw_cache.get("all") or []),
        },
    }


@app.post("/api/cache-clear")
def cache_clear():
    """Bust all result caches (called by the Refresh button)."""
    _rg_raw_cache.clear()
    _mdas_raw_cache.clear()
    # Don't clear fp_page_cache or fp_url_cache — those are expensive to rebuild
    # and puppy listings don't change that fast
    return {"ok": True, "cleared": ["rescuegroups", "miami_dade"]}


@app.get("/api/debug")
async def debug_api():
    cache_age = round(time.time() - _fp_url_cache["cached_at"])
    result = {
        "rescuegroups_key": bool(RG_KEY),
        "fp_breed_count":   len(FP_BREED_SLUGS),
        "fp_cached_urls":   len(_fp_url_cache["urls"]),
        "fp_cache_age_sec": cache_age if cache_age < 1_000_000 else "not populated",
        "playwright_mode":  "sync_playwright in ThreadPoolExecutor (Windows-safe)",
        "cache_status":     cache_status(),
        "sources":          {},
    }
    if RG_KEY:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                data = await _rg_page(client, 1)
            animals = data.get("data", [])
            age_c   = {}
            for a in animals:
                ag = a.get("attributes", {}).get("ageGroup", "Unknown")
                age_c[ag] = age_c.get(ag, 0) + 1
            result["sources"]["rescuegroups"] = {
                "status":        "ok",
                "total_page1":   len(animals),
                "passing_base":  sum(1 for a in animals if _rg_passes_base(a)),
                "age_breakdown": age_c,
            }
        except Exception as e:
            result["sources"]["rescuegroups"] = {"status": "error", "detail": str(e)}
    try:
        loop      = asyncio.get_event_loop()
        test_urls = await loop.run_in_executor(
            _thread_pool, _fp_sync_scrape_one_breed, "maltese"
        )
        result["sources"]["954_puppies"] = {
            "status":          "ok",
            "test_breed":      "maltese",
            "test_urls_found": len(test_urls),
            "sample_urls":     test_urls[:4],
            "cached_urls":     len(_fp_url_cache["urls"]),
        }
    except Exception as e:
        result["sources"]["954_puppies"] = {"status": "error", "detail": str(e)}
    try:
        mdas = await _fetch_mdas_all()
        result["sources"]["miami_dade"] = {"status": "ok", "listings_found": len(mdas)}
    except Exception as e:
        result["sources"]["miami_dade"] = {"status": "error", "detail": str(e)}
    return result


@app.get("/api/fp-refresh")
async def fp_refresh():
    urls = await _fp_refresh_cache()
    cached_until = datetime.utcfromtimestamp(
        _fp_url_cache["cached_at"] + FP_CACHE_TTL
    ).strftime("%Y-%m-%d %H:%M UTC")
    return {"status": "ok", "urls_found": len(urls), "cached_until": cached_until, "sample": urls[:5]}


@app.get("/api/search")
async def search_puppies(
    breed:  str = Query(default="All"),
    sort:   str = Query(default="newest"),
    source: str = Query(default="all"),
):
    rg, mdas, fp = await asyncio.gather(
        fetch_rescuegroups(breed),
        fetch_miami_dade(breed),
        fetch_954_puppies(breed),
        return_exceptions=True,
    )
    all_results: list = []
    for r in [rg, mdas, fp]:
        if isinstance(r, list):
            all_results.extend(r)

    if source == "store":
        all_results = [x for x in all_results if x.get("source_type") == "store"]
    elif source == "rescue":
        all_results = [x for x in all_results if x.get("source_type") == "rescue"]

    seen, deduped = set(), []
    for item in all_results:
        key = f"{item['name'].lower().strip()}_{item['breed'].lower().strip()}"
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    if sort == "distance":
        deduped.sort(key=lambda x: x.get("distance_miles") or 9999)
    else:
        deduped.sort(key=lambda x: x.get("posted_at") or "", reverse=True)

    state   = load_state()
    results = []
    for item in deduped:
        lid = item["id"]
        s   = state.get(lid, {})
        if s.get("status") == "hidden":
            continue
        item["status"] = s.get("status", "new")
        item["note"]   = s.get("note", "")
        results.append(item)

    # Include cache metadata so the frontend knows how fresh the data is
    return {
        "results": results,
        "total":   len(results),
        "cache": {
            "rg_ttl":   round(_rg_raw_cache.ttl_remaining("all")),
            "mdas_ttl": round(_mdas_raw_cache.ttl_remaining("all")),
            "fp_pages": sum(
                1 for k, (v, exp) in _fp_page_cache._store.items()
                if v is not False and time.time() < exp
            ),
        },
    }


@app.get("/api/state")
def get_state():
    return load_state()


@app.post("/api/state")
def update_state(body: StateUpdate):
    state = load_state()
    if body.status == "none":
        state.pop(body.listing_id, None)
    else:
        state[body.listing_id] = {
            "status":     body.status,
            "note":       body.note,
            "updated_at": datetime.utcnow().isoformat(),
        }
    save_state(state)
    return {"ok": True}


@app.get("/api/favorites")
def get_favorites():
    s = load_state()
    return [{"id": k, **v} for k, v in s.items() if v.get("status") == "favorite"]


@app.get("/api/hidden")
def get_hidden():
    s = load_state()
    return [{"id": k, **v} for k, v in s.items() if v.get("status") == "hidden"]


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def serve_index():
    return FileResponse(str(STATIC_DIR / "index.html"))