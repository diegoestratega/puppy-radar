import asyncio
import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
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

RG_ALLOWED_AGE_GROUPS  = {"baby", "young"}
RG_ALLOWED_SIZE_GROUPS = {"small"}

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
    if not url:
        return ""
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        p = urlparse(base)
        return f"{p.scheme}://{p.netloc}{url}"
    return url


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
    """Accept ONLY /puppies/{breed-slug}/{5+ digit id}"""
    path = urlparse(url).path.lower().rstrip("/")
    segs = [s for s in path.split("/") if s]
    if len(segs) != 3 or segs[0] != "puppies":
        return False
    if segs[1] not in FP_BREED_SLUGS_SET:
        return False
    return bool(re.match(r"^\d{5,}$", segs[2]))


# ── RescueGroups ──────────────────────────────────────────────────────────

def _rg_photo_map(animals: list, included: list) -> dict:
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
                m[aid] = photo_str(thumb)
    return m


def _rg_passes(a: dict, bf: str) -> bool:
    attrs  = a.get("attributes", {})
    age_g  = (attrs.get("ageGroup")  or "").lower()
    size_g = (attrs.get("sizeGroup") or "").lower()

    if age_g  not in RG_ALLOWED_AGE_GROUPS:  return False
    if size_g not in RG_ALLOWED_SIZE_GROUPS: return False

    age_s = (attrs.get("ageString") or "").lower()
    if _rg_has_years(age_s):
        return False
    mo = re.search(r"(\d+)\s*m(?:onth|o)s?", age_s, re.I)
    if mo and int(mo.group(1)) >= 6:
        return False

    breed = " ".join(filter(None, [
        attrs.get("breedPrimary") or "",
        attrs.get("breedSecondary") or "",
    ])).lower()
    if not _is_low_shed(breed):
        return False

    return matches_breed(
        attrs.get("name", ""),
        attrs.get("breedPrimary", ""),
        attrs.get("breedSecondary", ""),
        bf,
    )


async def _rg_page(client: httpx.AsyncClient, page: int) -> dict:
    r = await client.get(
        f"{RG_BASE}/public/animals/search/available/dogs/",
        headers=rg_headers(),
        params={
            "limit": 25, "page": page, "include": "pictures",
            "fields[animals]": (
                "name,breedPrimary,breedSecondary,ageGroup,ageString,"
                "sizeGroup,sex,locationCity,locationState,locationDistance,"
                "updatedDate,urlSingleAdbk,pictureThumbnailUrl"
            ),
            "filters[postalcode]": ZIP_CODE,
            "filters[distance]":   RADIUS_MILES,
        },
    )
    r.raise_for_status()
    return r.json()


async def fetch_rescuegroups(bf: str) -> list:
    if not RG_KEY:
        return []
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
        all_a.extend(pg.get("data", []))
        all_i.extend(pg.get("included", []))

    pm  = _rg_photo_map(all_a, all_i)
    out = []
    for a in all_a:
        if not _rg_passes(a, bf):
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
            "photo_url":       pm.get(lid, ""),
            "source":          "RescueGroups",
            "posted_at":       at.get("updatedDate", ""),
            "url":             at.get("urlSingleAdbk", ""),
            "low_shed":        True,
        })
    return out


# ── 954 Puppies — pure httpx, no browser needed ───────────────────────────
#
# 954puppies.com runs on Astro which server-renders all HTML.
# No JavaScript execution needed — plain httpx fetches return full DOM.
# All 23 breed pages are fetched concurrently (semaphore=5 to be polite).
# Total refresh time: ~10-15 seconds instead of 5+ minutes with Playwright.

async def _fp_fetch_breed_urls(
    client: httpx.AsyncClient, slug: str
) -> list[str]:
    """
    Fetch one breed listing page and extract all valid individual puppy URLs.
    Works because Astro pre-renders the puppy card links server-side.
    """
    try:
        r = await client.get(
            f"{FP_BASE}/puppies-for-sale/{slug}",
            timeout=15.0,
        )
        if r.status_code != 200:
            return []

        soup  = BeautifulSoup(r.text, "lxml")
        found = set()

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Normalise to absolute URL
            if href.startswith("/"):
                href = f"{FP_BASE}{href}"
            elif not href.startswith("http"):
                continue
            if _is_valid_fp_url(href):
                found.add(href)

        return list(found)

    except Exception:
        return []


async def _fp_refresh_cache() -> list[str]:
    """Fetch all 23 breed listing pages concurrently and update the cache."""
    global _fp_url_cache

    sem = asyncio.Semaphore(5)

    async def guarded(client: httpx.AsyncClient, slug: str) -> list[str]:
        async with sem:
            return await _fp_fetch_breed_urls(client, slug)

    async with httpx.AsyncClient(
        headers=SCRAPER_HEADERS,
        follow_redirects=True,
        timeout=20.0,
    ) as client:
        results = await asyncio.gather(
            *[guarded(client, slug) for slug in FP_BREED_SLUGS],
            return_exceptions=True,
        )

    all_urls: set[str] = set()
    for r in results:
        if isinstance(r, list):
            all_urls.update(r)

    url_list = list(all_urls)
    _fp_url_cache = {"urls": url_list, "cached_at": time.time()}
    return url_list


async def _fp_get_all_urls() -> list[str]:
    """Return cached URLs or refresh if stale / empty."""
    if _fp_url_cache["urls"] and (time.time() - _fp_url_cache["cached_at"]) < FP_CACHE_TTL:
        return _fp_url_cache["urls"]
    return await _fp_refresh_cache()


@app.on_event("startup")
async def _on_startup():
    """Warm the 954 Puppies cache in the background — fast now with httpx."""
    asyncio.create_task(_fp_refresh_cache())


async def _fp_parse_detail(
    client: httpx.AsyncClient, url: str, bf: str
) -> dict | None:
    """
    Fetch an individual puppy detail page (static Astro HTML).
    Extract name, photo, age, sex, price.
    """
    try:
        r = await client.get(url, timeout=12.0)
        if r.status_code != 200 or not re.search(r"\$[\d,]+", r.text):
            return None
        html = r.text
    except Exception:
        return None

    soup = BeautifulSoup(html, "lxml")
    txt  = soup.get_text(" ", strip=True)

    # Name from og:title: "Blu - French Bulldog Puppy | 954 Puppies"
    name = ""
    og   = soup.find("meta", property="og:title")
    if og:
        cand = re.split(r"\s*[-–|]\s*", og.get("content", ""))[0].strip()
        if 1 < len(cand) <= 25:
            name = cand
    if not name:
        h1 = soup.find("h1")
        if h1:
            cand = h1.get_text(strip=True)
            if 1 < len(cand) <= 25 and "puppies" not in cand.lower():
                name = cand
    if not name or len(name) < 2:
        return None

    segs       = urlparse(url).path.lower().strip("/").split("/")
    breed_slug = segs[1] if len(segs) >= 2 else ""
    breed_name = breed_slug.replace("-", " ").title()

    if not matches_breed(name, breed_name, "", bf):
        return None

    # Skip if page says "adopted" or "sold"
    body_lower = txt.lower()
    if any(w in body_lower for w in ("adopted", "this puppy has been", "no longer available")):
        return None

    photo = ""
    og_img = soup.find("meta", property="og:image")
    if og_img:
        photo = og_img.get("content", "")
    if not photo:
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if src and "placeholder" not in src.lower() and src.startswith("http"):
                photo = src
                break

    age   = _age_from_birthdate(txt) or "Puppy"
    sex   = _extract_sex(txt)
    pm    = re.search(r"\$[\d,]+", txt)
    price = pm.group(0) if pm else ""
    uid   = re.sub(r"[^a-z0-9]", "_", urlparse(url).path.lower())[-40:]

    return {
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
        "posted_at":       "",
        "url":             url,
        "price":           price,
        "low_shed":        True,
    }


async def fetch_954_puppies(bf: str) -> list:
    urls = await _fp_get_all_urls()
    if not urls:
        return []

    async with httpx.AsyncClient(
        timeout=30.0, follow_redirects=True, headers=SCRAPER_HEADERS
    ) as client:
        results = []
        # Process in batches of 10 concurrently
        for i in range(0, min(len(urls), 150), 10):
            batch = urls[i:i+10]
            for r in await asyncio.gather(
                *[_fp_parse_detail(client, u, bf) for u in batch],
                return_exceptions=True,
            ):
                if isinstance(r, dict) and r:
                    results.append(r)
        return results


# ── Miami-Dade Animal Services ────────────────────────────────────────────

def _parse_mdas(html: str, base: str, bf: str) -> list:
    soup = BeautifulSoup(html, "lxml")
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
        if not matches_breed(name, breed, "", bf):
            continue
        img = card.find("img")
        ph  = _make_absolute(
            (img.get("src") or img.get("data-src") or ""), base
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
            "posted_at":       "",
            "url":             _make_absolute(lk["href"], base) if lk else base,
            "low_shed":        True,
        })
    return out[:50]


async def fetch_miami_dade(bf: str) -> list:
    async with httpx.AsyncClient(
        timeout=25.0, follow_redirects=True, headers=SCRAPER_HEADERS
    ) as client:
        for url in MDAS_URLS:
            try:
                r = await client.get(url)
                if r.status_code == 200 and len(r.text) > 500:
                    res = _parse_mdas(r.text, str(r.url), bf)
                    if res:
                        return res
            except Exception:
                continue
    return []


# ── Models & Routes ───────────────────────────────────────────────────────

class StateUpdate(BaseModel):
    listing_id: str
    status: str
    note: str = ""


@app.get("/api/breeds")
def get_breeds():
    return {"breeds": BREEDS}


@app.get("/api/debug")
async def debug_api():
    cache_age = round(time.time() - _fp_url_cache["cached_at"])
    result = {
        "rescuegroups_key": bool(RG_KEY),
        "fp_breed_count":   len(FP_BREED_SLUGS),
        "fp_cached_urls":   len(_fp_url_cache["urls"]),
        "fp_cache_age_sec": cache_age if cache_age < 1_000_000 else "cache not yet populated",
        "scraping_mode":    "pure httpx — no browser, no Playwright",
        "sources":          {},
    }

    # RescueGroups
    if RG_KEY:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                data = await _rg_page(client, 1)
            animals = data.get("data", [])
            age_c   = {}
            for a in animals:
                ag = a.get("attributes", {}).get("ageGroup", "Unknown")
                age_c[ag] = age_c.get(ag, 0) + 1
            passing = [a for a in animals if _rg_passes(a, "All")]
            result["sources"]["rescuegroups"] = {
                "status":              "ok",
                "total_page1":         len(animals),
                "passing_all_filters": len(passing),
                "age_breakdown":       age_c,
            }
        except Exception as e:
            result["sources"]["rescuegroups"] = {"status": "error", "detail": str(e)}

    # 954 Puppies — test one breed page quickly
    try:
        async with httpx.AsyncClient(
            headers=SCRAPER_HEADERS, follow_redirects=True, timeout=15.0
        ) as client:
            test_urls = await _fp_fetch_breed_urls(client, "maltese")

        result["sources"]["954_puppies"] = {
            "status":          "ok",
            "test_breed":      "maltese",
            "test_urls_found": len(test_urls),
            "sample_urls":     test_urls[:4],
            "cached_urls":     len(_fp_url_cache["urls"]),
            "note":            "httpx only — no browser. If test_urls_found=0, page may be JS-rendered.",
        }
    except Exception as e:
        result["sources"]["954_puppies"] = {"status": "error", "detail": str(e)}

    # Miami-Dade
    try:
        mdas = await fetch_miami_dade("All")
        result["sources"]["miami_dade"] = {
            "status":         "ok",
            "listings_found": len(mdas),
            "sample":         [r["name"] for r in mdas[:5]],
        }
    except Exception as e:
        result["sources"]["miami_dade"] = {"status": "error", "detail": str(e)}

    return result


@app.get("/api/fp-refresh")
async def fp_refresh():
    """
    Refresh 954 Puppies URL cache using httpx.
    Should complete in ~10-15 seconds (vs 5+ minutes with Playwright).
    """
    t0   = time.time()
    urls = await _fp_refresh_cache()
    elapsed = round(time.time() - t0, 1)
    cached_until = datetime.utcfromtimestamp(
        _fp_url_cache["cached_at"] + FP_CACHE_TTL
    ).strftime("%Y-%m-%d %H:%M UTC")
    return {
        "status":         "ok",
        "urls_found":     len(urls),
        "elapsed_sec":    elapsed,
        "cached_until":   cached_until,
        "sample":         urls[:5],
    }


@app.get("/api/search")
async def search_puppies(breed: str = "All", sort: str = "newest"):
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

    return {"results": results, "total": len(results)}


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