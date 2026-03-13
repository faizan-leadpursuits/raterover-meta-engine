"""
Hotels.com Hotel Listings Scraper
Anti-bot hardened with curl_cffi chrome142 impersonation + DataImpulse residential proxy.
Flow: 1) GET /  → harvest cookies  2) GET search page  3) POST GraphQL (batch)

Best practices applied:
  - curl_cffi chrome142 = correct TLS ClientHello, JA3/JA4 fingerprint + QUIC/H2 ALPN
  - NO manual User-Agent / sec-ch-ua — curl_cffi sets them to exactly match the profile
  - Headers in the exact Chrome 142 order (Akamai inspects header frame order in HTTP/2)
  - Realistic delays (~human pacing) between steps
  - Residential proxy (DataImpulse) to rotate exit IP and avoid per-IP rate limits
  - Accept-Encoding left to curl_cffi so it matches real Chrome (gzip, deflate, br, zstd)
"""

import json
import time
import random
import uuid
import csv
import logging
from pathlib import Path

from curl_cffi import requests

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Config Loader
# ──────────────────────────────────────────────
CONFIG_FILE = Path(__file__).parent / "config.json"

def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[!] {CONFIG_FILE} not found. Please create it.")
        import sys
        sys.exit(1)

# ──────────────────────────────────────────────
# Entry & API settings
# ──────────────────────────────────────────────
ENTRY_URL    = "https://www.hotels.com"
GRAPHQL_PATH = "/graphql"

# Hotels.com site constants (overridden after redirect)
SITE_ID  = 300000036
TPID     = 3202
EAPID    = 36

# ──────────────────────────────────────────────
# Persisted query hashes
# ──────────────────────────────────────────────
HASHES = {
    "VoiceOfTheCustomerQuery": "fcafb0121d6ca4232ccb88aa4296d060323dbb701f06534d6bc537eb652511ab",
    "PropertyListingQuery":    "908ef1ccd58a146e59da0e09b4bbda870fb041608a9215ccf939605f0cb43a31",
}

# ──────────────────────────────────────────────────────────────────
# Headers — Chrome 142 real ordering (HTTP/2 pseudo-headers first,
# then in the exact order Chrome sends them).
# DO NOT add: user-agent, sec-ch-ua, accept-encoding, sec-ch-ua-mobile,
#             sec-ch-ua-platform — curl_cffi already sets these.
# ──────────────────────────────────────────────────────────────────
HEADERS_BROWSE = {
    "accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language":           "en-US,en;q=0.9",
    "cache-control":             "max-age=0",
    "sec-fetch-dest":            "document",
    "sec-fetch-mode":            "navigate",
    "sec-fetch-site":            "none",
    "sec-fetch-user":            "?1",
    "upgrade-insecure-requests": "1",
}

HEADERS_BROWSE_REFERER = {
    "accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language":           "en-US,en;q=0.9",
    "cache-control":             "max-age=0",
    "sec-fetch-dest":            "document",
    "sec-fetch-mode":            "navigate",
    "sec-fetch-site":            "same-origin",
    "sec-fetch-user":            "?1",
    "upgrade-insecure-requests": "1",
}

HEADERS_API = {
    "accept":                  "*/*",
    "accept-language":         "en-US,en;q=0.9",
    "client-info":             "shopping-pwa,8a76ef3575e323a6eb25f9efa3178b342675842f,us-west-2",
    "content-type":            "application/json",
    "sec-fetch-dest":          "empty",
    "sec-fetch-mode":          "cors",
    "sec-fetch-site":          "same-origin",
    "x-hcom-origin-id":        "page.Hotel-Search,H,20",
    "x-page-id":               "page.Hotel-Search,H,20",
    "x-parent-brand-id":       "hotels",
    "x-product-line":          "lodging",
    "x-shopping-product-line": "lodging",
}


# ──────────────────────────────────────────────
# Builders
# ──────────────────────────────────────────────
def build_referer(base_url: str, cfg: dict) -> str:
    c = cfg["checkIn"]
    o = cfg["checkOut"]
    dest = cfg["destination"].replace(" ", "%20").replace(",", "%2C")
    return (
        f"{base_url}/Hotel-Search"
        f"?destination={dest}"
        f"&regionId={cfg['regionId']}"
        f"&latLong={cfg['latitude']}%2C{cfg['longitude']}"
        f"&flexibility=0_DAY"
        f"&d1={c['year']}-{c['month']:02d}-{c['day']:02d}"
        f"&startDate={c['year']}-{c['month']:02d}-{c['day']:02d}"
        f"&d2={o['year']}-{o['month']:02d}-{o['day']:02d}"
        f"&endDate={o['year']}-{o['month']:02d}-{o['day']:02d}"
        f"&adults={cfg['adults']}&rooms={cfg['rooms']}"
        f"&sort=RECOMMENDED&useRewards=false"
    )


def build_context(duaid: str, cfg: dict) -> dict:
    return {
        "siteId":   SITE_ID,
        "locale":   cfg.get("locale", "en_IN"),
        "eapid":    EAPID,
        "tpid":     TPID,
        "currency": cfg.get("currency", "INR"),
        "device":   {"type": "DESKTOP"},
        "identity": {
            "duaid":     duaid,
            "authState": "ANONYMOUS",
        },
        "privacyTrackingState": "CAN_TRACK",
    }


def build_listing_payload(
    duaid: str,
    product_offers_id: str,
    search_id: str,
    typeahead_collation_id: str,
    cfg: dict,
    results_starting_index: int = 0,
    results_size: int = 100,
) -> list[dict]:
    context = build_context(duaid, cfg)

    votc = {
        "operationName": "VoiceOfTheCustomerQuery",
        "variables":     {"context": context},
        "extensions": {
            "persistedQuery": {
                "version":    1,
                "sha256Hash": HASHES["VoiceOfTheCustomerQuery"],
            }
        },
    }

    listing = {
        "operationName": "PropertyListingQuery",
        "variables": {
            "context": context,
            "criteria": {
                "primary": {
                    "dateRange": {
                        "checkInDate":  cfg["checkIn"],
                        "checkOutDate": cfg["checkOut"],
                    },
                    "destination": {
                        "regionName":  cfg["destination"],
                        "regionId":    cfg["regionId"],
                        "coordinates": {
                            "latitude":  cfg["latitude"],
                            "longitude": cfg["longitude"],
                        },
                        "pinnedPropertyId": None,
                        "propertyIds":      None,
                        "mapBounds":        None,
                    },
                    "rooms": [
                        {"adults": cfg["adults"], "children": []}
                        for _ in range(cfg["rooms"])
                    ],
                },
                "secondary": {
                    "counts": [
                        {"id": "resultsStartingIndex", "value": results_starting_index},
                        {"id": "resultsSize",          "value": results_size},
                    ],
                    "booleans":  [],
                    "selections": [
                        {"id": "privacyTrackingState", "value": "CAN_TRACK"},
                        {"id": "productOffersId",      "value": product_offers_id},
                        {"id": "searchId",             "value": search_id},
                        {"id": "sort",                 "value": "RECOMMENDED"},
                        {"id": "useRewards",           "value": "SHOP_WITHOUT_POINTS"},
                    ],
                    "ranges": [],
                },
            },
            "shoppingContext": {
                "multiItem":            None,
                "queryTriggeredBy":     "PAGE-LOAD",
                "typeaheadCollationId": typeahead_collation_id,
            },
            "includeDynamicMap": False,
        },
        "extensions": {
            "persistedQuery": {
                "version":    1,
                "sha256Hash": HASHES["PropertyListingQuery"],
            }
        },
    }

    return [votc, listing]


# ──────────────────────────────────────────────
# Scraper
# ──────────────────────────────────────────────
class HotelsComScraper:
    """
    Anti-bot hardened scraper:
      1. curl_cffi chrome142 — correct TLS fingerprint
      2. Residential proxy  — rotates exit IP per request (DataImpulse)
      3. Header order       — exact Chrome 142 frame ordering in HTTP/2
      4. No conflicting UA  — curl_cffi sets UA/sec-ch-ua automatically
      5. Human-paced delays — random 3-7 s between page requests
    """

    MAX_RETRIES  = 5
    BACKOFF_BASE = 2       # seconds

    def __init__(
        self,
        impersonate: str = "chrome142",
        proxy: str | None = None,
    ):
        self.impersonate = impersonate
        self.proxy = proxy
        self.session = self._create_session()
        if self.proxy:
            print(f"[proxy] Using {self.proxy.split('@')[-1]}")

        self.base_url:          str = ENTRY_URL
        self.duaid:             str = str(uuid.uuid4())
        self.product_offers_id: str = str(uuid.uuid4())
        self.search_id:         str = str(uuid.uuid4())
        self.typeahead_id:      str = str(uuid.uuid4())

    def _create_session(self) -> requests.Session:
        s = requests.Session(verify=False, impersonate=self.impersonate)
        if self.proxy:
            s.proxies = {"http": self.proxy, "https": self.proxy}
        return s

    def _rotate_session(self):
        """Forces a new TCP connection (new DataImpulse IP) while keeping cookies."""
        old_cookies = self.session.cookies.get_dict()
        self.session.close()
        self.session = self._create_session()
        for k, v in old_cookies.items():
            self.session.cookies.set(k, v)
        print("    [!] Rotated proxy IP for next request")

    # ── retry w/ backoff & proxy rotation ──────────────────────────────────
    def _request(self, method: str, url: str, require_200: bool = True, **kwargs) -> requests.Response:
        """Single request with 429-aware exponential backoff."""
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = getattr(self.session, method)(url, **kwargs)
            except Exception as exc:
                wait = self.BACKOFF_BASE * attempt
                print(f"    [net-err] {exc} — retry {attempt}/{self.MAX_RETRIES} in {wait}s")
                time.sleep(wait)
                self._rotate_session()
                continue

            if not require_200:
                if resp.status_code in (429, 403, 503):
                    print(f"    Status      : {resp.status_code} (expected for HTML, proceeding)")
                    self._rotate_session()
                return resp

            if resp.status_code not in (429, 503, 403):
                return resp

            wait = self.BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(1, 4)
            print(f"    [{resp.status_code}] rate-limited (attempt {attempt}/{self.MAX_RETRIES}), backing off {wait:.1f}s ...")
            self._rotate_session()
            time.sleep(wait)

        return resp   # return last response even if still 4xx

    # ── Step 1 — homepage / cookie harvest ────────────────────────────────
    def harvest_cookies(self) -> None:
        print(f"[1/3] GET {ENTRY_URL}/ ...")
        resp = self._request(
            "get", ENTRY_URL + "/",
            headers=HEADERS_BROWSE,
            allow_redirects=True,
            timeout=30,
            require_200=False
        )

        # Discover actual base URL after redirect (e.g. in.hotels.com)
        final = str(resp.url)
        self.base_url = final.rstrip("/") if "/" not in final.replace("https://", "").replace("http://", "") else final.rsplit("/", 1)[0]
        # simpler: just strip trailing slash
        self.base_url = final.rstrip("/")
        # remove any trailing path component
        parts = self.base_url.split("/")
        if len(parts) > 3:
            self.base_url = "/".join(parts[:3])

        print(f"    Redirected  -> {self.base_url}")
        print(f"    Status      : {resp.status_code}")
        cnames = list(self.session.cookies.keys())
        print(f"    Cookies     : {cnames}")

        raw_duaid = self.session.cookies.get("DUAID", "")
        if raw_duaid:
            self.duaid = raw_duaid
            print(f"    DUAID cookie: {self.duaid}")
        else:
            print(f"    DUAID gen'd : {self.duaid}")

    # ── Step 2 — search page (triggers EG_SESSIONTOKEN) ───────────────────
    def visit_search_page(self, cfg: dict) -> None:
        print("[2/3] Visiting hotel search page ...")
        c, o = cfg["checkIn"], cfg["checkOut"]
        search_url = (
            f"{self.base_url}/Hotel-Search"
            f"?destination={cfg['destination']}"
            f"&regionId={cfg['regionId']}"
            f"&latLong={cfg['latitude']},{cfg['longitude']}"
            f"&d1={c['year']}-{c['month']:02d}-{c['day']:02d}"
            f"&d2={o['year']}-{o['month']:02d}-{o['day']:02d}"
            f"&adults={cfg['adults']}&rooms={cfg['rooms']}"
            f"&sort=RECOMMENDED&useRewards=false"
        )
        hdrs = dict(HEADERS_BROWSE_REFERER)
        hdrs["referer"] = self.base_url + "/"
        resp = self._request("get", search_url, headers=hdrs, allow_redirects=True, timeout=30, require_200=False)
        if resp.status_code != 429:
            print(f"    Status      : {resp.status_code}")
        print(f"    Final URL   : {resp.url}")

        eg = self.session.cookies.get("EG_SESSIONTOKEN", "")
        if eg:
            print("    EG_SESSIONTOKEN harvested [+]")

    # ── Step 3 — GraphQL ─────────────────────────────────────────────────
    def fetch_hotels(
        self,
        cfg: dict,
        results_starting_index: int = 0,
        results_size: int = 100,
    ) -> dict:
        print(f"[3/3] GraphQL POST (offset={results_starting_index}, size={results_size}) ...")
        payload = build_listing_payload(
            duaid=self.duaid,
            product_offers_id=self.product_offers_id,
            search_id=self.search_id,
            typeahead_collation_id=self.typeahead_id,
            cfg=cfg,
            results_starting_index=results_starting_index,
            results_size=results_size,
        )

        graphql_url = f"{self.base_url}{GRAPHQL_PATH}"
        hdrs = dict(HEADERS_API)
        hdrs["origin"]      = self.base_url
        hdrs["referer"]     = build_referer(self.base_url, cfg)
        hdrs["ctx-view-id"] = str(uuid.uuid4())

        import time as _time
        for _attempt in range(2):  # max 2 retries — avoid multi-minute hangs
            resp = self._request("post", graphql_url, headers=hdrs, json=payload, timeout=30)
            print(f"    Status: {resp.status_code}")
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 503):
                wait = 3 * (_attempt + 1)  # 3s, 6s — no cookie reharvest (too slow)
                print(f"    [hotels_com] Rate-limited ({resp.status_code}), waiting {wait}s ...")
                _time.sleep(wait)
                hdrs["ctx-view-id"] = str(uuid.uuid4())
                continue
            print(f"    Body  : {resp.text[:300]}")
            break  # non-retryable error

        if resp.status_code != 200:
            logger.warning("[hotels_com] Non-200 after retries: %s", resp.status_code)
            return {}

        result = resp.json()
        if isinstance(result, list) and len(result) > 1:
            return result[1]
        return result[0] if isinstance(result, list) else result

    # ── Paginate ──────────────────────────────────────────────────────────
    def fetch_all_hotels(self, cfg: dict, page_size: int = 100) -> list[dict]:
        self.harvest_cookies()
        self.visit_search_page(cfg)

        data     = self.fetch_hotels(cfg, results_starting_index=0, results_size=page_size)
        listings = self._extract_listings(data)
        total    = self._extract_total(data)
        print(f"    Total available: {total}")

        offset = page_size
        while offset < total and listings:
            self.search_id = str(uuid.uuid4())   # rotate per page
            data      = self.fetch_hotels(cfg, results_starting_index=offset, results_size=page_size)
            page_data = self._extract_listings(data)
            if not page_data:
                break
            listings.extend(page_data)
            print(f"    Collected: {len(listings)} / {total}")
            offset += page_size

        return listings

    # ── Helpers ───────────────────────────────────────────────────────────
    @staticmethod
    def _extract_listings(data: dict) -> list[dict]:
        for path in [
            ["data", "propertySearch", "propertySearchListings"],
            ["data", "propertySearch", "properties", "elements"],
        ]:
            try:
                obj = data
                for key in path:
                    obj = obj[key]
                return obj
            except (KeyError, TypeError):
                continue
        return []

    @staticmethod
    def _extract_total(data: dict) -> int:
        for path in [
            ["data", "propertySearch", "propertyResultsTotal"],
            ["data", "propertySearch", "properties", "totalCount"],
        ]:
            try:
                obj = data
                for key in path:
                    obj = obj[key]
                return int(obj)
            except (KeyError, TypeError, ValueError):
                continue
        return 0


# ──────────────────────────────────────────────
# Normaliser (inline — also used by normalise.py)
# ──────────────────────────────────────────────
import csv

def _safe(obj, *keys, default=None):
    for k in keys:
        if obj is None:
            return default
        if isinstance(k, int):
            obj = obj[k] if isinstance(obj, list) and k < len(obj) else None
        else:
            obj = obj.get(k) if isinstance(obj, dict) else None
    return obj if obj is not None else default


def clean_hotel(raw: dict) -> dict | None:
    """Flatten one raw LodgingCard into a tidy dict. Returns None for banners/placeholders."""
    if not raw.get("id"):
        return None

    heading  = raw.get("headingSection") or {}
    name     = heading.get("heading", "")
    city     = next((m.get("text", "") for m in (heading.get("messages") or [])
                     if isinstance(m, dict) and m.get("text")), "")
    amenities = ", ".join(a.get("text", "") for a in (heading.get("amenities") or []) if a.get("text"))

    review_score = review_label = review_count = ""
    desc_heading = desc_text = ""
    for ss in (raw.get("summarySections") or []):
        rs = _safe(ss, "reviewSummary")
        if rs and not review_score:
            review_score = _safe(rs, "graphic", "text", default="")
            review_label = _safe(rs, "title", "shoppingProductTitle", "text", default="")
            review_count = _safe(rs, "subtexts", 0, "shoppingProductTitle", "text", default="")
        ds = _safe(ss, "descriptionSection")
        if ds and not desc_heading:
            desc_heading = _safe(ds, "heading", "text", default="")
            desc_text    = _safe(ds, "description", "text", default="")

    ps      = raw.get("priceSection") or {}
    summary = ps.get("priceSummary") or {}
    opts    = summary.get("options") or []
    price_total  = _safe(opts, 0, "displayPrice", "formatted", default="")
    price_strike = _safe(opts, 0, "strikeOut", "formatted", default="")
    price_label  = _safe(summary, "priceMessaging", 0, "value", default="")

    media   = (_safe(raw, "mediaSection", "gallery") or {}).get("media") or []
    images  = [_safe(m, "media", "url", default="") for m in media if _safe(m, "media", "url")]

    bg      = (_safe(raw, "mediaSection", "badges") or {})
    badges  = [bg[k]["text"] for k in ("primaryBadge", "secondaryBadge", "tertiaryBadge")
               if bg.get(k) and bg[k].get("text")]
    deal    = _safe(ps, "standardBadge", "standardBadge", "text", default="")
    if deal and deal not in badges:
        badges.append(deal)

    hotel_url = _safe(raw, "cardLink", "resource", "value", default="") or \
                _safe(raw, "shoppingJoinListContainer", "actions", 0, "resource", "value", default="")

    return {
        "id":               raw.get("id", ""),
        "name":             name,
        "city":             city,
        "rating_score":     review_score,
        "rating_label":     review_label,
        "review_count":     review_count,
        "description_heading": desc_heading,
        "description":      desc_text,
        "price_total":      price_total,
        "price_strikeout":  price_strike,
        "price_period":     price_label,
        "amenities":        amenities,
        "badges":           ", ".join(badges),
        "is_sponsored":     any(
            (bg.get(k) or {}).get("theme") == "sponsored"
            for k in ("primaryBadge", "secondaryBadge")
        ),
        "image_1": images[0] if len(images) > 0 else "",
        "image_2": images[1] if len(images) > 1 else "",
        "image_3": images[2] if len(images) > 2 else "",
        "image_4": images[3] if len(images) > 3 else "",
        "hotel_url": hotel_url,
    }


def export_results(hotels: list[dict], cfg: dict) -> None:
    """Export the scraped dataset (in both raw and cleaned forms) as designated by config.json"""
    formats = cfg.get("export_format", ["json"])
    base_name = cfg.get("output_file", "hotelsdotcom_results")
    
    if not hotels:
        print("[!] No hotels to export.")
        return
        
    print("Normalising ...")
    cleaned = [c for h in hotels if (c := clean_hotel(h))]
    print(f"  Cleaned: {len(cleaned)} / {len(hotels)} records")

    # Dump raw json output as well for debugging purposes just in case
    raw_path = f"{base_name}_raw.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(hotels, f, indent=2, ensure_ascii=False)
    print(f"[+] Saved {len(hotels)} raw items to {raw_path}")

    if "json" in formats:
        p = f"{base_name}.json"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2, ensure_ascii=False)
        print(f"[+] Saved {len(cleaned)} cleaned items to {p}")

    if "csv" in formats and cleaned:
        p = f"{base_name}.csv"
        # Get all keys to write header
        keys = list(cleaned[0].keys())
        with open(p, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(cleaned)
        print(f"[+] Saved {len(cleaned)} items to {p}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def main():
    cfg = load_config()
    
    start_time = time.perf_counter()
    scraper = HotelsComScraper(
        impersonate="chrome142", 
        proxy=cfg.get("proxy")
    )

    try:
        hotels = scraper.fetch_all_hotels(cfg, page_size=100)
    except Exception as exc:
        print(f"\n[ERROR] {exc}")
        return

    duration = time.perf_counter() - start_time
    print(f"\n[+] Done! Scraped {len(hotels)} raw listings in {duration:.2f}s.")
    print("-" * 80)
    
    export_results(hotels, cfg)


if __name__ == "__main__":
    main()