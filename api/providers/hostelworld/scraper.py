"""
Hostelworld Hotels — Playwright-based hostel/hotel search.

Hostelworld renders property cards with class 'property-card'.
Prices in '.from-price-value' elements (format: 'US$31.14').
Names in property card headings.
Works WITHOUT proxy (DC proxy causes tunnel failures).
"""

from __future__ import annotations
import asyncio, json, logging, re
from datetime import datetime
logger = logging.getLogger(__name__)

_BLOCKED_TYPES = {"media", "font"}
_BLOCKED_URL_PATTERNS = ["google-analytics","doubleclick","facebook","hotjar","sentry",".woff",".woff2",".ttf"]


class HostelworldScraper:
    BASE_URL = "https://www.hostelworld.com"

    def build_search_url(self, city, check_in, check_out, adults=2, rooms=1, currency="USD"):
        city_slug = city.lower().replace(" ", "-")
        return f"{self.BASE_URL}/st/hostels/{city_slug}/?DateRange={check_in},{check_out}&NumberOfGuests={adults}"

    def search(self, city, check_in, check_out, adults=2, rooms=1, currency="USD",
               proxy=None, proxy_manager=None):
        from core.proxy_helpers import build_proxy_list, is_proxy_failure
        proxy_configs = build_proxy_list(proxy_manager, proxy)

        for proxy_name, proxy_config in proxy_configs:
            logger.info("[hostelworld] Trying proxy: %s", proxy_name)
            try:
                result = self._run_async(
                    self._async_search(city, check_in, check_out, adults,
                                       rooms, currency, proxy_config))
                if result:
                    logger.info("[hostelworld] OK %d hotels via %s",
                                len(result), proxy_name)
                    return result
            except Exception as e:
                if is_proxy_failure(str(e)):
                    logger.warning("[hostelworld] %s failed: %s -> next",
                                   proxy_name, str(e)[:60])
                    continue
                raise
        return []

    @staticmethod
    def _run_async(coro):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    return pool.submit(asyncio.run, coro).result()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)

    async def _async_search(self, city, check_in, check_out, adults, rooms, currency, proxy):
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("[hostelworld] playwright not installed"); return []

        url = self.build_search_url(city, check_in, check_out, adults, rooms, currency)
        logger.info("[hostelworld] Opening %s", url)

        try:
            async with async_playwright() as pw:
                proxy_config = (proxy if isinstance(proxy, dict) else {"server": proxy} if isinstance(proxy, str) and proxy else None)
                browser = await pw.chromium.launch(headless=True,
                    args=["--no-sandbox","--disable-blink-features=AutomationControlled"],
                    proxy=proxy_config)
                ctx = await browser.new_context(viewport={"width":1366,"height":900},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                    locale="en-US")
                page = await ctx.new_page()
                await page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>false});window.chrome={runtime:{}};")
                
                async def block(route):
                    if route.request.resource_type in _BLOCKED_TYPES or any(p in route.request.url.lower() for p in _BLOCKED_URL_PATTERNS):
                        await route.abort()
                    else: await route.continue_()
                await page.route("**/*", block)

                try: await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception as e: logger.warning("[hostelworld] Nav: %s", e)

                # Dismiss cookie popup
                try:
                    btn = page.locator("button:has-text('Accept'), button:has-text('Got it'), #onetrust-accept-btn-handler")
                    if await btn.count() > 0: await btn.first.click()
                except Exception: pass

                # Wait for property cards to load
                try: await page.wait_for_selector('[class*="property-card"]', timeout=12000)
                except Exception: await asyncio.sleep(5)

                # Scroll to load more
                for _ in range(5):
                    try: await page.evaluate("window.scrollBy(0, 1500)"); await asyncio.sleep(1.5)
                    except Exception: break

                hotels = await self._parse_dom(page, city, check_in, check_out, currency)
                await browser.close()
        except Exception as e:
            logger.error("[hostelworld] Error: %s", e); return []

        hotels.sort(key=lambda x: x.get("price", 9999))
        logger.info("[hostelworld] %d hotels parsed", len(hotels))
        return hotels

    async def _parse_dom(self, page, city, check_in, check_out, currency):
        hotels = []
        try:
            cin = datetime.strptime(check_in, "%Y-%m-%d"); cout = datetime.strptime(check_out, "%Y-%m-%d")
            nights = max(1, (cout - cin).days)
        except Exception: nights = 1

        try:
            items = await page.evaluate("""
                () => {
                    const hotels = []; const seen = new Set();
                    
                    // Hostelworld uses .property-card class
                    document.querySelectorAll('[class*="property-card"]').forEach(card => {
                        try {
                            // Name from heading or link text
                            const nameEl = card.querySelector('h2, h3, [class*="property-name"], a[href*="/hostel"] span, a[href*="/hostel"]');
                            let name = nameEl ? nameEl.textContent.trim() : '';
                            
                            // Fallback: get from aria-label
                            if (!name) {
                                const ariaEl = card.querySelector('[aria-label]');
                                if (ariaEl) name = ariaEl.getAttribute('aria-label');
                            }
                            
                            if (!name || name.length < 3 || seen.has(name.toLowerCase())) return;
                            seen.add(name.toLowerCase());
                            
                            // Price from .from-price-value (format: US$31.14)
                            const priceEl = card.querySelector('.from-price-value, [class*="from-price-value"]');
                            let priceText = priceEl ? priceEl.textContent.trim() : '';
                            
                            // Fallback: .property-accommodation-from-price (format: FromUS$31.14)
                            if (!priceText) {
                                const pEl2 = card.querySelector('.property-accommodation-from-price, [class*="from-price"]');
                                if (pEl2) priceText = pEl2.textContent.trim();
                            }
                            
                            // Rating
                            const ratingEl = card.querySelector('[class*="rating-score"], [class*="score"]');
                            const rating = ratingEl ? ratingEl.textContent.trim() : '';
                            
                            // Image
                            const imgEl = card.querySelector('img');
                            
                            // Link
                            const linkEl = card.querySelector('a[href*="/hostel"]');
                            
                            hotels.push({
                                name,
                                priceText,
                                rating,
                                image: imgEl ? (imgEl.src || '') : '',
                                link: linkEl ? linkEl.href : '',
                            });
                        } catch(e) {}
                    });
                    return hotels;
                }
            """)

            for item in (items or []):
                try:
                    name = str(item.get("name","")).strip()
                    if not name or len(name) < 3: continue
                    
                    price = self._parse_price(item.get("priceText",""))
                    if price <= 0: continue
                    
                    rating = 0.0
                    r_match = re.search(r'(\d+\.?\d*)', str(item.get("rating","")))
                    if r_match: rating = float(r_match.group(1))
                    if rating > 10: rating = rating / 10  # Normalize if > 10
                    
                    # Hostelworld prices are per night
                    ppn = price
                    total_price = price * nights
                    
                    hotels.append({
                        "hotel_name": name, "hotel_address": "", "city": city,
                        "latitude": 0.0, "longitude": 0.0, "star_rating": 0,
                        "guest_rating": round(rating,1), "review_count": 0,
                        "check_in": check_in, "check_out": check_out, "nights": nights,
                        "room_type": "Dorm", "board_type": "",
                        "price": round(total_price,2), "price_per_night": round(ppn,2),
                        "currency": currency, "booking_provider": "Hostelworld",
                        "cancellation": "", "amenities": "",
                        "deep_link": str(item.get("link","")),
                        "image_url": str(item.get("image","")),
                    })
                except Exception: continue
        except Exception as e:
            logger.debug("[hostelworld] DOM parse error: %s", e)
        return hotels

    @staticmethod
    def _parse_price(text):
        """Parse price from text like 'US$31.14' or 'FromUS$19.07'."""
        if not text: return 0.0
        # Match currency symbol followed by amount
        match = re.search(r'(?:US)?\$\s*([\d,.]+)', str(text))
        if match:
            try: return float(match.group(1).replace(",",""))
            except ValueError: pass
        # Fallback: any number with decimal
        match = re.search(r'([\d]+\.[\d]+)', str(text))
        if match:
            try: return float(match.group(1))
            except ValueError: pass
        return 0.0
"""
Hostelworld API Client
======================
Direct REST API access to Hostelworld's property endpoint.
No browser/Playwright required — uses a static API key.

Entirely config-driven via config.json:
  - api.*       → API key, URL, timeouts, result limits
  - search.*    → city, dates, guests, currency
  - output.*    → whether/where to save files (off by default)
  - display.*   → console printing options

Typical usage:
    cfg = HostelworldAPIClient.load_config()
    client = HostelworldAPIClient(cfg)
    properties, meta = client.search()

    # or override any search param:
    properties, meta = client.search(city_id=202, checkin="2026-04-01", nights=3)
"""



import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Property type labels
# ─────────────────────────────────────────────────────────────────────────────
PROPERTY_TYPES = {
    1:  "Hostel",
    2:  "Hotel",
    3:  "Guesthouse / B&B",
    4:  "Apartment",
    5:  "Campsite",
    6:  "Lodge",
    7:  "Inn",
    8:  "Boat / Ship",
    9:  "Haus / Chalet",
    10: "Homestay",
    11: "Farm Stay",
    12: "Villa",
}

# Common city IDs for quick reference
COMMON_CITIES = {
    53:  ("Dublin",     "Ireland"),
    57:  ("Amsterdam",  "Netherlands"),
    66:  ("Barcelona",  "Spain"),
    72:  ("Berlin",     "Germany"),
    88:  ("Budapest",   "Hungary"),
    94:  ("Cape Town",  "South Africa"),
    134: ("Karachi",    "Pakistan"),
    135: ("Lahore",     "Pakistan"),
    202: ("London",     "UK"),
    247: ("Melbourne",  "Australia"),
    262: ("Milan",      "Italy"),
    302: ("New York",   "USA"),
    323: ("Paris",      "France"),
    343: ("Prague",     "Czech Republic"),
    406: ("Rome",       "Italy"),
    456: ("Sydney",     "Australia"),
    467: ("Tokyo",      "Japan"),
    499: ("Vienna",     "Austria"),
}


# ─────────────────────────────────────────────────────────────────────────────
#  Client
# ─────────────────────────────────────────────────────────────────────────────

class HostelworldAPIClient:
    """
    REST API wrapper for Hostelworld's property search endpoint.

    All defaults come from config.json; every parameter can be overridden
    when calling ``search()``.
    """

    _DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

    # ── Construction ─────────────────────────────────────────────────────────

    def __init__(self, config: dict | None = None):
        """
        Args:
            config: Merged config dict (from load_config). If None, loads
                    config.json from the provider directory automatically.
        """
        if config is None:
            config = self.load_config()
        self._cfg = config

        api_cfg = config.get("api", {})
        self._base_url = api_cfg.get("base_url",
            "https://prod.apigee.hostelworld.com/legacy-hwapi-service/2.2")
        self._api_key  = api_cfg.get("key", "")
        self._timeout  = api_cfg.get("timeout", 30)
        self._per_page = api_cfg.get("per_page", 1000)
        self._show_rooms  = api_cfg.get("show_rooms", 1)
        self._num_images  = api_cfg.get("num_images", 3)

        self._user_id = str(uuid.uuid4())
        self._session = self._build_session()

    # ── Config helpers ────────────────────────────────────────────────────────

    @classmethod
    def load_config(cls, path: str | None = None) -> dict:
        """Load and return config.json as a dict."""
        cfg_path = path or cls._DEFAULT_CONFIG_PATH
        if not os.path.exists(cfg_path):
            logger.warning("[hostelworld] config.json not found at %s — using defaults", cfg_path)
            return {}
        with open(cfg_path, encoding="utf-8") as f:
            return json.load(f)

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "accept":             "application/json, text/plain, */*",
            "accept-language":    "en",
            "api-key":            self._api_key,
            "origin":             "https://www.hostelworld.com",
            "referer":            "https://www.hostelworld.com/",
            "priority":           "u=1, i",
            "sec-ch-ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest":     "empty",
            "sec-fetch-mode":     "cors",
            "sec-fetch-site":     "same-site",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        })
        return s

    # ── City lookup ───────────────────────────────────────────────────────────

    def find_city_id(self, query: str) -> Optional[int]:
        """Search for a city by name and return its Hostelworld ID."""
        query_lower = query.strip().lower()
        for cid, (name, country) in COMMON_CITIES.items():
            if query_lower == name.lower():
                logger.info(
                    "[hostelworld] Found city: %s (ID: %s) — %s",
                    name, cid, country,
                )
                return cid
        logger.warning("[hostelworld] City '%s' not found in known cities", query)
        return None

    # ── Core API call ─────────────────────────────────────────────────────────

    def fetch_raw(
        self,
        city_id:  int,
        checkin:  str,
        nights:   int,
        guests:   int = 2,
        currency: str = "USD",
        per_page: Optional[int] = None,
    ) -> Optional[dict]:
        """
        Hit the Hostelworld /cities/{id}/properties/ endpoint.

        Returns the raw JSON dict, or None on failure.
        """
        url = f"{self._base_url}/cities/{city_id}/properties/"
        params = {
            "currency":            currency,
            "application":         "web",
            "user-id":             self._user_id,
            "date-start":          checkin,
            "num-nights":          nights,
            "guests":              guests,
            "per-page":            per_page or self._per_page,
            "show-rooms":          self._show_rooms,
            "property-num-images": self._num_images,
            "v":                   "control",
        }

        logger.info("[hostelworld] GET %s | city=%s checkin=%s nights=%s guests=%s currency=%s",
                    url, city_id, checkin, nights, guests, currency)

        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:
            logger.error("[hostelworld] Request failed: %s", exc)
            return None

        if resp.status_code == 401:
            logger.error(
                "[hostelworld] 401 Unauthorized — the API key has expired. "
                "Grab a fresh one from DevTools → Network → any /properties/ request → api-key header."
            )
            return None
        if resp.status_code == 404:
            logger.error("[hostelworld] 404 — city ID %s not found.", city_id)
            return None

        try:
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error("[hostelworld] Response error: %s", exc)
            return None

    # ── Parsers ───────────────────────────────────────────────────────────────

    def _parse_room(self, room: dict) -> dict:
        pb = room.get("priceBreakdown", {}).get("bestPrice", {}).get("perNight", {})
        return {
            "id":               room.get("id"),
            "name":             room.get("name", ""),
            "type":             room.get("roomType", ""),
            "is_private":       room.get("isPrivate", False),
            "beds":             room.get("beds"),
            "mixed_dorm":       room.get("isMixedDorm"),
            "min_price":        pb.get("value"),
            "currency":         pb.get("currency"),
            "formatted_price":  pb.get("formattedValue"),
            "availability":     room.get("availability"),
            "free_cancellation": room.get("freeCancellation", False),
        }

    def _parse_property(self, prop: dict, rank: int, cfg_display: dict) -> dict:
        max_fac    = cfg_display.get("max_facilities", 10)
        max_imgs   = cfg_display.get("max_images", 3)
        max_rooms  = cfg_display.get("max_rooms_per_prop", 5)

        # Basic
        name      = prop.get("name", "Unknown")
        prop_id   = prop.get("id", "")
        prop_type = PROPERTY_TYPES.get(prop.get("type"), prop.get("typeName", "Unknown"))

        # Rating
        rating    = prop.get("rating") or {}
        score     = rating.get("overall") or rating.get("score") or "N/A"
        num_rev   = rating.get("numberOfRatings") or rating.get("reviewsCount") or 0
        score_cat = ""
        if isinstance(score, dict):
            score_cat = score.get("category", "")
            score     = score.get("score", "N/A")

        # Price
        pb        = prop.get("lowestPricePerNight") or prop.get("price") or {}
        price_val = pb.get("value")
        price_fmt = pb.get("formattedValue") or (
            f"{pb.get('currency', '')} {price_val}" if price_val else "N/A"
        )
        currency  = pb.get("currency", "")

        # Location
        address1  = prop.get("address1", "")
        address2  = prop.get("address2", "")
        city_info = prop.get("city") or {}
        city_nm   = city_info.get("name", "") if isinstance(city_info, dict) else ""
        country   = (prop.get("country") or {}).get("name", "")
        full_addr = ", ".join(filter(None, [address1, address2, city_nm, country]))
        lat       = prop.get("latitude", "") or prop.get("lat", "")
        lon       = prop.get("longitude", "") or prop.get("lng", "")

        # Facilities
        facilities = [
            f.get("name", "") for f in (prop.get("facilities") or [])
            if isinstance(f, dict) and f.get("name")
        ][:max_fac]

        # Images
        images = [
            img.get("prefix", "") + img.get("suffix", "")
            for img in (prop.get("images") or [])[:max_imgs]
            if isinstance(img, dict) and img.get("prefix")
        ]

        # Rooms — API may return a list OR a dict keyed by room ID
        rooms_raw = prop.get("rooms") or []
        if isinstance(rooms_raw, dict):
            rooms_raw = list(rooms_raw.values())
        rooms = [
            self._parse_room(r)
            for r in rooms_raw[:max_rooms]
            if isinstance(r, dict)
        ]

        # URL
        url_path = prop.get("urlFriendlyName") or prop.get("urlFriendly", "")
        hw_url   = (
            f"https://www.hostelworld.com/pwa/hosteldetails.php/{url_path}/{prop_id}"
            if url_path else ""
        )

        return {
            "rank":              rank,
            "id":                prop_id,
            "name":              name,
            "type":              prop_type,
            "guest_score":       score,
            "score_category":    score_cat,
            "review_count":      num_rev,
            "lowest_price":      price_fmt,
            "price_value":       price_val,
            "currency":          currency,
            "address":           full_addr,
            "city":              city_nm,
            "country":           country,
            "latitude":          lat,
            "longitude":         lon,
            "facilities":        facilities,
            "rooms":             rooms,
            "images":            images,
            "url":               hw_url,
            "is_featured":       prop.get("isFeatured", False),
            "free_cancellation": prop.get("freeCancellation", False),
        }

    def parse_results(self, raw: dict) -> tuple[list, dict]:
        """
        Parse raw API response into (properties_list, meta_dict).

        Returns:
            properties: list of dicts, one per property
            meta:       pagination / city info
        """
        cfg_display = self._cfg.get("display", {})
        props_raw   = raw.get("properties") or []
        city_info   = raw.get("city") or {}
        paging      = raw.get("pagination") or {}

        meta = {
            "total":    paging.get("total", len(props_raw)),
            "page":     paging.get("page", 1),
            "per_page": paging.get("perPage", len(props_raw)),
            "city":     city_info.get("name", ""),
            "country":  city_info.get("countryName", ""),
        }
        properties = [
            self._parse_property(p, i + 1, cfg_display)
            for i, p in enumerate(props_raw)
        ]
        return properties, meta

    # ── Output helpers ────────────────────────────────────────────────────────

    def _make_filename(self, prefix: str, meta: dict, checkin: str, suffix: str = "") -> str:
        city_slug = re.sub(r"[^\w]", "_", meta.get("city", "unknown"))[:25]
        ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
        name      = f"{prefix}_{city_slug}_{checkin}_{ts}"
        if suffix:
            name += f"_{suffix}"
        return name + ".json"

    def save_results(
        self,
        properties: list,
        meta:       dict,
        checkin:    str,
        nights:     int,
        raw:        dict | None = None,
    ) -> list[str]:
        """
        Save parsed results (and optionally raw JSON) according to config.
        Returns list of file paths written.

        Called automatically by ``search()`` when output.save_results is true.
        """
        out_cfg  = self._cfg.get("output", {})
        out_dir  = out_cfg.get("output_dir", ".")
        prefix   = out_cfg.get("filename_prefix", "hostelworld")
        save_raw = out_cfg.get("save_raw", False)

        os.makedirs(out_dir, exist_ok=True)
        written  = []

        # Parsed results
        fname = os.path.join(out_dir, self._make_filename(prefix, meta, checkin))
        payload = {
            "meta":       meta,
            "checkin":    checkin,
            "nights":     nights,
            "scraped_at": datetime.now().isoformat(),
            "count":      len(properties),
            "properties": properties,
        }
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        logger.info("[hostelworld] Results saved → %s", fname)
        written.append(fname)

        # Raw response (optional)
        if save_raw and raw is not None:
            raw_fname = os.path.join(
                out_dir, self._make_filename(prefix, meta, checkin, "RAW")
            )
            with open(raw_fname, "w", encoding="utf-8") as f:
                json.dump(raw, f, indent=2, ensure_ascii=False)
            logger.info("[hostelworld] Raw JSON saved → %s", raw_fname)
            written.append(raw_fname)

        return written

    def print_results(self, properties: list, meta: dict, checkin: str, nights: int):
        """Print a human-readable summary to stdout."""
        checkout = (
            datetime.strptime(checkin, "%Y-%m-%d") + timedelta(days=nights)
        ).strftime("%Y-%m-%d")

        print("\n" + "═" * 72)
        print("  HOSTELWORLD RESULTS")
        loc = meta["city"]
        if meta.get("country"):
            loc += f", {meta['country']}"
        print(f"  Location : {loc}")
        print(f"  Dates    : {checkin}  →  {checkout}  ({nights} night{'s' if nights != 1 else ''})")
        print(f"  Found    : {len(properties)} of {meta['total']} properties")
        print("═" * 72)

        if not properties:
            print("\n  No properties found.\n")
            return

        for p in properties:
            cancel = "  [Free cancellation]" if p["free_cancellation"] else ""
            feat   = "  [Featured]"          if p["is_featured"]       else ""
            print(f"\n  [{p['rank']:>3}]  {p['name']}  ({p['type']}){feat}")
            print(f"         Score    : {p['guest_score']}  {p['score_category']}  ({p['review_count']} reviews)")
            print(f"         Price    : {p['lowest_price']}/night{cancel}")
            if p["address"]:
                print(f"         Address  : {p['address']}")
            if p["latitude"]:
                print(f"         Coords   : {p['latitude']}, {p['longitude']}")
            if p["facilities"]:
                print(f"         Amenities: {', '.join(p['facilities'])}")
            cfg_rooms = self._cfg.get("display", {}).get("max_rooms_per_prop", 5)
            if p["rooms"]:
                print(f"         Rooms    :")
                for r in p["rooms"][:cfg_rooms]:
                    kind  = "Private" if r["is_private"] else "Dorm"
                    price = r.get("formatted_price") or (
                        f"{r['currency']} {r['min_price']}" if r["min_price"] else "N/A"
                    )
                    canc  = " [Free cancel]" if r.get("free_cancellation") else ""
                    print(f"                   • {r['name']} ({kind}) — {price}/night{canc}")
            if p["url"]:
                print(f"         URL      : {p['url']}")

        print("\n" + "═" * 72 + "\n")

    # ── High-level search ─────────────────────────────────────────────────────

    def search(
        self,
        city_id:   Optional[int] = None,
        city_name: Optional[str] = None,
        checkin:   Optional[str] = None,
        nights:    Optional[int] = None,
        guests:    Optional[int] = None,
        currency:  Optional[str] = None,
    ) -> tuple[list, dict]:
        """
        Run a full search: fetch → parse → optionally print + save.

        Parameters override config.json values when provided.
        If city_name is given it takes precedence over city_id
        (looks up the ID from the API first).

        Returns:
            (properties, meta)  — properties is a list of dicts.
        """
        s_cfg = self._cfg.get("search", {})
        o_cfg = self._cfg.get("output", {})
        d_cfg = self._cfg.get("display", {})

        # Resolve params (arg > config)
        eff_city_id   = city_id   or s_cfg.get("city_id", 53)
        eff_checkin   = checkin   or s_cfg.get("checkin",
            (datetime.today() + timedelta(days=7)).strftime("%Y-%m-%d"))
        eff_nights    = nights    if nights is not None  else s_cfg.get("nights", 3)
        eff_guests    = guests    if guests is not None  else s_cfg.get("guests", 2)
        eff_currency  = currency  or s_cfg.get("currency", "USD")

        # City name lookup overrides city_id
        eff_city_name = city_name or s_cfg.get("city_name", "")
        if eff_city_name:
            found = self.find_city_id(eff_city_name)
            if found:
                eff_city_id = found
            else:
                logger.warning(
                    "[hostelworld] City %r not found via API, falling back to city_id=%s",
                    eff_city_name, eff_city_id,
                )

        # Fetch
        raw = self.fetch_raw(
            city_id  = eff_city_id,
            checkin  = eff_checkin,
            nights   = eff_nights,
            guests   = eff_guests,
            currency = eff_currency,
        )
        if raw is None:
            logger.error("[hostelworld] No data returned.")
            return [], {}

        # Parse
        properties, meta = self.parse_results(raw)
        logger.info("[hostelworld] Parsed %d properties", len(properties))

        # Display
        if d_cfg.get("print_results", True):
            self.print_results(properties, meta, eff_checkin, eff_nights)

        # Save (only if explicitly enabled in config)
        if o_cfg.get("save_results", False):
            saved = self.save_results(properties, meta, eff_checkin, eff_nights, raw)
            for path in saved:
                print(f"  Saved → {path}")

        return properties, meta


# ─────────────────────────────────────────────────────────────────────────────
#  Standalone runner (python api_client.py)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s  %(name)s  %(message)s",
    )

    client = HostelworldAPIClient()
    properties, meta = client.search()

    if not properties:
        print("No results. Check city_id / checkin in config.json or verify the API key.")