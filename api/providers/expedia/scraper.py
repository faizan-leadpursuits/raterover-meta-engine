"""
Expedia hotel scraper & adapter — uses the same Expedia Group GraphQL engine as Hotels.com.
Scrapes hotel listings via curl_cffi with chrome impersonation + residential proxy.

Flow:
  1. GET expedia.com/ → harvest cookies
  2. GET Hotel-Search page → get search context (duaid, searchId, etc.)
  3. POST GraphQL → fetch hotel listings
"""

import re
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, quote_plus

from curl_cffi.requests import Session

logger = logging.getLogger(__name__)

BASE_URL = "https://www.expedia.com"
GRAPHQL_URL = f"{BASE_URL}/graphql"

HEADERS_BROWSE = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "upgrade-insecure-requests": "1",
}

HEADERS_API = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "client-info": "shopping-pwa",
    "x-page-id": "page.Hotels.Infosite.Information",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}


class ExpediaScraper:
    """Scrape Expedia hotel search results via HTML parsing."""

    def __init__(self, proxy: str = None):
        self.session = Session(impersonate="chrome131")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def search(self, city: str, check_in: str, check_out: str,
               adults: int = 2, rooms: int = 1, currency: str = "USD") -> list[dict]:
        """Search Expedia and return a list of hotel dicts."""

        # Step 1: Harvest cookies
        logger.info("[expedia] Step 1: Harvesting cookies...")
        try:
            self.session.get(BASE_URL + "/", headers=HEADERS_BROWSE, timeout=15)
        except Exception as e:
            logger.warning("[expedia] Cookie harvest failed: %s", e)

        # Step 2: Search page
        logger.info("[expedia] Step 2: Loading search for '%s'...", city)
        params = {
            "destination": city,
            "startDate": check_in,
            "endDate": check_out,
            "adults": adults,
            "rooms": rooms,
        }
        search_url = f"{BASE_URL}/Hotel-Search?{urlencode(params)}"

        try:
            resp = self.session.get(search_url, headers=HEADERS_BROWSE,
                                   timeout=25, allow_redirects=True)
        except Exception as e:
            logger.error("[expedia] Search page failed: %s", e)
            return []

        if resp.status_code != 200:
            logger.warning("[expedia] Search returned %d", resp.status_code)
            return []

        html = resp.text or ""
        logger.info("[expedia] Search page loaded (%d chars)", len(html))

        # Step 3: Parse hotels from embedded JSON in the page
        hotels = self._parse_from_html(html, city, check_in, check_out, currency)
        logger.info("[expedia] Parsed %d hotels", len(hotels))
        return hotels

    def _parse_from_html(self, html: str, city: str, check_in: str,
                         check_out: str, currency: str) -> list[dict]:
        """Extract hotel data from Expedia's server-rendered HTML/JSON."""
        hotels = []

        # Try to find __NEXT_DATA__ or Apollo state in the page
        json_patterns = [
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            r'window\.__APOLLO_STATE__\s*=\s*({.*?});',
            r'"lodgingSearchResults"\s*:\s*(\{.*?\})\s*,\s*"',
        ]

        for pattern in json_patterns:
            m = re.search(pattern, html, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    parsed = self._extract_from_next_data(data, city, check_in, check_out, currency)
                    if parsed:
                        return parsed
                except json.JSONDecodeError:
                    continue

        # Fallback: regex parse hotel cards from HTML
        return self._extract_from_html_cards(html, city, check_in, check_out, currency)

    def _extract_from_next_data(self, data: dict, city: str, check_in: str,
                                check_out: str, currency: str) -> list[dict]:
        """Extract hotels from __NEXT_DATA__ JSON."""
        hotels = []

        # Navigate the nested structure
        # Try props > pageProps path
        props = data.get("props", {}).get("pageProps", data)

        # Look for property listings in various possible paths
        listings = []

        def find_listings(obj, depth=0):
            if depth > 8 or not isinstance(obj, (dict, list)):
                return
            if isinstance(obj, dict):
                # Check for common listing array keys
                for key in ["properties", "propertyResults", "listings",
                           "lodgingCards", "searchResults", "results"]:
                    val = obj.get(key)
                    if isinstance(val, list) and len(val) > 0:
                        listings.extend(val)
                        return
                for v in obj.values():
                    find_listings(v, depth + 1)
            elif isinstance(obj, list):
                for item in obj[:5]:  # Don't go too deep
                    find_listings(item, depth + 1)

        find_listings(props)

        for item in listings:
            if not isinstance(item, dict):
                continue
            hotel = self._normalize_listing(item, city, check_in, check_out, currency)
            if hotel and hotel.get("name"):
                hotels.append(hotel)

        return hotels

    def _normalize_listing(self, item: dict, city: str, check_in: str,
                          check_out: str, currency: str) -> dict | None:
        """Normalize a single hotel listing from Expedia's data."""
        try:
            # Try multiple possible paths for hotel name
            name = (item.get("name") or
                   item.get("hotelName") or
                   item.get("propertyName") or
                   _deep_get(item, "header", "text") or
                   _deep_get(item, "cardLink", "accessibilityLabel") or "")

            if not name or len(name) < 2:
                return None

            # Price
            price = 0.0
            price_info = item.get("price") or item.get("offerSummary") or item.get("priceSection") or {}
            if isinstance(price_info, dict):
                price = (
                    _to_float(_deep_get(price_info, "lead", "amount")) or
                    _to_float(_deep_get(price_info, "displayPrice", "amount")) or
                    _to_float(price_info.get("amount")) or
                    _to_float(price_info.get("price")) or 0.0
                )
            elif isinstance(price_info, (int, float)):
                price = float(price_info)

            # Star rating
            stars = _to_int(item.get("star") or
                          _deep_get(item, "starRating", "value") or
                          item.get("starRating") or 0)

            # Guest rating
            rating = _to_float(_deep_get(item, "reviews", "score") or
                              _deep_get(item, "guestReviews", "score") or
                              item.get("guestRating") or 0)

            # Review count
            reviews = _to_int(_deep_get(item, "reviews", "total") or
                            _deep_get(item, "guestReviews", "count") or
                            item.get("reviewCount") or 0)

            # Image
            image = (_deep_get(item, "image", "url") or
                    _deep_get(item, "propertyImage", "url") or
                    _deep_get(item, "thumbnail", "url") or "")

            # Deep link
            link = (item.get("detailLink") or
                   _deep_get(item, "cardLink", "url") or
                   item.get("url") or "")
            if link and not link.startswith("http"):
                link = f"{BASE_URL}{link}"

            # Coordinates
            lat = _to_float(_deep_get(item, "coordinate", "lat") or
                          _deep_get(item, "location", "latitude") or 0)
            lng = _to_float(_deep_get(item, "coordinate", "lon") or
                          _deep_get(item, "location", "longitude") or 0)

            return {
                "name": name.strip(),
                "price": price,
                "star_rating": min(stars, 5),
                "guest_rating": rating,
                "review_count": reviews,
                "image_url": image,
                "deep_link": link,
                "latitude": lat,
                "longitude": lng,
                "city": city,
                "currency": currency,
            }
        except Exception as e:
            logger.debug("[expedia] Failed to normalize listing: %s", e)
            return None

    def _extract_from_html_cards(self, html: str, city: str, check_in: str,
                                 check_out: str, currency: str) -> list[dict]:
        """Fallback: extract hotels from HTML using regex patterns."""
        hotels = []

        # Pattern for hotel name + price pairs in Expedia HTML
        name_pattern = r'aria-label="([^"]+?)"[^>]*class="[^"]*uitk-heading[^"]*"'
        price_pattern = r'"formatted"\s*:\s*"\$?([\d,]+)"'

        names = re.findall(name_pattern, html)
        prices = re.findall(price_pattern, html)

        # Also try JSON-LD
        jsonld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
        for m in re.finditer(jsonld_pattern, html, re.DOTALL):
            try:
                ld = json.loads(m.group(1))
                if isinstance(ld, dict) and ld.get("@type") == "Hotel":
                    name = ld.get("name", "")
                    price_obj = ld.get("priceRange") or ld.get("offers", {})
                    price = 0.0
                    if isinstance(price_obj, dict):
                        price = _to_float(price_obj.get("price", 0))

                    hotels.append({
                        "name": name,
                        "price": price,
                        "star_rating": _to_int(ld.get("starRating", {}).get("ratingValue", 0)),
                        "guest_rating": _to_float(ld.get("aggregateRating", {}).get("ratingValue", 0)),
                        "review_count": _to_int(ld.get("aggregateRating", {}).get("reviewCount", 0)),
                        "image_url": ld.get("image", ""),
                        "deep_link": ld.get("url", ""),
                        "latitude": _to_float(ld.get("geo", {}).get("latitude", 0)),
                        "longitude": _to_float(ld.get("geo", {}).get("longitude", 0)),
                        "city": city,
                        "currency": currency,
                    })
            except (json.JSONDecodeError, AttributeError):
                continue

        return hotels


def _deep_get(d: dict, *keys, default=None):
    """Safely get nested dict value."""
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        else:
            return default
    return d


def _to_float(val) -> float:
    """Convert value to float safely."""
    if val is None:
        return 0.0
    try:
        if isinstance(val, str):
            val = re.sub(r"[^\d.]", "", val.replace(",", ""))
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


def _to_int(val) -> int:
    """Convert value to int safely."""
    if val is None:
        return 0
    try:
        if isinstance(val, str):
            val = re.sub(r"[^\d]", "", val)
        return int(float(val)) if val else 0
    except (ValueError, TypeError):
        return 0
