"""
Trivago hotel scraper — scrapes hotel search results via curl_cffi.
Uses the Trivago search API endpoint to get hotel listings.
"""

import re
import json
import time
import logging
from urllib.parse import urlencode, quote_plus

from curl_cffi.requests import Session

logger = logging.getLogger(__name__)

BASE_URL = "https://www.trivago.com"


class TrivagoScraper:
    """Scrape Trivago hotel search results."""

    def __init__(self, proxy: str = None):
        self.session = Session(impersonate="chrome131")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def search(self, city: str, check_in: str, check_out: str,
               adults: int = 2, rooms: int = 1, currency: str = "USD") -> list[dict]:
        logger.info("[trivago] Searching '%s'...", city)

        # Step 1: Search via the API-like endpoint
        try:
            search_url = f"{BASE_URL}/en-US/srl/hotels-{quote_plus(city.lower().replace(' ', '-'))}"
            params = {
                "search": f"100-{int(time.time())}",
                "arrivalDate": check_in,
                "departureDate": check_out,
                "rooms": rooms,
                "adults": adults,
            }

            resp = self.session.get(
                f"{search_url}?{urlencode(params)}",
                headers={
                    "accept": "text/html,application/xhtml+xml",
                    "accept-language": "en-US,en;q=0.9",
                },
                timeout=20,
                allow_redirects=True,
            )

            if resp.status_code != 200:
                logger.warning("[trivago] Search returned %d", resp.status_code)
                return []

            return self._parse_html(resp.text or "", city, currency)

        except Exception as e:
            logger.error("[trivago] Search failed: %s", e)
            return []

    def _parse_html(self, html: str, city: str, currency: str) -> list[dict]:
        """Parse hotel data from Trivago's HTML."""
        hotels = []

        # Try to find embedded JSON data
        patterns = [
            r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
            r'"accommodations"\s*:\s*(\[.*?\])',
            r'"searchResult"\s*:\s*(\{.*?\})\s*,',
            r'__NEXT_DATA__.*?"props"\s*:\s*(\{.*?\})\s*,\s*"page"',
        ]

        for pattern in patterns:
            for m in re.finditer(pattern, html, re.DOTALL):
                try:
                    data = json.loads(m.group(1))
                    parsed = self._extract_hotels(data, city, currency)
                    if parsed:
                        return parsed
                except (json.JSONDecodeError, KeyError):
                    continue

        # Fallback: regex extraction from HTML
        # Look for hotel name patterns
        name_re = r'data-testid="[^"]*item-name[^"]*"[^>]*>([^<]+)<'
        price_re = r'data-testid="[^"]*recommended-price[^"]*"[^>]*>.*?(\d[\d,]*)'

        names = re.findall(name_re, html)
        prices = re.findall(price_re, html, re.DOTALL)

        for i, name in enumerate(names):
            price = 0.0
            if i < len(prices):
                try:
                    price = float(prices[i].replace(",", ""))
                except ValueError:
                    pass
            hotels.append({
                "name": name.strip(),
                "price": price,
                "star_rating": 0,
                "guest_rating": 0,
                "review_count": 0,
                "image_url": "",
                "deep_link": "",
                "latitude": 0, "longitude": 0,
                "city": city, "currency": currency,
            })

        return hotels

    def _extract_hotels(self, data, city: str, currency: str) -> list[dict]:
        """Extract hotels from parsed JSON."""
        hotels = []

        def find_accommodations(obj, depth=0):
            if depth > 6 or not isinstance(obj, (dict, list)):
                return
            if isinstance(obj, dict):
                for key in ["accommodations", "items", "hotels", "results", "properties"]:
                    val = obj.get(key)
                    if isinstance(val, list) and val:
                        for item in val:
                            if isinstance(item, dict):
                                h = self._parse_item(item, city, currency)
                                if h:
                                    hotels.append(h)
                        return
                for v in obj.values():
                    find_accommodations(v, depth + 1)

        find_accommodations(data)
        return hotels

    def _parse_item(self, item: dict, city: str, currency: str) -> dict | None:
        name = item.get("name", item.get("accommodationName", ""))
        if not name:
            return None

        price = 0.0
        deal = item.get("deal", item.get("bestDeal", item.get("bestOffer", {})))
        if isinstance(deal, dict):
            price = float(deal.get("price", deal.get("pricePerNight", 0)) or 0)

        return {
            "name": name,
            "price": price,
            "star_rating": int(item.get("category", item.get("stars", 0)) or 0),
            "guest_rating": float(item.get("ratingValue", item.get("rating", 0)) or 0),
            "review_count": int(item.get("reviewCount", item.get("reviews", 0)) or 0),
            "image_url": item.get("image", {}).get("url", "") if isinstance(item.get("image"), dict) else "",
            "deep_link": item.get("url", item.get("path", "")),
            "latitude": float(item.get("latitude", item.get("geo", {}).get("lat", 0)) or 0),
            "longitude": float(item.get("longitude", item.get("geo", {}).get("lng", 0)) or 0),
            "city": city,
            "currency": currency,
        }
