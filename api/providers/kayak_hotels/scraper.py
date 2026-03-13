"""
Kayak Hotels scraper — uses curl_cffi to scrape hotel search results.
Kayak is a meta-search engine owned by Booking Holdings.
"""
import re, json, time, logging
from urllib.parse import urlencode, quote_plus
from curl_cffi.requests import Session

logger = logging.getLogger(__name__)

class KayakHotelsScraper:
    def __init__(self, proxy: str = None):
        self.session = Session(impersonate="chrome131")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def search(self, city: str, check_in: str, check_out: str,
               adults: int = 2, rooms: int = 1, currency: str = "USD") -> list[dict]:
        logger.info("[kayak_hotels] Searching '%s'...", city)
        city_slug = city.lower().replace(" ", "-")
        url = f"https://www.kayak.com/hotels/{quote_plus(city)}/{check_in}/{check_out}/{rooms}rooms/{adults}adults"
        try:
            resp = self.session.get(url, headers={
                "accept": "text/html",
                "accept-language": "en-US,en;q=0.9",
            }, timeout=20, allow_redirects=True)
            if resp.status_code != 200:
                logger.warning("[kayak_hotels] Status %d", resp.status_code)
                return []
            return self._parse(resp.text or "", city, currency)
        except Exception as e:
            logger.error("[kayak_hotels] Error: %s", e)
            return []

    def _parse(self, html: str, city: str, currency: str) -> list[dict]:
        hotels = []
        # Try embedded JSON
        for pattern in [r'"results"\s*:\s*(\[.*?\])\s*,', r'"hotels"\s*:\s*(\[.*?\])',
                       r'<script[^>]*type="application/json"[^>]*>(.*?)</script>']:
            for m in re.finditer(pattern, html, re.DOTALL):
                try:
                    data = json.loads(m.group(1))
                    if isinstance(data, list):
                        for item in data:
                            h = self._normalize(item, city, currency)
                            if h:
                                hotels.append(h)
                        if hotels:
                            return hotels
                except (json.JSONDecodeError, TypeError):
                    continue
        # Fallback: regex from HTML cards
        names = re.findall(r'class="[^"]*hotel-name[^"]*"[^>]*>([^<]+)', html)
        prices = re.findall(r'class="[^"]*price-text[^"]*"[^>]*>\s*\$?([\d,]+)', html)
        for i, name in enumerate(names):
            price = float(prices[i].replace(",", "")) if i < len(prices) else 0.0
            hotels.append({"name": name.strip(), "price": price, "star_rating": 0, "guest_rating": 0,
                          "review_count": 0, "image_url": "", "deep_link": "",
                          "latitude": 0, "longitude": 0, "city": city, "currency": currency})
        return hotels

    def _normalize(self, item: dict, city: str, currency: str) -> dict | None:
        name = item.get("name", item.get("displayName", ""))
        if not name:
            return None
        price = float(item.get("price", item.get("cheapestPrice", item.get("minPrice", 0))) or 0)
        return {"name": name, "price": price,
                "star_rating": int(item.get("stars", item.get("starRating", 0)) or 0),
                "guest_rating": float(item.get("rating", item.get("overallRating", 0)) or 0),
                "review_count": int(item.get("reviewCount", item.get("reviews", 0)) or 0),
                "image_url": item.get("imageUrl", item.get("thumbnailUrl", "")),
                "deep_link": item.get("url", item.get("deeplink", "")),
                "latitude": float(item.get("latitude", 0) or 0),
                "longitude": float(item.get("longitude", 0) or 0),
                "city": city, "currency": currency}
