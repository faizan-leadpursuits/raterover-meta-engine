"""
MomondoHotels hotel scraper — curl_cffi HTML scraper for www.momondo.com.
"""
import re, json, logging
from urllib.parse import quote_plus
from curl_cffi.requests import Session

logger = logging.getLogger(__name__)


class MomondoHotelsScraper:
    def __init__(self, proxy: str = None):
        self.session = Session(impersonate="chrome131")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def search(self, city: str, check_in: str, check_out: str,
               adults: int = 2, rooms: int = 1, currency: str = "USD") -> list[dict]:
        logger.info("[momondo_hotels] Searching '%s'...", city)
        url = "https://www.momondo.com/hotels/{city}/{check_in}/{check_out}/{rooms}rooms/{adults}adults".format(
            city=quote_plus(city), check_in=check_in, check_out=check_out,
            adults=adults, rooms=rooms)
        try:
            resp = self.session.get(url, headers={
                "accept": "text/html,application/xhtml+xml",
                "accept-language": "en-US,en;q=0.9",
            }, timeout=20, allow_redirects=True)
            if resp.status_code != 200:
                logger.warning("[momondo_hotels] Status %d", resp.status_code)
                return []
            return self._parse(resp.text or "", city, currency)
        except Exception as e:
            logger.error("[momondo_hotels] Error: %s", e)
            return []

    def _parse(self, html: str, city: str, currency: str) -> list[dict]:
        hotels = []
        # Try embedded JSON data
        for pat in [r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
                    r'"results"\s*:\s*(\[.*?\])\s*,',
                    r'"hotels"\s*:\s*(\[.*?\])']:
            for m in re.finditer(pat, html, re.DOTALL):
                try:
                    data = json.loads(m.group(1))
                    parsed = self._extract(data, city, currency)
                    if parsed:
                        return parsed
                except (json.JSONDecodeError, TypeError):
                    continue
        # JSON-LD fallback
        for m in re.finditer(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL):
            try:
                ld = json.loads(m.group(1))
                if isinstance(ld, list):
                    for item in ld:
                        if isinstance(item, dict) and item.get("@type") in ("Hotel", "LodgingBusiness"):
                            hotels.append(self._from_jsonld(item, city, currency))
                elif isinstance(ld, dict) and ld.get("@type") in ("Hotel", "LodgingBusiness"):
                    hotels.append(self._from_jsonld(ld, city, currency))
            except (json.JSONDecodeError, TypeError):
                continue
        return [h for h in hotels if h]

    def _extract(self, data, city: str, currency: str) -> list[dict]:
        hotels = []
        def walk(obj, depth=0):
            if depth > 6 or not isinstance(obj, (dict, list)):
                return
            if isinstance(obj, dict):
                for key in ["results", "hotels", "properties", "items", "accommodations", "listings"]:
                    val = obj.get(key)
                    if isinstance(val, list) and val:
                        for item in val:
                            if isinstance(item, dict):
                                h = self._norm(item, city, currency)
                                if h:
                                    hotels.append(h)
                        return
                for v in obj.values():
                    walk(v, depth + 1)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    h = self._norm(item, city, currency)
                    if h:
                        hotels.append(h)
        else:
            walk(data)
        return hotels

    def _norm(self, item: dict, city: str, currency: str) -> dict | None:
        name = item.get("name", item.get("hotelName", item.get("displayName", "")))
        if not name:
            return None
        price = float(item.get("price", item.get("cheapestPrice", item.get("minPrice", 0))) or 0)
        return {"name": name, "price": price,
                "star_rating": int(item.get("stars", item.get("starRating", 0)) or 0),
                "guest_rating": float(item.get("rating", item.get("overallRating", 0)) or 0),
                "review_count": int(item.get("reviewCount", item.get("reviews", 0)) or 0),
                "image_url": item.get("imageUrl", item.get("thumbnailUrl", item.get("image", ""))),
                "deep_link": item.get("url", item.get("deeplink", item.get("link", ""))),
                "latitude": float(item.get("latitude", item.get("lat", 0)) or 0),
                "longitude": float(item.get("longitude", item.get("lng", item.get("lon", 0))) or 0),
                "city": city, "currency": currency}

    def _from_jsonld(self, ld: dict, city: str, currency: str) -> dict | None:
        name = ld.get("name", "")
        if not name:
            return None
        price = 0.0
        offers = ld.get("offers", {})
        if isinstance(offers, dict):
            price = float(offers.get("price", 0) or 0)
        geo = ld.get("geo", {})
        return {"name": name, "price": price,
                "star_rating": int(ld.get("starRating", {}).get("ratingValue", 0) or 0) if isinstance(ld.get("starRating"), dict) else 0,
                "guest_rating": float(ld.get("aggregateRating", {}).get("ratingValue", 0) or 0) if isinstance(ld.get("aggregateRating"), dict) else 0,
                "review_count": int(ld.get("aggregateRating", {}).get("reviewCount", 0) or 0) if isinstance(ld.get("aggregateRating"), dict) else 0,
                "image_url": ld.get("image", ""), "deep_link": ld.get("url", ""),
                "latitude": float(geo.get("latitude", 0) or 0),
                "longitude": float(geo.get("longitude", 0) or 0),
                "city": city, "currency": currency}
