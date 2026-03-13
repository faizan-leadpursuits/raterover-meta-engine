"""
Trip.com Hotels adapter — wraps the TripHotelsScraper with standard
HotelBaseAdapter interface for the metasearch API.
"""

import re
import logging
import pandas as pd

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import make_hotel_row
from .scraper import TripHotelsScraper

logger = logging.getLogger(__name__)


# ── City-ID cache & known-good mappings ──────────────────────────
# Trip.com uses opaque numeric city IDs for its internal fetchHotelList API.
# These are resolved dynamically at runtime via _resolve_city_id().
# The map below is a warm-start cache of verified IDs.
_CITY_ID_CACHE = {
    "dubai": 220,
    "london": 338,
    "paris": 192,
    "vienna": 374,
    "wien": 374,
    "new york": 676,
    "tokyo": 370,
    "bangkok": 141,
    "singapore": 187,
    "istanbul": 182,
    "rome": 386,
    "barcelona": 1444,
    "mumbai": 1424,
    "delhi": 1330,
    "cairo": 491,
    "kuala lumpur": 171,
    "hong kong": 58,
    "amsterdam": 318,
    "berlin": 307,
    "sydney": 262,
    "los angeles": 977,
    "san francisco": 972,
}


def _resolve_city_id(city: str, proxy_manager=None) -> int | None:
    """
    Dynamically resolve a city name to a Trip.com cityId.

    Strategy:
      1. Check the warm-start cache
      2. Hit Trip.com /hotels/list?cityName=<city> and extract the resolved
         cityId from the resulting page or redirected URL
      3. Cache the result for future calls
    """
    key = city.lower().strip()

    # ── 1. Cache hit ──────────────────────────────────────────────
    if key in _CITY_ID_CACHE:
        return _CITY_ID_CACHE[key]

    # ── 2. Dynamic resolution via Trip.com search page ────────────
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        logger.warning("[trip_hotels] curl_cffi not available for city resolution")
        return None

    proxy_url = None
    if proxy_manager:
        proxy_url = proxy_manager.get_proxy()

    try:
        sess = cffi_requests.Session(verify=False, impersonate="chrome124", timeout=20)
        if proxy_url:
            sess.proxies = {"http": proxy_url, "https": proxy_url}

        # Trip.com search page — the server embeds the resolved cityId in the page
        search_url = f"https://www.trip.com/hotels/list?cityName={city}"
        resp = sess.get(search_url, allow_redirects=True, timeout=20, headers={
            "accept": "text/html,application/xhtml+xml",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })

        if resp.status_code != 200:
            logger.warning("[trip_hotels] City resolution HTTP %s for '%s'", resp.status_code, city)
            return None

        final_url = str(resp.url)
        text = resp.text

        # Try to extract cityId from the redirected URL
        url_match = re.search(r"[?&]city[Ii]d=(\d+)", final_url)
        if url_match:
            city_id = int(url_match.group(1))
            _CITY_ID_CACHE[key] = city_id
            logger.info("[trip_hotels] Resolved '%s' -> cityId=%d (from URL)", city, city_id)
            return city_id

        # Try to extract from page content (various patterns)
        for pattern in [
            r'"cityId":\s*"?(\d+)"?',
            r"city[=:](\d+)",
            r'"CityId":\s*(\d+)',
        ]:
            matches = re.findall(pattern, text[:300000])
            if matches:
                # Take the most common value
                from collections import Counter
                most_common = Counter(matches).most_common(1)[0][0]
                city_id = int(most_common)
                if city_id > 0:
                    _CITY_ID_CACHE[key] = city_id
                    logger.info("[trip_hotels] Resolved '%s' -> cityId=%d (from page)", city, city_id)
                    return city_id

        logger.warning("[trip_hotels] Could not resolve cityId for '%s'", city)
        return None

    except Exception as e:
        logger.warning("[trip_hotels] City resolution error for '%s': %s", city, e)
        return None


class TripHotelsAdapter(HotelBaseAdapter):
    """Adapter for Trip.com Hotels via internal fetchHotelList API."""

    NAME = "trip_hotels"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int = 2,
        children: int = 0,
        rooms: int = 1,
        currency: str = "USD",
        **kwargs,
    ) -> pd.DataFrame:
        logger.info("[%s] Searching hotels in %s...", self.NAME, city)

        # Resolve city to Trip.com internal ID (dynamic + cached)
        city_id = _resolve_city_id(city, proxy_manager=self.proxy_manager)
        if city_id is None:
            logger.warning("[%s] Cannot resolve city '%s', skipping", self.NAME, city)
            return self.empty_result()

        # Get proxy URL strings for curl_cffi (not Playwright dicts)
        proxy_list = None
        if self.proxy_manager:
            from core.proxy_helpers import build_curl_cffi_proxy_list
            cffi_proxies = build_curl_cffi_proxy_list(proxy_manager=self.proxy_manager)
            proxy_list = [url for _, url in cffi_proxies if url]

        scraper = TripHotelsScraper(proxies=proxy_list)

        try:
            max_results = kwargs.get("max_results", 0)
            # Compute pages: trip.com page_size≈25, so 1 page often covers limit=100
            if max_results > 0:
                pages = min(kwargs.get("pages", 2), max(1, (max_results + 24) // 25))
            else:
                pages = kwargs.get("pages", 2)
            raw_results = scraper.search(
                city_id=city_id,
                city_name=city.capitalize(),
                checkin=check_in,
                checkout=check_out,
                adults=adults,
                rooms=rooms,
                pages=pages,
                currency=currency,
                locale=kwargs.get("locale", "en-XX"),
            )
        except Exception as e:
            logger.error("[%s] Scraper error: %s", self.NAME, e, exc_info=True)
            return self.empty_result()

        # Normalize to HOTEL_COMMON_COLUMNS
        rows = []
        for prop in raw_results:
            try:
                import json
                raw_data_str = json.dumps(prop, default=str, ensure_ascii=False)
                rows.append(make_hotel_row(
                    source=self.NAME,
                    hotel_name=prop.get("hotel_name", ""),
                    hotel_address=prop.get("district", ""),
                    city=prop.get("city", city),
                    country=prop.get("country", ""),
                    latitude=float(prop.get("latitude", 0.0) or 0.0),
                    longitude=float(prop.get("longitude", 0.0) or 0.0),
                    star_rating=int(prop.get("star_rating", 0) or 0),
                    guest_rating=float(prop.get("guest_rating", 0.0) or 0.0),
                    review_count=int(prop.get("review_count", 0) or 0),
                    check_in=check_in,
                    check_out=check_out,
                    nights=prop.get("nights", 1),
                    room_type=prop.get("room_type", ""),
                    board_type="",
                    price=float(prop.get("total_price", 0.0) or 0.0),
                    price_per_night=float(prop.get("price_per_night", 0.0) or 0.0),
                    currency=prop.get("currency", currency),
                    booking_provider="Trip.com",
                    cancellation="",
                    amenities=", ".join(prop.get("amenities", []) or []) if isinstance(prop.get("amenities"), list) else str(prop.get("amenities", "")),
                    deep_link=prop.get("booking_url", ""),
                    image_url=prop.get("thumbnail_url", ""),
                    raw_data=raw_data_str,
                ))
            except Exception as e:
                logger.warning("[%s] Parse error for hotel '%s': %s", self.NAME, prop.get("hotel_name"), e)

        logger.info("[%s] Found %d hotels", self.NAME, len(rows))
        return pd.DataFrame(rows) if rows else self.empty_result()

