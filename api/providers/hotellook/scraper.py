"""
Hotellook hotel scraper — uses the Hotellook/Aviasales search API.
Hotellook aggregates prices from Booking.com, Agoda, Hotels.com, and other OTAs.

API: https://engine.hotellook.com/api/v2/search/start
     https://engine.hotellook.com/api/v2/search/getResult

Flow:
  1. POST /search/start → get searchId
  2. Poll /search/getResult → get hotel results
"""

import re
import json
import time
import logging
from urllib.parse import urlencode

from curl_cffi.requests import Session

logger = logging.getLogger(__name__)

API_BASE = "https://engine.hotellook.com/api/v2"
MARKER = "direct"  # Public/affiliate marker


class HotellookScraper:
    """Scrape hotel search results from Hotellook's public API."""

    def __init__(self, proxy: str = None):
        self.session = Session(impersonate="chrome131")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def search(self, city: str, check_in: str, check_out: str,
               adults: int = 2, rooms: int = 1, currency: str = "USD") -> list[dict]:
        """Search Hotellook and return hotel dicts."""

        # Resolve city to IATA-like code or use city name
        logger.info("[hotellook] Searching '%s' %s to %s...", city, check_in, check_out)

        # Start search
        params = {
            "cityId": city,
            "checkIn": check_in,
            "checkOut": check_out,
            "adultsCount": adults,
            "roomsCount": rooms,
            "currency": currency.lower(),
            "waitForResult": 1,
            "marker": MARKER,
        }

        try:
            url = f"{API_BASE}/search/start?{urlencode(params)}"
            resp = self.session.get(url, headers={
                "accept": "application/json",
                "user-agent": "Mozilla/5.0",
            }, timeout=15)

            if resp.status_code != 200:
                logger.warning("[hotellook] Start search returned %d", resp.status_code)
                return []

            data = resp.json()
            search_id = data.get("searchId", "")
            if not search_id:
                logger.warning("[hotellook] No searchId in response")
                return []

        except Exception as e:
            logger.error("[hotellook] Start search failed: %s", e)
            return []

        # Poll for results
        hotels = []
        for attempt in range(10):
            time.sleep(2)
            try:
                result_url = f"{API_BASE}/search/getResult?searchId={search_id}&limit=100&sortBy=price&sortAsc=1&marker={MARKER}"
                resp = self.session.get(result_url, headers={
                    "accept": "application/json",
                }, timeout=15)

                if resp.status_code != 200:
                    continue

                data = resp.json()
                result_list = data.get("results", data.get("hotels", []))

                if isinstance(result_list, list) and result_list:
                    for item in result_list:
                        hotel = self._normalize(item, city, check_in, check_out, currency)
                        if hotel:
                            hotels.append(hotel)
                    break

                # Check if search is complete
                if data.get("status") == "ok" or data.get("completed"):
                    break

            except Exception as e:
                logger.debug("[hotellook] Poll %d failed: %s", attempt, e)

        logger.info("[hotellook] Found %d hotels", len(hotels))
        return hotels

    def _normalize(self, item: dict, city: str, check_in: str,
                   check_out: str, currency: str) -> dict | None:
        """Normalize a Hotellook hotel result."""
        try:
            name = item.get("hotelName", item.get("name", ""))
            if not name:
                return None

            # Get the best price from available options
            price = 0.0
            min_price = item.get("minPrice", item.get("priceFrom", 0))
            if isinstance(min_price, (int, float)) and min_price > 0:
                price = float(min_price)
            else:
                # Check rooms/offers for prices
                rooms = item.get("rooms", item.get("offers", []))
                if isinstance(rooms, list):
                    for room in rooms:
                        rp = room.get("price", room.get("total", 0))
                        if isinstance(rp, (int, float)) and rp > 0:
                            price = float(rp)
                            break

            return {
                "name": name,
                "price": price,
                "star_rating": int(item.get("stars", item.get("starRating", 0)) or 0),
                "guest_rating": float(item.get("rating", item.get("guestRating", 0)) or 0),
                "review_count": int(item.get("reviews", item.get("reviewCount", 0)) or 0),
                "image_url": item.get("photoUrl", item.get("photo", "")),
                "deep_link": item.get("url", item.get("deepLink", "")),
                "latitude": float(item.get("latitude", item.get("lat", 0)) or 0),
                "longitude": float(item.get("longitude", item.get("lon", 0)) or 0),
                "address": item.get("address", ""),
                "city": city,
                "currency": currency,
            }
        except Exception:
            return None
