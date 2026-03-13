"""
Hotwire adapter — uses the unified ExpediaGroupScraper GraphQL engine.
"""

import logging
from datetime import datetime
import pandas as pd
import re

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import make_hotel_row, HOTEL_COMMON_COLUMNS
from core.city_resolver import get_resolver
from providers.hotels_com.scraper import clean_hotel # Shared normalization logic

logger = logging.getLogger(__name__)

def _parse_price(formatted: str) -> float:
    if not formatted: return 0.0
    digits = re.sub(r"[^\d.]", "", formatted.replace(",", ""))
    try: return float(digits) if digits else 0.0
    except ValueError: return 0.0

class HotwireAdapter(HotelBaseAdapter):
    NAME = "hotwire"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="USD", **kw):
        from core.expedia_group_engine import ExpediaGroupScraper
        
        logger.info("[%s] Searching %s...", self.NAME, city)
        
        # 1. Resolve Region (Hotwire uses the same regionIds as Expedia)
        resolver = get_resolver()
        region = resolver.resolve_hotels_com_region(city)
        if not region:
            logger.warning("[%s] Could not resolve region for %s", self.NAME, city)
            return self.empty_result()
            
        # 2. Scrape
        provider = self.proxy_manager.get_provider("dataimpulse") if self.proxy_manager else None
        proxy_url = provider.get_proxy_url() if provider else None
        scraper = ExpediaGroupScraper(brand="hotwire", proxy=proxy_url)
        
        raw_hotels = scraper.search({
            "city": city,
            "regionId": region["regionId"],
            "lat": region["lat"],
            "lng": region["lng"],
            "check_in": check_in,
            "check_out": check_out,
            "adults": adults,
            "rooms": rooms,
            "currency": currency,
        })
        
        return self._normalize(raw_hotels, city, check_in, check_out, currency)

    def _normalize(self, hotels, city, check_in, check_out, currency="USD"):
        if not hotels:
            return self.empty_result()
            
        try:
            ci = datetime.strptime(check_in, "%Y-%m-%d")
            co = datetime.strptime(check_out, "%Y-%m-%d")
            nights = max((co - ci).days, 1)
        except:
            nights = 1
            
        rows = []
        for raw in hotels:
            cleaned = clean_hotel(raw)
            if not cleaned: continue
            
            try:
                price = _parse_price(cleaned.get("price_total", ""))
                ppn = round(price / nights, 2) if price > 0 else 0.0
                
                rows.append(make_hotel_row(
                    source=self.NAME,
                    hotel_name=cleaned["name"],
                    city=city,
                    check_in=check_in,
                    check_out=check_out,
                    nights=nights,
                    price=price,
                    price_per_night=ppn,
                    currency=currency,
                    image_url=cleaned.get("image_1", ""),
                    deep_link=cleaned.get("hotel_url", ""),
                    booking_provider="Hotwire",
                    amenities=cleaned.get("amenities", ""),
                ))
            except Exception as e:
                logger.debug("[%s] Normalize error: %s", self.NAME, e)
                
        logger.info("[%s] Found %d hotels", self.NAME, len(rows))
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)
