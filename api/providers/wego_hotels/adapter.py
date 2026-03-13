"""
Wego Hotels adapter.
"""

import logging
import uuid
import pandas as pd
from datetime import datetime

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import make_hotel_row, HOTEL_COMMON_COLUMNS

logger = logging.getLogger(__name__)

class WegoHotelsAdapter(HotelBaseAdapter):
    NAME = "wego_hotels"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="USD", **kw) -> pd.DataFrame:
        from providers.wego_hotels.scraper import WegoScraper, parse_results
        
        logger.info("[%s] Searching hotels in %s...", self.NAME, city)
        
        # Simple city code resolution
        CITY_CODES = {
            "london": "LON", "paris": "PAR", "new york": "NYC", "dubai": "DXB",
            "tokyo": "TYO", "rome": "ROM", "barcelona": "BCN", "istanbul": "IST",
            "bangkok": "BKK", "amsterdam": "AMS", "berlin": "BER", "madrid": "MAD",
            "singapore": "SIN", "lisbon": "LIS", "prague": "PRG", "vienna": "VIE",
            "milan": "MIL", "los angeles": "LAX"
        }
        city_code = CITY_CODES.get(city.lower().strip(), city[:3].upper())
        
        proxy_url = None
        if self.proxy_manager:
            try:
                prov = self.proxy_manager.get_provider("dataimpulse")
                proxy_url = prov.get_proxy_url() if prov and prov.enabled else self.proxy_manager.get_proxy()
            except Exception:
                proxy_url = self.proxy_manager.get_proxy()
        scraper = WegoScraper(proxy=proxy_url)
        scraper.currency = currency
        
        client_id = str(uuid.uuid4())
        session_id = str(uuid.uuid4())
        
        params = {
            "city_code": city_code,
            "check_in": check_in,
            "check_out": check_out,
            "adults": int(adults),
            "rooms": int(rooms),
            "children": int(children or 0),
        }
        
        try:
            scraper.init_session(client_id, session_id)
            search_id = scraper.create_search(client_id, session_id, params)
            if not search_id:
                return self.empty_result()
                
            poll_data = scraper.poll_results(session_id, search_id, max_polls=5)
            rows_raw = parse_results(poll_data, params)
            
            if not rows_raw:
                return self.empty_result()
                
            return self._normalize(rows_raw, check_in, check_out, currency)
            
        except Exception as e:
            logger.error("[%s] Search failed: %s", self.NAME, e)
            return self.empty_result()

    def _normalize(self, rows_raw, check_in, check_out, currency):
        try:
            ci = datetime.strptime(check_in, "%Y-%m-%d")
            co = datetime.strptime(check_out, "%Y-%m-%d")
            nights = max(1, (co - ci).days)
        except:
            nights = 1

        norm = []
        for r in rows_raw:
            try:
                norm.append(make_hotel_row(
                    source=self.NAME,
                    hotel_name=str(r.get("hotel_name", "")),
                    hotel_address=str(r.get("address", "")),
                    city=str(r.get("city", "")),
                    latitude=float(r.get("latitude") or 0),
                    longitude=float(r.get("longitude") or 0),
                    star_rating=int(r.get("star_rating") or 0),
                    guest_rating=float(r.get("guest_rating") or 0),
                    review_count=int(r.get("review_count") or 0),
                    check_in=check_in,
                    check_out=check_out,
                    nights=nights,
                    room_type=str(r.get("room_type", "")),
                    board_type=str(r.get("board_type", "")),
                    price=float(r.get("price_total", 0)),
                    price_per_night=float(r.get("price_nightly", 0)),
                    currency=str(r.get("currency", currency)),
                    booking_provider="Wego",
                    cancellation=str(r.get("cancellation", "")),
                    deep_link=str(r.get("handoff_url", "")),
                    image_url=str(r.get("image_url", ""))
                ))
            except Exception as e:
                logger.debug("[%s] Row normalization error: %s", self.NAME, e)

        return pd.DataFrame(norm, columns=HOTEL_COMMON_COLUMNS) if norm else self.empty_result()
