"""Trivago adapter — normalizes Trivago scraper output to universal hotel schema."""
import logging
from datetime import datetime
import pandas as pd
from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import make_hotel_row
from .scraper import TrivagoScraper

logger = logging.getLogger(__name__)

class TrivagoAdapter(HotelBaseAdapter):
    NAME = "trivago"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(self, city: str, check_in: str, check_out: str,
               adults: int = 2, children: int = 0, rooms: int = 1,
               currency: str = "USD", **kwargs) -> pd.DataFrame:
        print(f"  [{self.NAME}] Searching hotels in {city}...")
        try:
            nights = max((datetime.strptime(check_out, "%Y-%m-%d") - datetime.strptime(check_in, "%Y-%m-%d")).days, 1)
        except ValueError:
            return self.empty_result()
        proxy = self.proxy_manager.get_proxy() if self.proxy_manager else None
        try:
            raw = TrivagoScraper(proxy=proxy).search(city, check_in, check_out, adults, rooms, currency)
        except Exception as e:
            logger.error("[%s] Error: %s", self.NAME, e)
            return self.empty_result()
        rows = []
        for h in raw:
            price = float(h.get("price", 0) or 0)
            ppn = round(price / nights, 2) if price > 0 else 0.0
            rows.append(make_hotel_row(
                source=self.NAME, hotel_name=h.get("name", ""), hotel_address="",
                city=city, country="",
                latitude=float(h.get("latitude", 0) or 0), longitude=float(h.get("longitude", 0) or 0),
                star_rating=int(h.get("star_rating", 0) or 0), guest_rating=float(h.get("guest_rating", 0) or 0),
                review_count=int(h.get("review_count", 0) or 0),
                check_in=check_in, check_out=check_out, nights=nights,
                room_type="", board_type="",
                price=price, price_per_night=ppn, currency=currency,
                booking_provider="Trivago", cancellation="", amenities="",
                deep_link=h.get("deep_link", ""), image_url=h.get("image_url", ""),
            ))
        print(f"  [{self.NAME}] ✓ Found {len(rows)} hotels")
        return pd.DataFrame(rows) if rows else self.empty_result()
