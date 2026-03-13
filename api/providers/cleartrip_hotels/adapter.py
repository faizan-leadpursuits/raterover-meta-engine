"""Cleartrip Hotels adapter - normalizes results into universal hotel schema."""

import logging

import pandas as pd

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import HOTEL_COMMON_COLUMNS, make_hotel_row

logger = logging.getLogger(__name__)


class CleartripHotelsAdapter(HotelBaseAdapter):
    NAME = "cleartrip_hotels"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="USD", **kw):
        from .scraper import CleartripHotelsScraper

        print(f"  [{self.NAME}] Searching hotels in {city}...")
        raw = CleartripHotelsScraper().search(
            city,
            check_in,
            check_out,
            adults,
            rooms,
            currency,
            proxy_manager=self.proxy_manager,
            **kw,
        )
        return self._normalize(raw, currency)

    def _normalize(self, hotels, currency="USD"):
        if not hotels:
            return self.empty_result()

        rows = []
        for h in hotels:
            try:
                price = float(h.get("price", 0) or 0)
                if price <= 0:
                    continue
                import json
                raw_data_str = json.dumps(h, default=str, ensure_ascii=False)
                rows.append(
                    make_hotel_row(
                        source=self.NAME,
                        hotel_name=str(h.get("hotel_name", "")),
                        hotel_address=str(h.get("hotel_address", "")),
                        city=str(h.get("city", "")),
                        latitude=float(h.get("latitude", 0) or 0),
                        longitude=float(h.get("longitude", 0) or 0),
                        star_rating=int(h.get("star_rating", 0) or 0),
                        guest_rating=float(h.get("guest_rating", 0) or 0),
                        review_count=int(h.get("review_count", 0) or 0),
                        check_in=str(h.get("check_in", "")),
                        check_out=str(h.get("check_out", "")),
                        nights=int(h.get("nights", 1) or 1),
                        room_type=str(h.get("room_type", "")),
                        board_type=str(h.get("board_type", "")),
                        price=price,
                        price_per_night=float(h.get("price_per_night", 0) or 0),
                        currency=str(h.get("currency", currency) or currency),
                        booking_provider=str(h.get("booking_provider", "")),
                        cancellation=str(h.get("cancellation", "")),
                        amenities=str(h.get("amenities", "")),
                        deep_link=str(h.get("deep_link", "")),
                        image_url=str(h.get("image_url", "")),
                        raw_data=raw_data_str,
                    )
                )
            except Exception:
                continue

        logger.info("[%s] Found %d hotels", self.NAME, len(rows))
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)
