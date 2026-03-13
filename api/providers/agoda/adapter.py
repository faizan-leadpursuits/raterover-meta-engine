"""Agoda Hotels adapter — normalizes API results into universal hotel schema."""

import json
import pandas as pd
from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import HOTEL_COMMON_COLUMNS, make_hotel_row


class AgodaHotelsAdapter(HotelBaseAdapter):
    """Adapter for Agoda Hotels (GraphQL API)."""

    NAME = "agoda_hotels"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="USD", **kw):
        from .scraper import AgodaHotelsScraper

        print(f"  [{self.NAME}] Searching hotels in {city} (GraphQL API)...")
        raw = AgodaHotelsScraper().search(
            city, check_in, check_out, adults, rooms, currency,
            proxy_manager=self.proxy_manager,
            **kw
        )
        return self._normalize(raw, currency)

    def _normalize(self, hotels, currency="USD"):
        if not hotels:
            return self.empty_result()

        rows = []
        for h in hotels:
            try:
                price = float(h.get("price", 0))
                if price <= 0:
                    continue

                # Build raw_data JSON with all the extended fields
                extended = {}
                for key in [
                    "property_id", "property_type", "accommodation_type",
                    "country_code", "area_name", "is_sustainable", "award_year",
                    "original_price", "crossed_out_price", "discount_percent",
                    "cashback_percent", "rooms_available", "is_available",
                    "free_cancellation_date", "pay_later", "pay_at_hotel",
                    "no_credit_card", "is_easy_cancel", "supplier_name",
                    "facility_ids", "family_features", "children_policy",
                    "landmarks", "has_airport_transfer", "city_center_distance",
                    "has_nearby_transport", "favorite_features", "engagement",
                    "hotel_tags", "hotel_views", "atmospheres",
                    "top_selling_point", "is_popular", "booking_count",
                    "cheapest_room_size_sqm", "room_facilities",
                    "promotion_badge", "promotion_desc", "deal_expiry",
                    "is_insider_deal", "renovation_year", "all_images",
                    "review_snippets", "nha_info", "host_level",
                    "supports_long_stay",
                ]:
                    if key in h:
                        extended[key] = h[key]

                raw_data_str = json.dumps(extended, default=str, ensure_ascii=False) if extended else ""

                rows.append(make_hotel_row(
                    source=self.NAME,
                    hotel_name=str(h.get("hotel_name", "")),
                    hotel_address=str(h.get("hotel_address", "")),
                    city=str(h.get("city", "")),
                    country=str(h.get("country", "")),
                    latitude=float(h.get("latitude", 0)),
                    longitude=float(h.get("longitude", 0)),
                    star_rating=int(h.get("star_rating", 0)),
                    guest_rating=float(h.get("guest_rating", 0)),
                    review_count=int(h.get("review_count", 0)),
                    check_in=str(h.get("check_in", "")),
                    check_out=str(h.get("check_out", "")),
                    nights=int(h.get("nights", 1)),
                    room_type=str(h.get("room_type", "")),
                    board_type=str(h.get("board_type", "")),
                    price=price,
                    price_per_night=float(h.get("price_per_night", 0)),
                    currency=str(h.get("currency", currency)),
                    booking_provider=str(h.get("booking_provider", "Agoda")),
                    cancellation=str(h.get("cancellation", "")),
                    amenities=str(h.get("amenities", "")),
                    deep_link=str(h.get("deep_link", "")),
                    image_url=str(h.get("image_url", "")),
                    raw_data=raw_data_str,
                ))
            except Exception:
                continue

        print(f"  [{self.NAME}] ✓ Found {len(rows)} hotels")
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)
