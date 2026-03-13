"""Lastminute Hotels adapter — normalizes searchPageData JSON to universal schema."""

import json
from datetime import datetime
import pandas as pd
from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import HOTEL_COMMON_COLUMNS, make_hotel_row


class LastminuteHotelsAdapter(HotelBaseAdapter):
    """Adapter for Lastminute via searchPageData API."""

    NAME = "lastminute"
    DOMAIN = "hotels"
    NEEDS_PROXY = False  # Cloudflare is bypassed via curl_cffi TLS impersonation
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="GBP", **kw):
        from .scraper import LastminuteScraper
        print(f"  [{self.NAME}] Searching hotels in {city}...")
        raw = LastminuteScraper().search(
            city, check_in, check_out,
            adults=adults, rooms=rooms,
            proxy_manager=self.proxy_manager,
            **kw
        )
        return self._normalize(raw, currency)

    def _normalize(self, hotels: list, currency: str = "GBP") -> pd.DataFrame:
        if not hotels:
            return self.empty_result()

        rows = []
        for card in hotels:
            try:
                meta = card.get("_search_meta", {})
                check_in = meta.get("check_in", "")
                check_out = meta.get("check_out", "")
                adults = meta.get("adults", 2)
                city = meta.get("city", "")

                try:
                    ci = datetime.strptime(check_in, "%Y-%m-%d")
                    co = datetime.strptime(check_out, "%Y-%m-%d")
                    nights = max(1, (co - ci).days)
                except Exception:
                    nights = 1

                prod = card.get("product", {})
                acc = prod.get("accommodation", {})
                rate = prod.get("rate", {})
                geo = prod.get("geo", {})

                # ── Name & Location ──────────────────────────────────────
                hotel_name = str(acc.get("name", ""))
                address = str(acc.get("address", ""))
                country = str(geo.get("countryCode", ""))

                lat = float(acc.get("latitude") or 0.0)
                lng = float(acc.get("longitude") or 0.0)

                # ── Ratings ──────────────────────────────────────────────
                star_rating = int(acc.get("stars", 0))
                # Rating is usually out of 100 on LM, convert to 10
                raw_guest = float(acc.get("rating") or 0.0)
                guest_rating = round(raw_guest / 10.0, 1) if raw_guest > 10 else raw_guest
                review_count = int(acc.get("reviewsNum", 0))

                # ── Pricing ──────────────────────────────────────────────
                price_block = rate.get("price", {})
                price_total = float(price_block.get("price") or 0.0)
                if price_total <= 0:
                    continue
                price_per_night = round(price_total / nights, 2)
                cur = str(price_block.get("currency") or currency)

                # ── Amenities ────────────────────────────────────────────
                fac = acc.get("facilities", {})
                am_list = [k for k, v in fac.items() if v is True]
                amenities = ", ".join(am_list).replace("food", "").replace("family", "")

                # ── Extras ───────────────────────────────────────────────
                image_url = str(acc.get("image", ""))
                deep_link_path = str(card.get("deepLink", ""))
                deep_link = f"https://www.lastminute.com/s/tsx?{deep_link_path}" if deep_link_path else ""

                rows.append(make_hotel_row(
                    source=self.NAME,
                    hotel_name=hotel_name,
                    hotel_address=address,
                    city=city,
                    country=country,
                    latitude=lat,
                    longitude=lng,
                    star_rating=star_rating,
                    guest_rating=guest_rating,
                    review_count=review_count,
                    check_in=check_in,
                    check_out=check_out,
                    nights=nights,
                    room_type="",
                    board_type="",
                    price=price_total,
                    price_per_night=price_per_night,
                    currency=cur,
                    booking_provider="Lastminute",
                    cancellation="Free Cancellation" if rate.get("features", {}).get("cancellation") else "",
                    amenities=amenities,
                    deep_link=deep_link,
                    image_url=image_url,
                    raw_data=json.dumps(card, default=str),
                ))

            except Exception as e:
                print(f"  [{self.NAME}] Parse error: {e}")
                continue

        print(f"  [{self.NAME}] OK Found {len(rows)} hotels")
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)
