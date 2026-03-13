"""Priceline Hotels adapter (curl_cffi-based scraper)."""

import json
import logging

import pandas as pd

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import HOTEL_COMMON_COLUMNS, make_hotel_row

logger = logging.getLogger(__name__)


class PricelineAdapter(HotelBaseAdapter):
    NAME = "priceline"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="USD", **kw):
        from .scraper import PricelineScraper

        print(f"  [{self.NAME}] Searching hotels in {city}...")
        try:
            raw = PricelineScraper().search(
                city,
                check_in,
                check_out,
                adults=adults,
                rooms=rooms,
                currency=currency,
                proxy_manager=self.proxy_manager,
                max_results=kw.pop("max_results", 500),
                **kw,
            )
        except Exception as exc:
            logger.error("[priceline] Search failed: %s", exc, exc_info=True)
            return self.empty_result()

        return self._normalize(raw, currency)

    def _normalize(self, hotels, currency="USD"):
        if not hotels:
            return self.empty_result()

        rows = []
        for h in hotels:
            try:
                info = h.get("hotelInfo", {})
                rate = h.get("minRateSummary", {})

                # Try multiple known Priceline amount shapes to avoid dropping all rows.
                grand_total_amt = (rate.get("grandTotal") or {}).get("amount", 0)
                grand_total_extax = (rate.get("grandTotalExcludingTax") or {}).get("amount", 0)
                min_price_amt = (rate.get("minPrice") or {}).get("amount", 0)
                fallback_amt = (rate.get("price") or {}).get("amount", 0)
                amount_raw = grand_total_amt or grand_total_extax or min_price_amt or fallback_amt or 0
                if isinstance(amount_raw, str):
                    amount_raw = amount_raw.replace(",", "").strip()

                price = float(amount_raw or 0)
                if price <= 0:
                    continue

                loc = info.get("location", {})
                geo = info.get("geoCoordinate", {})

                amenities_raw = info.get("amenities", [])
                amenities = ", ".join(str(a.get("name", "")) for a in amenities_raw if isinstance(a, dict))

                images_raw = info.get("images", [])
                if not isinstance(images_raw, list):
                    images_raw = []
                image_url = ""
                for img in images_raw:
                    if isinstance(img, dict):
                        image_url = img.get("fastlyUrl") or img.get("source") or ""
                        if image_url:
                            break

                scores_raw = info.get("reviewInfo", {}).get("reviewSummary", {}).get("scores", [])
                review_score = 0.0
                if isinstance(scores_raw, list):
                    for s in scores_raw:
                        if isinstance(s, dict) and s.get("score") is not None:
                            try:
                                review_score = float(s["score"])
                                break
                            except (TypeError, ValueError):
                                pass
                elif isinstance(scores_raw, dict):
                    try:
                        review_score = float(scores_raw.get("score", 0) or 0)
                    except (TypeError, ValueError):
                        pass

                stars = 0
                star_text = str(info.get("starLevelText", ""))
                try:
                    stars = float(star_text.split(" ")[0]) if star_text else 0
                except Exception:
                    pass

                hotel_id = info.get("id", "")
                link = f"https://www.priceline.com/relax/at/{hotel_id}" if hotel_id else ""

                extended = {
                    "rates": rate,
                    "badges": info.get("traitBadges", []),
                }
                raw_data_str = json.dumps(extended, default=str, ensure_ascii=False)

                rows.append(
                    make_hotel_row(
                        source=self.NAME,
                        hotel_name=str(info.get("name", "")),
                        hotel_address=f"{info.get('neighborhood', {}).get('name', '')}, {loc.get('city', '')}".strip(" ,"),
                        city=str(loc.get("city", "")),
                        latitude=float(geo.get("latitude", 0) or 0),
                        longitude=float(geo.get("longitude", 0) or 0),
                        star_rating=int(stars),
                        guest_rating=float(review_score),
                        review_count=0,
                        check_in="",
                        check_out="",
                        nights=1,
                        room_type="",
                        board_type="",
                        price=price,
                        price_per_night=float(min_price_amt or 0),
                        currency=(rate.get("minPrice") or {}).get("currencyCode", currency),
                        booking_provider="Priceline",
                        cancellation=str((rate.get("cancellationPolicy") or {}).get("label", "")),
                        amenities=amenities,
                        deep_link=link,
                        image_url=str(image_url),
                        raw_data=raw_data_str,
                    )
                )
            except Exception as e:
                logger.debug("[priceline] Parse error: %s", e)
                continue

        logger.info("[%s] Found %d hotels", self.NAME, len(rows))
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)
