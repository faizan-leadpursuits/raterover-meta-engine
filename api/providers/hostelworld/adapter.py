"""Hostelworld Hotels adapter — bridges HostelworldScraper to the common hotel schema."""

import logging
import pandas as pd

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import HOTEL_COMMON_COLUMNS, make_hotel_row
from .scraper import HostelworldScraper

logger = logging.getLogger(__name__)


class HostelworldAdapter(HotelBaseAdapter):
    """
    Adapter for Hostelworld. Uses the Playwright-based scraper (no API key needed).
    """

    NAME             = "hostelworld"
    DOMAIN           = "hotels"
    NEEDS_PROXY      = False
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def __init__(self, proxy_manager=None):
        super().__init__(proxy_manager)
        self._scraper = HostelworldScraper()

    # ── Public search ─────────────────────────────────────────────────────────

    def search(
        self,
        city:      str,
        check_in:  str,
        check_out: str,
        adults:    int = 2,
        rooms:     int = 1,
        currency:  str = "USD",
        **kwargs,
    ) -> pd.DataFrame:
        from datetime import datetime

        try:
            nights = max(1, (
                datetime.strptime(check_out, "%Y-%m-%d") -
                datetime.strptime(check_in,  "%Y-%m-%d")
            ).days)
        except ValueError:
            nights = 1

        logger.info("[%s] Searching %s | %s → %s (%d nights, %d guests)",
                    self.NAME, city, check_in, check_out, nights, adults)

        try:
            results = self._scraper.search(
                city=city,
                check_in=check_in,
                check_out=check_out,
                adults=adults,
                rooms=rooms,
                currency=currency,
                proxy=kwargs.get("proxy"),
                proxy_manager=self.proxy_manager,
            )
        except Exception as exc:
            logger.error("[%s] search() raised: %s", self.NAME, exc)
            return self.empty_result()

        return self._normalize(results, check_in, check_out, nights, currency)

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize(
        self,
        results: list,
        check_in:   str,
        check_out:  str,
        nights:     int,
        currency:   str,
    ) -> pd.DataFrame:
        """Normalize results from the Playwright-based HostelworldScraper."""
        if not results:
            return self.empty_result()

        rows = []
        for p in results:
            try:
                total_price = float(p.get("price", 0))
                if total_price <= 0:
                    continue

                ppn = float(p.get("price_per_night", total_price / max(nights, 1)))

                import json
                raw_data_str = json.dumps(p, default=str, ensure_ascii=False)
                rows.append(make_hotel_row(
                    source          = self.NAME,
                    hotel_name      = str(p.get("hotel_name", "")),
                    hotel_address   = str(p.get("hotel_address", "")),
                    city            = str(p.get("city", "")),
                    latitude        = float(p.get("latitude") or 0.0),
                    longitude       = float(p.get("longitude") or 0.0),
                    star_rating     = int(p.get("star_rating") or 0),
                    guest_rating    = _safe_float(p.get("guest_rating")),
                    review_count    = int(p.get("review_count") or 0),
                    check_in        = check_in,
                    check_out       = check_out,
                    nights          = nights,
                    room_type       = str(p.get("room_type", "Dorm")),
                    board_type      = str(p.get("board_type", "")),
                    price           = round(total_price, 2),
                    price_per_night = round(ppn, 2),
                    currency        = str(p.get("currency") or currency),
                    booking_provider= "Hostelworld",
                    cancellation    = str(p.get("cancellation", "")),
                    amenities       = str(p.get("amenities", "")),
                    deep_link       = str(p.get("deep_link", "")),
                    image_url       = str(p.get("image_url", "")),
                    raw_data        = raw_data_str,
                ))
            except Exception as exc:
                logger.debug("[%s] Skipping property %r: %s", self.NAME, p.get("hotel_name"), exc)
                continue

        logger.info("[%s] Normalized %d/%d properties", self.NAME, len(rows), len(results))
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)

    def empty_result(self) -> pd.DataFrame:
        return pd.DataFrame(columns=HOTEL_COMMON_COLUMNS)


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(value) -> float:
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return 0.0


def _first_room_type(rooms: list | None) -> str:
    """Return the room type of the cheapest / first room, or empty string."""
    if not rooms:
        return ""
    first = rooms[0]
    if first.get("is_private"):
        return "Private"
    return first.get("type") or "Dorm"
