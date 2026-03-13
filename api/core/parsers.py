"""
Provider-specific parsers for extracting hotel data from raw Lambda responses.

Lambda returns raw HTTP responses (strings or dicts). These parsers extract
hotel listings from each provider's specific response format.
"""

import json
import logging
import re
from typing import Any

logger = logging.getLogger("api.parsers")


def parse_provider_response(provider: str, extracted: dict) -> list[dict]:
    """
    Parse raw Lambda extracted data into a list of hotel dicts.
    Each parser knows its provider's response structure.
    """
    parser = PARSERS.get(provider, _parse_generic)
    try:
        hotels = parser(extracted)
        if hotels:
            logger.debug("[%s] Parsed %d hotels", provider, len(hotels))
        return hotels or []
    except Exception as e:
        logger.warning("[%s] Parse error: %s", provider, e)
        return []


# ═══════════════════════════════════════════════════════════════
# Provider Parsers
# ═══════════════════════════════════════════════════════════════

def _safe_json(val: Any) -> Any:
    """Parse JSON string if needed."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return val
    return val


def _deep_get(obj: Any, path: str, default=None):
    """Get nested dict value by dot-separated path. Supports list index."""
    for key in path.split("."):
        if obj is None:
            return default
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, list):
            try:
                obj = obj[int(key)]
            except (IndexError, ValueError):
                return default
        else:
            return default
    return obj if obj is not None else default


# ── Booking.com ──────────────────────────────────────────────

def _parse_booking_com(extracted: dict) -> list[dict]:
    """Booking.com GraphQL → hotel list."""
    raw = _safe_json(extracted.get("graphql_response"))
    if not isinstance(raw, dict):
        return []

    results = _deep_get(raw, "data.searchQueries.search.results", [])
    if not isinstance(results, list):
        return []

    hotels = []
    for r in results:
        name = _deep_get(r, "displayName.text", "")
        if not name:
            continue
        price_raw = _deep_get(r, "priceDisplayInfoIrene.displayPrice.amountPerStay.amountUnformatted", 0)
        hotels.append({
            "hotel_name": name,
            "star_rating": _deep_get(r, "basicPropertyData.starRating.value", 0),
            "guest_rating": _deep_get(r, "basicPropertyData.reviews.totalScore", 0),
            "review_count": _deep_get(r, "basicPropertyData.reviews.reviewsCount", 0),
            "address": _deep_get(r, "basicPropertyData.location.address", ""),
            "city": _deep_get(r, "basicPropertyData.location.city", ""),
            "country_code": _deep_get(r, "basicPropertyData.location.countryCode", ""),
            "price": float(price_raw) if price_raw else 0,
            "currency": _deep_get(r, "priceDisplayInfoIrene.displayPrice.amountPerStay.currency", "USD"),
            "image_url": _deep_get(r, "basicPropertyData.photos.main.highResUrl.relativeUrl", ""),
            "free_cancellation": _deep_get(r, "blocks.0.freeCancellationUntil", ""),
        })
    return hotels


# ── Agoda ────────────────────────────────────────────────────

def _parse_agoda(extracted: dict) -> list[dict]:
    """Agoda GraphQL → hotel list."""
    raw = _safe_json(extracted.get("graphql_response"))
    if not isinstance(raw, dict):
        return []

    properties = _deep_get(raw, "data.citySearch.properties", [])
    if not isinstance(properties, list):
        return []

    hotels = []
    for p in properties:
        name = _deep_get(p, "content.informationSummary.displayName", "")
        if not name:
            continue

        # Price from offers
        price_ppn = _deep_get(p, "pricing.offers.roomOffers.0.room.pricing.price.perRoomPerNight.exclusive.display", 0)
        total = _deep_get(p, "pricing.offers.roomOffers.0.room.pricing.price.perBook.exclusive.display", 0)

        hotels.append({
            "hotel_name": name,
            "star_rating": _deep_get(p, "content.informationSummary.rating", 0),
            "guest_rating": _deep_get(p, "content.reviews.cumulative.score", 0),
            "review_count": _deep_get(p, "content.reviews.cumulative.reviewCount", 0),
            "latitude": _deep_get(p, "content.informationSummary.geoInfo.latitude", 0),
            "longitude": _deep_get(p, "content.informationSummary.geoInfo.longitude", 0),
            "price_per_night": float(price_ppn) if price_ppn else 0,
            "price": float(total) if total else 0,
            "currency": _deep_get(p, "pricing.offers.roomOffers.0.room.pricing.currency", "USD"),
            "free_cancellation": _deep_get(p, "pricing.isEasyCancel", False),
            "deep_link": _deep_get(p, "content.informationSummary.propertyLinks.propertyPage", ""),
        })
    return hotels


# ── Trip.com ─────────────────────────────────────────────────

def _parse_trip_com(extracted: dict) -> list[dict]:
    """Trip.com API → hotel list."""
    raw = _safe_json(extracted.get("api_response"))
    if not isinstance(raw, dict):
        return []

    hotel_list = _deep_get(raw, "data.hotelList", [])
    if not isinstance(hotel_list, list):
        return []

    hotels = []
    for h in hotel_list:
        name = _deep_get(h, "hotelInfo.nameInfo.enName", "")
        if not name:
            continue
        hotels.append({
            "hotel_name": name,
            "hotel_id": _deep_get(h, "hotelInfo.summary.hotelId", ""),
            "star_rating": _deep_get(h, "hotelInfo.hotelStar.star", 0),
            "guest_rating": _deep_get(h, "hotelInfo.commentInfo.commentScore", 0),
            "review_count": _deep_get(h, "hotelInfo.commentInfo.commenterNumber", 0),
            "city": _deep_get(h, "hotelInfo.positionInfo.cityNameEn", ""),
            "address": _deep_get(h, "hotelInfo.positionInfo.positionDesc", ""),
            "image_url": _deep_get(h, "hotelInfo.hotelImages.url", ""),
            "price_per_night": _deep_get(h, "roomInfo.0.priceInfo.price", 0),
            "price": _deep_get(h, "roomInfo.0.priceInfoLayer.total.content", 0),
        })
    return hotels


# ── Hotels.com ───────────────────────────────────────────────

def _parse_hotels_com(extracted: dict) -> list[dict]:
    """Hotels.com GraphQL → hotel list."""
    raw = _safe_json(extracted.get("graphql_response"))
    if not isinstance(raw, dict):
        return []

    # Hotels.com uses Expedia's API structure
    results = _deep_get(raw, "data.propertySearch.properties", [])
    if not results:
        results = _deep_get(raw, "data.propertySearch.propertySearchListings", [])
    if not isinstance(results, list):
        return []

    hotels = []
    for r in results:
        name = _deep_get(r, "name", "")
        if not name:
            continue
        price = _deep_get(r, "price.lead.amount", 0)
        hotels.append({
            "hotel_name": name,
            "star_rating": _deep_get(r, "star", 0),
            "guest_rating": _deep_get(r, "reviews.score", 0),
            "review_count": _deep_get(r, "reviews.total", 0),
            "price_per_night": float(price) if price else 0,
            "currency": _deep_get(r, "price.lead.currencyInfo.code", "USD"),
            "image_url": _deep_get(r, "propertyImage.image.url", ""),
            "neighborhood": _deep_get(r, "neighborhood.name", ""),
        })
    return hotels


# ── Priceline ────────────────────────────────────────────────

def _parse_priceline(extracted: dict) -> list[dict]:
    """Priceline GraphQL → hotel list."""
    raw = _safe_json(extracted.get("graphql_response"))
    if not isinstance(raw, dict):
        return []

    results = _deep_get(raw, "data.getHotelSearchResults.results.hotels", [])
    if not results:
        results = _deep_get(raw, "data.hotelListings.listings", [])
    if not isinstance(results, list):
        return []

    hotels = []
    for r in results:
        name = _deep_get(r, "name", "") or _deep_get(r, "hotelName", "")
        if not name:
            continue
        hotels.append({
            "hotel_name": name,
            "star_rating": _deep_get(r, "starRating", 0),
            "guest_rating": _deep_get(r, "overallGuestRating", 0),
            "review_count": _deep_get(r, "totalReviews", 0),
            "price": _deep_get(r, "ratesSummary.minPrice", 0),
            "currency": _deep_get(r, "ratesSummary.minCurrencyCode", "USD"),
            "address": _deep_get(r, "location.address.addressLine1", ""),
            "city": _deep_get(r, "location.address.cityName", ""),
            "image_url": _deep_get(r, "thumbnailUrl", ""),
        })
    return hotels


# ── Expedia ──────────────────────────────────────────────────

def _parse_expedia(extracted: dict) -> list[dict]:
    """Expedia — extract from search page HTML or GraphQL."""
    # Try GraphQL first
    raw = _safe_json(extracted.get("graphql_response"))
    if isinstance(raw, dict):
        results = _deep_get(raw, "data.propertySearch.properties", [])
        if isinstance(results, list) and results:
            hotels = []
            for r in results:
                name = _deep_get(r, "name", "")
                if not name:
                    continue
                hotels.append({
                    "hotel_name": name,
                    "star_rating": _deep_get(r, "star", 0),
                    "guest_rating": _deep_get(r, "reviews.score", 0),
                    "review_count": _deep_get(r, "reviews.total", 0),
                    "price_per_night": _deep_get(r, "price.lead.amount", 0),
                    "currency": _deep_get(r, "price.lead.currencyInfo.code", "USD"),
                    "image_url": _deep_get(r, "propertyImage.image.url", ""),
                })
            return hotels

    # Fallback: parse HTML search page
    html = extracted.get("search_page", "")
    if not isinstance(html, str) or len(html) < 1000:
        return []
    return _parse_html_json_state(html, "expedia")


# ── Generic HTML parser (shared by many browser-based providers) ──

def _parse_html_json_state(html: str, provider: str) -> list[dict]:
    """
    Extract hotels from embedded JSON/JS state in HTML pages.
    Many travel sites embed their data in <script> tags as JSON.
    """
    hotels = []
    
    # Try to find JSON-LD structured data
    ld_matches = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
    for ld in ld_matches:
        try:
            data = json.loads(ld)
            if isinstance(data, dict) and data.get("@type") == "Hotel":
                hotels.append({
                    "hotel_name": data.get("name", ""),
                    "star_rating": _deep_get(data, "starRating.ratingValue", 0),
                    "guest_rating": _deep_get(data, "aggregateRating.ratingValue", 0),
                    "review_count": _deep_get(data, "aggregateRating.reviewCount", 0),
                    "address": _deep_get(data, "address.streetAddress", ""),
                    "city": _deep_get(data, "address.addressLocality", ""),
                    "image_url": data.get("image", ""),
                })
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") in ("Hotel", "LodgingBusiness"):
                        hotels.append({
                            "hotel_name": item.get("name", ""),
                            "star_rating": _deep_get(item, "starRating.ratingValue", 0),
                            "address": _deep_get(item, "address.streetAddress", ""),
                            "city": _deep_get(item, "address.addressLocality", ""),
                        })
        except (json.JSONDecodeError, ValueError):
            continue

    if hotels:
        return hotels

    # Try to find __NEXT_DATA__ or similar embedded state
    next_data_match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if next_data_match:
        try:
            data = json.loads(next_data_match.group(1))
            # Traverse common paths for hotel data
            props = _deep_get(data, "props.pageProps", {})
            if isinstance(props, dict):
                for key in ["results", "hotels", "properties", "listings", "searchResults"]:
                    items = props.get(key, [])
                    if isinstance(items, list) and items:
                        for item in items[:50]:
                            if isinstance(item, dict):
                                name = item.get("name", item.get("hotel_name", item.get("title", "")))
                                if name:
                                    hotels.append({
                                        "hotel_name": str(name),
                                        "price": item.get("price", item.get("rate", 0)),
                                        "star_rating": item.get("stars", item.get("star_rating", 0)),
                                        "guest_rating": item.get("rating", item.get("score", 0)),
                                    })
                        if hotels:
                            return hotels
        except (json.JSONDecodeError, ValueError):
            pass

    # Try generic JSON blob extraction
    json_blobs = re.findall(r'"hotelName"\s*:\s*"([^"]+)"', html)
    if json_blobs:
        for name in json_blobs[:30]:
            hotels.append({"hotel_name": name})
        return hotels

    return hotels


# ── Kayak / Momondo / Cheapflights (same parent company) ─────

def _parse_kayak(extracted: dict) -> list[dict]:
    html = extracted.get("search_page", "")
    if isinstance(html, str) and len(html) > 500:
        return _parse_html_json_state(html, "kayak")
    return []


# ── Trivago ──────────────────────────────────────────────────

def _parse_trivago(extracted: dict) -> list[dict]:
    html = extracted.get("search_page", "")
    if isinstance(html, str) and len(html) > 500:
        return _parse_html_json_state(html, "trivago")
    return []


# ── Skyscanner ───────────────────────────────────────────────

def _parse_skyscanner(extracted: dict) -> list[dict]:
    html = extracted.get("search_page", "")
    if isinstance(html, str) and len(html) > 500:
        return _parse_html_json_state(html, "skyscanner")
    return []


# ── Generic parser (fallback for all providers) ──────────────

def _parse_generic(extracted: dict) -> list[dict]:
    """Generic parser: try pre-parsed hotel list, then HTML extraction."""
    # Check if Lambda already extracted hotels
    hotels = extracted.get("hotels")
    if isinstance(hotels, list) and hotels:
        return hotels

    # Check for GraphQL/API response
    for key in ["graphql_response", "api_response"]:
        raw = _safe_json(extracted.get(key))
        if isinstance(raw, dict):
            # Try common paths
            for path in ["data.results", "data.hotels", "data.properties",
                         "data.searchResults", "results", "hotels", "properties"]:
                items = _deep_get(raw, path, [])
                if isinstance(items, list) and items:
                    return items

    # Try HTML parsing from search_page
    for key in ["search_page", "_search_page"]:
        html = extracted.get(key, "")
        if isinstance(html, str) and len(html) > 1000:
            result = _parse_html_json_state(html, "generic")
            if result:
                return result

    return []


# ── Hotellook ─────────────────────────────────────────────────

def _parse_hotellook(extracted: dict) -> list[dict]:
    """Hotellook cache API → hotel list."""
    raw = _safe_json(extracted.get("api_response"))
    if isinstance(raw, list):
        hotels = []
        for h in raw:
            if not isinstance(h, dict):
                continue
            name = h.get("hotelName", "")
            if not name:
                continue
            hotels.append({
                "hotel_name": name,
                "star_rating": h.get("stars", 0),
                "guest_rating": h.get("rating", 0),
                "review_count": h.get("reviews", 0),
                "price": h.get("priceFrom", 0),
                "price_per_night": h.get("priceAvg", 0),
                "image_url": h.get("photoUrl", ""),
                "latitude": h.get("latitude", 0),
                "longitude": h.get("longitude", 0),
                "address": h.get("address", ""),
            })
        return hotels
    return []


# ── Hostelworld ───────────────────────────────────────────────

def _parse_hostelworld(extracted: dict) -> list[dict]:
    """Hostelworld browser extraction → hotel list."""
    raw = _safe_json(extracted.get("hotels_raw") or extracted.get("hotels"))
    if isinstance(raw, list):
        return [h for h in raw if isinstance(h, dict) and h.get("hotel_name")]
    return []


# ═══════════════════════════════════════════════════════════════
# PARSER REGISTRY
# ═══════════════════════════════════════════════════════════════

PARSERS = {
    "booking_com": _parse_booking_com,
    "agoda": _parse_agoda,
    "trip_com": _parse_trip_com,
    "hotels_com": _parse_hotels_com,
    "priceline": _parse_priceline,
    "expedia": _parse_expedia,
    "kayak_hotels": _parse_kayak,
    "cheapflights_hotels": _parse_kayak,       # same as kayak
    "momondo_hotels": _parse_kayak,             # same as kayak
    "trivago": _parse_trivago,
    "skyscanner_hotels": _parse_skyscanner,
    "orbitz": _parse_expedia,                   # same as expedia (Expedia Group)
    "hotelscombined": _parse_kayak,             # same as kayak (Kayak Group)
    "hotellook": _parse_hotellook,
    "hostelworld": _parse_hostelworld,
}
