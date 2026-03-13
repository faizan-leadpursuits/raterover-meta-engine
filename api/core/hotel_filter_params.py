"""
Unified hotel filter parameters and per-provider translation.

HotelFilterParams is the single source of truth for user-facing filters.
Each provider translator converts these into site-specific URL parameters.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import quote


@dataclass
class HotelFilterParams:
    """Unified hotel search filters accepted by all providers."""

    # ── Star rating ──
    star_rating: List[int] = field(default_factory=list)
    """Filter by hotel star class (e.g. [4, 5] for 4- and 5-star only)."""

    # ── Guest rating ──
    min_guest_rating: Optional[float] = None
    """Minimum guest review score (0-10 scale). E.g. 8.0 for "Very good+"."""

    # ── Price ──
    max_price: Optional[float] = None
    """Maximum total price for the stay."""
    max_price_per_night: Optional[float] = None
    """Maximum price per night."""

    # ── Cancellation / Meals ──
    free_cancellation: bool = False
    """Only show properties with free cancellation."""
    breakfast_included: bool = False
    """Only show properties that include breakfast."""

    # ── Amenities ──
    amenities: List[str] = field(default_factory=list)
    """Filter by amenities. Supported keys:
       wifi, pool, parking, fitness, spa, pet_friendly,
       restaurant, air_conditioning, kitchen, elevator"""

    # ── Property type ──
    property_type: Optional[str] = None
    """Property type filter. Options: hotel, apartment, hostel, resort,
       bed_and_breakfast, guest_house, villa"""

    # ── Sorting ──
    sort_by: str = "price"
    """Sort results by: price, rating, reviews, distance"""

    # ── Min reviews ──
    min_reviews: Optional[int] = None
    """Minimum number of reviews."""

    def is_empty(self) -> bool:
        """True if no filters are set."""
        return (
            not self.star_rating
            and self.min_guest_rating is None
            and self.max_price is None
            and self.max_price_per_night is None
            and not self.free_cancellation
            and not self.breakfast_included
            and not self.amenities
            and self.property_type is None
            and self.sort_by == "price"
            and self.min_reviews is None
        )


# ══════════════════════════════════════════════════════════════
# BOOKING.COM FILTER TRANSLATION
# Uses nflt= parameter with ;-separated key=value pairs
# ══════════════════════════════════════════════════════════════

_BOOKING_PROPERTY_TYPES = {
    "hotel": "204", "apartment": "201", "hostel": "203",
    "resort": "206", "bed_and_breakfast": "208",
    "guest_house": "214", "villa": "213",
}

_BOOKING_AMENITIES = {
    "wifi": "hotelfacility=107",
    "pool": "hotelfacility=11",
    "parking": "hotelfacility=2",
    "fitness": "hotelfacility=11",  # gym
    "spa": "hotelfacility=54",
    "restaurant": "hotelfacility=5",
    "pet_friendly": "hotelfacility=4",
    "air_conditioning": "hotelfacility=75",
    "kitchen": "hotelfacility=999",  # kitchen/self-catering
    "elevator": "hotelfacility=72",
}


def booking_filter_nflt(params: HotelFilterParams) -> str:
    """Convert HotelFilterParams → Booking.com nflt query string value."""
    parts = []

    # Star rating
    for s in sorted(params.star_rating):
        if 1 <= s <= 5:
            parts.append(f"class={s}")

    # Guest rating (Booking uses score × 10: 90, 80, 70, 60)
    if params.min_guest_rating:
        score = int(params.min_guest_rating * 10)
        # Round down to nearest Booking bucket
        if score >= 90:
            parts.append("review_score=90")
        elif score >= 80:
            parts.append("review_score=80")
        elif score >= 70:
            parts.append("review_score=70")
        elif score >= 60:
            parts.append("review_score=60")

    # Free cancellation
    if params.free_cancellation:
        parts.append("fc=2")

    # Breakfast
    if params.breakfast_included:
        parts.append("mealplan=1")

    # Amenities
    for a in params.amenities:
        key = a.lower().replace(" ", "_")
        if key in _BOOKING_AMENITIES:
            parts.append(_BOOKING_AMENITIES[key])

    # Property type
    if params.property_type:
        pt = params.property_type.lower().replace(" ", "_")
        if pt in _BOOKING_PROPERTY_TYPES:
            parts.append(f"ht_id={_BOOKING_PROPERTY_TYPES[pt]}")

    # Price (Booking uses: price=CURRENCY-min-max-1)
    if params.max_price_per_night:
        parts.append(f"price=USD-0-{int(params.max_price_per_night)}-1")

    return ";".join(parts)


def booking_sort_param(params: HotelFilterParams) -> str:
    """Convert sort_by → Booking.com order= value."""
    return {
        "price": "price",
        "rating": "review_score_and_price",
        "reviews": "review_score_and_price",
        "distance": "distance",
    }.get(params.sort_by, "price")


# ══════════════════════════════════════════════════════════════
# KAYAK / MOMONDO FILTER TRANSLATION
# Uses fs= parameter with ;-separated key=value pairs
# ══════════════════════════════════════════════════════════════

_KAYAK_AMENITIES = {
    "wifi": "wifi", "pool": "pool", "parking": "parking",
    "fitness": "fitness", "spa": "spa", "restaurant": "restaurant",
    "pet_friendly": "petfriendly", "air_conditioning": "ac",
    "kitchen": "kitchenette", "elevator": "elevator",
}

_KAYAK_PROPERTY_TYPES = {
    "hotel": "hotels", "apartment": "rentals",
    "hostel": "hotels", "resort": "hotels",
}


def kayak_filter_fs(params: HotelFilterParams) -> str:
    """Convert HotelFilterParams → Kayak/Momondo fs query string value."""
    parts = []

    # Star rating
    if params.star_rating:
        stars_str = ",".join(str(s) for s in sorted(params.star_rating))
        parts.append(f"stars={stars_str}")

    # Guest rating (Kayak uses score × 10)
    if params.min_guest_rating:
        score = int(params.min_guest_rating * 10)
        parts.append(f"reviewscore={score}")

    # Freebies
    freebies = []
    if params.free_cancellation:
        freebies.append("freecancellation")
    if params.breakfast_included:
        freebies.append("freebreakfast")
    if freebies:
        parts.append(f"freebies={','.join(freebies)}")

    # Amenities
    amenity_keys = []
    for a in params.amenities:
        key = a.lower().replace(" ", "_")
        if key in _KAYAK_AMENITIES:
            amenity_keys.append(_KAYAK_AMENITIES[key])
    if amenity_keys:
        parts.append(f"amenities={','.join(amenity_keys)}")

    # Property type
    if params.property_type:
        pt = params.property_type.lower().replace(" ", "_")
        if pt in _KAYAK_PROPERTY_TYPES:
            parts.append(f"propertytype={_KAYAK_PROPERTY_TYPES[pt]}")

    # Price
    if params.max_price_per_night:
        parts.append(f"price=-{int(params.max_price_per_night)}")

    return ";".join(parts)


def kayak_sort_param(params: HotelFilterParams) -> str:
    """Convert sort_by → Kayak sort= value."""
    return {
        "price": "price_a",
        "rating": "rating_a",
        "reviews": "rating_a",
        "distance": "distance_a",
    }.get(params.sort_by, "price_a")


# ══════════════════════════════════════════════════════════════
# TRIVAGO FILTER TRANSLATION
# Uses category code pairs in the search= path segment
# ══════════════════════════════════════════════════════════════

_TRIVAGO_STARS = {
    5: "105-1322", 4: "105-1320", 3: "105-1318",
    2: "105-1316", 1: "105-1314",
}

_TRIVAGO_RATINGS = {
    9.0: "106-1529",   # Excellent
    8.5: "106-1528",   # Excellent
    8.0: "106-1527",   # Very good
    7.0: "106-1525",   # Good
}

_TRIVAGO_PROPERTY_TYPES = {
    "hotel": "101-2", "bed_and_breakfast": "101-3",
    "apartment": "101-4", "hostel": "101-5",
    "resort": "101-6", "guest_house": "101-7",
    "villa": "101-8",
}

_TRIVAGO_AMENITIES = {
    "wifi": "300-732", "pool": "300-502", "parking": "300-586",
    "spa": "300-508", "fitness": "300-504",
    "restaurant": "300-518", "air_conditioning": "300-510",
    "pet_friendly": "300-512", "elevator": "300-516",
}


def trivago_filter_codes(params: HotelFilterParams) -> list:
    """Convert HotelFilterParams → list of Trivago category code strings."""
    codes = []

    # Star rating
    for s in sorted(params.star_rating):
        if s in _TRIVAGO_STARS:
            codes.append(_TRIVAGO_STARS[s])

    # Guest rating (pick closest bucket)
    if params.min_guest_rating:
        r = params.min_guest_rating
        for threshold in sorted(_TRIVAGO_RATINGS.keys(), reverse=True):
            if r >= threshold:
                codes.append(_TRIVAGO_RATINGS[threshold])
                break

    # Free cancellation
    if params.free_cancellation:
        codes.append("412-1")

    # Breakfast
    if params.breakfast_included:
        codes.append("411-2")

    # Amenities
    for a in params.amenities:
        key = a.lower().replace(" ", "_")
        if key in _TRIVAGO_AMENITIES:
            codes.append(_TRIVAGO_AMENITIES[key])

    # Property type
    if params.property_type:
        pt = params.property_type.lower().replace(" ", "_")
        if pt in _TRIVAGO_PROPERTY_TYPES:
            codes.append(_TRIVAGO_PROPERTY_TYPES[pt])

    return codes


def trivago_sort_param(params: HotelFilterParams) -> str:
    """Convert sort_by → Trivago sort code."""
    return {
        "price": "1",     # Cheapest first
        "rating": "5",    # Best rated
        "reviews": "5",   # Best rated (same as rating)
        "distance": "4",  # Distance
    }.get(params.sort_by, "1")
