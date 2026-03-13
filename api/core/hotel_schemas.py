"""
Common schema definitions for hotel search providers.
Defines the universal output columns that every hotel adapter must produce.
"""

# ═══════════════════════════════════════════════════════════════
# UNIVERSAL HOTEL OUTPUT SCHEMA
# ═══════════════════════════════════════════════════════════════
# Every hotel adapter must return a DataFrame with (at minimum) these columns.
# Extra columns are allowed but will be dropped during merge.

HOTEL_COMMON_COLUMNS = [
    "source",           # Provider name (e.g. "kayak_hotels")
    "hotel_name",       # Hotel display name
    "hotel_address",    # Full address
    "city",             # City name
    "country",          # Country code or name
    "latitude",         # GPS latitude (float)
    "longitude",        # GPS longitude (float)
    "star_rating",      # 1-5 stars (0 = unrated)
    "guest_rating",     # Guest review score (0-10 scale)
    "review_count",     # Number of reviews (int)
    "check_in",         # Check-in date (YYYY-MM-DD)
    "check_out",        # Check-out date (YYYY-MM-DD)
    "nights",           # Number of nights (int)
    "room_type",        # Room type description
    "board_type",       # Room only, Breakfast, Half board, All inclusive
    "price",            # Total price for the stay (float)
    "price_per_night",  # Price per night (float)
    "currency",         # Currency code (e.g. "USD")
    "booking_provider", # OTA/provider offering this price (e.g. "Booking.com")
    "cancellation",     # Free cancellation / Non-refundable / etc.
    "amenities",        # Comma-separated amenities
    "deep_link",        # Booking URL
    "image_url",        # Hotel image URL
    "raw_data",         # JSON string of original data (optional)
]


def make_hotel_row(**kwargs):
    """
    Create a standardized hotel result row.

    Ensures all HOTEL_COMMON_COLUMNS are present with defaults.
    """
    row = {col: "" for col in HOTEL_COMMON_COLUMNS}
    row["star_rating"] = 0
    row["guest_rating"] = 0.0
    row["review_count"] = 0
    row["nights"] = 1
    row["price"] = 0.0
    row["price_per_night"] = 0.0
    row["latitude"] = 0.0
    row["longitude"] = 0.0
    row.update(kwargs)
    return row
