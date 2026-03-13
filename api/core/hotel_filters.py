"""
Post-search filter engine for hotel results.
Applies client-side filters after results have been collected and merged.
"""

import pandas as pd


def apply_hotel_filters(
    df: pd.DataFrame,
    max_price: float = None,
    max_price_per_night: float = None,
    min_stars: int = None,
    max_stars: int = None,
    min_rating: float = None,
    min_reviews: int = None,
    board_type: str = None,
    free_cancellation_only: bool = False,
    include_amenities: list = None,
    sort_by: str = "price",
) -> pd.DataFrame:
    """
    Apply post-search filters to merged hotel results.

    Args:
        df: Merged DataFrame with HOTEL_COMMON_COLUMNS
        max_price: Maximum total price for the stay
        max_price_per_night: Maximum price per night
        min_stars: Minimum star rating (1-5)
        max_stars: Maximum star rating (1-5)
        min_rating: Minimum guest rating (0-10)
        min_reviews: Minimum number of reviews
        board_type: Filter by board type (e.g. "Breakfast", "All inclusive")
        free_cancellation_only: Only show free cancellation options
        include_amenities: Only show hotels with ALL of these amenities
        sort_by: Sort field — "price", "price_per_night", "rating", "stars", "reviews"

    Returns:
        Filtered and sorted DataFrame
    """
    if df.empty:
        return df

    # ── Max total price ──
    if max_price is not None and "price" in df.columns:
        df = df[df["price"] <= max_price]

    # ── Max price per night ──
    if max_price_per_night is not None and "price_per_night" in df.columns:
        df = df[df["price_per_night"] <= max_price_per_night]

    # ── Star rating range ──
    if min_stars is not None and "star_rating" in df.columns:
        df = df[df["star_rating"] >= min_stars]
    if max_stars is not None and "star_rating" in df.columns:
        df = df[df["star_rating"] <= max_stars]

    # ── Guest rating ──
    if min_rating is not None and "guest_rating" in df.columns:
        df = df[df["guest_rating"] >= min_rating]

    # ── Minimum reviews ──
    if min_reviews is not None and "review_count" in df.columns:
        df = df[df["review_count"] >= min_reviews]

    # ── Board type ──
    if board_type and "board_type" in df.columns:
        board_lower = board_type.lower()
        df = df[df["board_type"].str.lower().str.contains(board_lower, na=False)]

    # ── Free cancellation ──
    if free_cancellation_only and "cancellation" in df.columns:
        df = df[df["cancellation"].str.lower().str.contains("free", na=False)]

    # ── Amenities filter ──
    if include_amenities and "amenities" in df.columns:
        for amenity in include_amenities:
            amenity_lower = amenity.lower()
            df = df[df["amenities"].str.lower().str.contains(amenity_lower, na=False)]

    # ── Sort ──
    sort_map = {
        "price": ("price", True),
        "price_per_night": ("price_per_night", True),
        "rating": ("guest_rating", False),
        "stars": ("star_rating", False),
        "reviews": ("review_count", False),
    }
    sort_col, ascending = sort_map.get(sort_by, ("price", True))
    if sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=ascending, na_position="last")

    return df.reset_index(drop=True)
