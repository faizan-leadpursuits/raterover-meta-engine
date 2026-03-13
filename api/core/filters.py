"""Hotel search result filters."""


def apply_filters(
    hotels: list[dict],
    min_price: float | None = None,
    max_price: float | None = None,
    min_stars: int | None = None,
    max_stars: int | None = None,
    min_rating: float | None = None,
    free_cancel: bool = False,
    sort_by: str = "price",
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    """Apply filters, sorting, and pagination to hotel results."""
    filtered = hotels

    if min_price is not None:
        filtered = [h for h in filtered if h.get("price", 0) >= min_price]
    if max_price is not None:
        filtered = [h for h in filtered if 0 < h.get("price", 0) <= max_price]
    if min_stars is not None:
        filtered = [h for h in filtered if h.get("star_rating", 0) >= min_stars]
    if max_stars is not None:
        filtered = [h for h in filtered if h.get("star_rating", 0) <= max_stars]
    if min_rating is not None:
        filtered = [h for h in filtered if h.get("guest_rating", 0) >= min_rating]
    if free_cancel:
        filtered = [h for h in filtered if h.get("cancellation", "")]

    # Sort
    sort_keys = {
        "price": lambda h: h.get("price", 999999),
        "price_desc": lambda h: -h.get("price", 0),
        "rating": lambda h: -h.get("guest_rating", 0),
        "stars": lambda h: -h.get("star_rating", 0),
        "reviews": lambda h: -h.get("review_count", 0),
        "name": lambda h: h.get("hotel_name", "").lower(),
    }
    sort_fn = sort_keys.get(sort_by, sort_keys["price"])
    filtered.sort(key=sort_fn)

    # Paginate
    total = len(filtered)
    filtered = filtered[offset:offset + limit]

    return filtered, total
