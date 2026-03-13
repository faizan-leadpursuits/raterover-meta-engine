"""
Hotel Search API Routes — All endpoints for the hotel metasearch API.

Endpoints:
  GET  /v2/hotels/search          — Multi-provider parallel search
  GET  /v2/hotels/cheapest        — Single cheapest hotel
  GET  /v2/hotels/compare         — Compare one hotel across providers
  GET  /v2/hotels/deals           — Cross-provider price deals
  GET  /v2/hotels/autocomplete    — City/destination typeahead
  GET  /v2/hotels/providers       — List all providers
  POST /v2/hotels/search/batch    — Multi-city batch search
"""

import asyncio
import json
import logging
import re
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from core.engine import (
    get_provider_names,
    invoke_provider,
    search_all_providers,
)
from core.filters import apply_filters
from core.city_resolver import autocomplete as city_autocomplete, resolve_all_ids

logger = logging.getLogger("api.routes.hotels")
router = APIRouter(prefix="/v2/hotels", tags=["Hotels"])

# ── Validation ───────────────────────────────────────────────

_VALID_CURRENCIES = {
    "USD", "EUR", "GBP", "AED", "AUD", "CAD", "CHF", "CNY", "DKK",
    "HKD", "INR", "JPY", "KRW", "MXN", "NOK", "NZD", "PKR", "PLN",
    "SAR", "SEK", "SGD", "THB", "TRY", "ZAR",
}

# Provider aliases (friendly names → payload names)
_PROVIDER_ALIASES = {
    "booking": "booking", "booking.com": "booking", "bookingcom": "booking",
    "trip": "trip", "trip.com": "trip",
    "cleartrip": "cleartrip",
    "wego": "wego",
    "agoda": "agoda",
    "priceline": "priceline",
    "traveloka": "traveloka",
    "hostelworld": "hostelworld"
}



def _validate_dates(check_in: str, check_out: str):
    try:
        ci = datetime.strptime(check_in, "%Y-%m-%d")
        co = datetime.strptime(check_out, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Dates must be YYYY-MM-DD format")
    if ci >= co:
        raise HTTPException(400, "check_out must be after check_in")
    if ci.date() < datetime.now().date():
        raise HTTPException(400, "check_in cannot be in the past")
    if (co - ci).days > 30:
        raise HTTPException(400, "Maximum stay is 30 nights")
    return ci, co


def _validate_currency(currency: str) -> str:
    c = currency.upper().strip()
    if c not in _VALID_CURRENCIES:
        raise HTTPException(400, f"Unsupported currency: {c}")
    return c


def _normalize_sources(sources: str | None) -> list[str] | None:
    if not sources:
        return None
    all_providers = set(get_provider_names())
    result = []
    for s in sources.split(","):
        s = s.strip().lower()
        canonical = _PROVIDER_ALIASES.get(s, s)
        if canonical in all_providers:
            result.append(canonical)
    return result if result else None


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 1: Search
# ═══════════════════════════════════════════════════════════════

@router.get("/search", summary="Search hotels across all providers")
async def search_hotels(
    request: Request,
    city: str = Query(..., min_length=2, description="City name"),
    check_in: str = Query(..., description="Check-in date YYYY-MM-DD"),
    check_out: str = Query(..., description="Check-out date YYYY-MM-DD"),
    adults: int = Query(2, ge=1, le=9),
    rooms: int = Query(1, ge=1, le=5),
    currency: str = Query("USD"),
    sources: Optional[str] = Query(None, description="Comma-separated provider names"),
    min_price: Optional[float] = Query(None, ge=0),
    max_price: Optional[float] = Query(None, ge=0),
    min_stars: Optional[int] = Query(None, ge=1, le=5),
    max_stars: Optional[int] = Query(None, ge=1, le=5),
    min_rating: Optional[float] = Query(None, ge=0, le=10),
    free_cancel: bool = Query(False),
    sort_by: str = Query("price", description="Sort: price, rating, stars, reviews"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    _validate_dates(check_in, check_out)
    currency = _validate_currency(currency)
    source_list = _normalize_sources(sources)

    # Resolve provider-specific city IDs (Agoda city_id, Booking dest_id, etc.)
    city_ids = await resolve_all_ids(city)

    # Run search in thread pool to not block event loop
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: search_all_providers(
            city=city, check_in=check_in, check_out=check_out,
            adults=adults, rooms=rooms, currency=currency,
            sources=source_list, extra_vars=city_ids,
        ),
    )

    hotels = result.get("hotels", [])
    filtered, total_filtered = apply_filters(
        hotels,
        min_price=min_price, max_price=max_price,
        min_stars=min_stars, max_stars=max_stars,
        min_rating=min_rating, free_cancel=free_cancel,
        sort_by=sort_by, limit=limit, offset=offset,
    )

    return {
        "status": "ok",
        "search_id": str(uuid.uuid4()),
        "city": city,
        "check_in": check_in,
        "check_out": check_out,
        "adults": adults,
        "rooms": rooms,
        "currency": currency,
        "results": filtered,
        "total": total_filtered,
        "total_before_filters": result.get("total", 0),
        "providers_queried": result.get("providers_queried", 0),
        "providers_with_results": result.get("providers_with_results", 0),
        "provider_details": [
            {
                "name": r["provider"],
                "status": r["status"],
                "count": r["count"],
                "time_ms": r["time_ms"],
                "error": r.get("error"),
            }
            for r in result.get("provider_results", [])
        ],
        "search_time_ms": result.get("search_time_ms", 0),
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 2: Cheapest Hotel
# ═══════════════════════════════════════════════════════════════

@router.get("/cheapest", summary="Find the single cheapest hotel")
async def cheapest_hotel(
    request: Request,
    city: str = Query(...),
    check_in: str = Query(...),
    check_out: str = Query(...),
    adults: int = Query(2, ge=1, le=9),
    rooms: int = Query(1, ge=1, le=5),
    currency: str = Query("USD"),
    min_stars: Optional[int] = Query(None, ge=1, le=5),
    sources: Optional[str] = Query(None),
):
    _validate_dates(check_in, check_out)
    currency = _validate_currency(currency)
    city_ids = await resolve_all_ids(city)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: search_all_providers(
            city=city, check_in=check_in, check_out=check_out,
            adults=adults, rooms=rooms, currency=currency,
            sources=_normalize_sources(sources), extra_vars=city_ids,
        ),
    )

    hotels = result.get("hotels", [])
    if min_stars:
        hotels = [h for h in hotels if h.get("star_rating", 0) >= min_stars]

    # Filter to only hotels with a valid price > 0
    priced = [h for h in hotels if h.get("price", 0) > 0]
    if not priced:
        return {"status": "ok", "cheapest": None, "message": "No hotels found with valid prices"}

    cheapest = min(priced, key=lambda h: h["price"])
    return {
        "status": "ok",
        "cheapest": cheapest,
        "total_compared": len(priced),
        "providers_queried": result.get("providers_queried", 0),
        "search_time_ms": result.get("search_time_ms", 0),
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 3: Compare hotel across providers
# ═══════════════════════════════════════════════════════════════

@router.get("/compare", summary="Compare one hotel across all providers")
async def compare_hotel(
    request: Request,
    hotel_name: str = Query(..., description="Hotel name to compare"),
    city: str = Query(...),
    check_in: str = Query(...),
    check_out: str = Query(...),
    adults: int = Query(2, ge=1, le=9),
    rooms: int = Query(1, ge=1, le=5),
    currency: str = Query("USD"),
):
    _validate_dates(check_in, check_out)
    currency = _validate_currency(currency)
    city_ids = await resolve_all_ids(city)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: search_all_providers(
            city=city, check_in=check_in, check_out=check_out,
            adults=adults, rooms=rooms, currency=currency,
            extra_vars=city_ids,
        ),
    )

    # Fuzzy match hotel name
    query = _normalize_name(hotel_name)
    matches = []
    for h in result.get("hotels", []):
        if query in _normalize_name(h.get("hotel_name", "")):
            matches.append(h)

    if not matches:
        return {"status": "ok", "hotel_name": hotel_name, "matches": [], "message": "No matches found"}

    # Group by provider
    matches.sort(key=lambda h: h.get("price", 999999))
    cheapest = matches[0]["price"] if matches else 0

    return {
        "status": "ok",
        "hotel_name": hotel_name,
        "matches": matches,
        "cheapest_price": cheapest,
        "cheapest_provider": matches[0]["source"] if matches else None,
        "providers_found": len(set(m["source"] for m in matches)),
        "search_time_ms": result.get("search_time_ms", 0),
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 4: Deals (cross-provider price comparison)
# ═══════════════════════════════════════════════════════════════

@router.get("/deals", summary="Find price deals across providers")
async def hotel_deals(
    request: Request,
    city: str = Query(...),
    check_in: str = Query(...),
    check_out: str = Query(...),
    adults: int = Query(2, ge=1, le=9),
    rooms: int = Query(1, ge=1, le=5),
    currency: str = Query("USD"),
    min_savings: float = Query(0, ge=0, le=100, description="Min savings % to include"),
    limit: int = Query(20, ge=1, le=100),
):
    _validate_dates(check_in, check_out)
    currency = _validate_currency(currency)
    city_ids = await resolve_all_ids(city)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: search_all_providers(
            city=city, check_in=check_in, check_out=check_out,
            adults=adults, rooms=rooms, currency=currency,
            extra_vars=city_ids,
        ),
    )

    deals = _find_deals(result.get("hotels", []), min_savings)
    return {
        "status": "ok",
        "city": city,
        "check_in": check_in,
        "check_out": check_out,
        "deals": deals[:limit],
        "total_deals": len(deals),
        "total_hotels_analyzed": result.get("total", 0),
        "search_time_ms": result.get("search_time_ms", 0),
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 5: Autocomplete
# ═══════════════════════════════════════════════════════════════

@router.get("/autocomplete", summary="City/destination autocomplete")
async def autocomplete_endpoint(
    query: str = Query(..., min_length=2, max_length=100),
    limit: int = Query(10, ge=1, le=20),
):
    """Search for cities using Agoda's autocomplete API."""
    results = await city_autocomplete(query, limit)
    return {"status": "ok", "suggestions": results, "query": query}


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 6: Providers list
# ═══════════════════════════════════════════════════════════════

@router.get("/providers", summary="List all available providers")
async def list_providers():
    providers = get_provider_names()
    return {
        "status": "ok",
        "count": len(providers),
        "providers": [
            {"name": p, "display_name": _provider_display(p)}
            for p in providers
        ],
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 7: Batch search (multi-city)
# ═══════════════════════════════════════════════════════════════

@router.post("/search/batch", summary="Search multiple cities in one request")
async def batch_search(request: Request):
    body = await request.json()
    cities = body.get("cities", [])
    check_in = body.get("check_in", "")
    check_out = body.get("check_out", "")
    adults = body.get("adults", 2)
    rooms = body.get("rooms", 1)
    currency = body.get("currency", "USD")
    sources = body.get("sources")

    if not cities or not check_in or not check_out:
        raise HTTPException(400, "cities, check_in, and check_out are required")
    if len(cities) > 5:
        raise HTTPException(400, "Maximum 5 cities per batch")

    _validate_dates(check_in, check_out)
    currency = _validate_currency(currency)
    source_list = _normalize_sources(sources) if sources else None

    loop = asyncio.get_event_loop()
    start = time.time()

    async def search_city(city: str):
        city_ids = await resolve_all_ids(city)
        return await loop.run_in_executor(
            None,
            lambda: search_all_providers(
                city=city, check_in=check_in, check_out=check_out,
                adults=adults, rooms=rooms, currency=currency,
                sources=source_list, extra_vars=city_ids,
            ),
        )

    tasks = [search_city(city) for city in cities]
    results = await asyncio.gather(*tasks)

    city_results = {}
    for city, result in zip(cities, results):
        city_results[city] = {
            "hotels": result.get("hotels", []),
            "total": result.get("total", 0),
            "providers_queried": result.get("providers_queried", 0),
        }

    return {
        "status": "ok",
        "cities": city_results,
        "total_time_ms": int((time.time() - start) * 1000),
    }


# ═══════════════════════════════════════════════════════════════
# ENDPOINT 8: SSE Streaming
# ═══════════════════════════════════════════════════════════════

@router.get("/search/stream", summary="Stream results as they arrive (SSE)")
async def search_stream(
    request: Request,
    city: str = Query(...),
    check_in: str = Query(...),
    check_out: str = Query(...),
    adults: int = Query(2, ge=1, le=9),
    rooms: int = Query(1, ge=1, le=5),
    currency: str = Query("USD"),
    sources: Optional[str] = Query(None),
):
    _validate_dates(check_in, check_out)
    currency = _validate_currency(currency)
    source_list = _normalize_sources(sources)
    all_providers = source_list or get_provider_names()

    async def event_generator():
        loop = asyncio.get_event_loop()

        city_ids = await resolve_all_ids(city)

        for provider in all_providers:
            try:
                result = await loop.run_in_executor(
                    None,
                    lambda p=provider: invoke_provider(
                        p, city, check_in, check_out, adults, rooms, currency,
                        extra_vars=city_ids,
                    ),
                )
                from core.engine import normalize_hotel
                hotels = []
                for h in result.get("hotels", []):
                    n = normalize_hotel(h, provider, city, check_in, check_out, currency)
                    if n:
                        hotels.append(n)

                event = {
                    "provider": provider,
                    "status": result["status"],
                    "count": len(hotels),
                    "hotels": hotels[:20],
                    "time_ms": result["time_ms"],
                }
                yield f"data: {json.dumps(event)}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'provider': provider, 'status': 'error', 'error': str(e)[:200]})}\n\n"

        yield f"data: {json.dumps({'event': 'done'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _find_deals(hotels: list[dict], min_savings_pct: float = 0) -> list[dict]:
    """Group hotels by fuzzy name and find price differences across providers."""
    groups = defaultdict(list)
    for h in hotels:
        key = _normalize_name(h.get("hotel_name", ""))
        if key and h.get("price", 0) > 0:
            groups[key].append(h)

    deals = []
    for key, entries in groups.items():
        if len(entries) < 2:
            continue
        entries.sort(key=lambda x: x["price"])
        cheapest = entries[0]
        most_expensive = entries[-1]
        savings_pct = round((1 - cheapest["price"] / most_expensive["price"]) * 100, 1)

        if savings_pct >= min_savings_pct:
            deals.append({
                "hotel_name": cheapest["hotel_name"],
                "cheapest_price": cheapest["price"],
                "cheapest_provider": cheapest["source"],
                "most_expensive_price": most_expensive["price"],
                "most_expensive_provider": most_expensive["source"],
                "savings_pct": savings_pct,
                "savings_amount": round(most_expensive["price"] - cheapest["price"], 2),
                "currency": cheapest.get("currency", "USD"),
                "star_rating": cheapest.get("star_rating", 0),
                "guest_rating": cheapest.get("guest_rating", 0),
                "providers": len(entries),
                "all_prices": [
                    {"provider": e["source"], "price": e["price"]}
                    for e in entries
                ],
            })

    deals.sort(key=lambda d: -d["savings_pct"])
    return deals


def _provider_display(name: str) -> str:
    from core.engine import _provider_display
    return _provider_display(name)
