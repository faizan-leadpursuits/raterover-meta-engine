"""
Hotel Search Engine — ScraperFlow Lambda Integration.

This module replaces the local scrapers with parallel ScraperFlow Lambda calls.
It loads JSON payloads from 'working_payloads/', fills templates with 
search parameters, and invokes the Lambda function.
"""

import os
import json
import time
import logging
import re
import asyncio
import boto3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from datetime import datetime

logger = logging.getLogger("api.engine")

# ── Config ──
WORKING_PAYLOADS_DIR = Path(__file__).resolve().parent.parent.parent / "working_payloads"
LAMBDA_FUNCTION = os.getenv("SCRAPERFLOW_LAMBDA", "scraperflow-engine-dev")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
PROVIDER_TIMEOUT = int(os.getenv("PROVIDER_TIMEOUT", "60"))
SEARCH_TIMEOUT = int(os.getenv("SEARCH_TIMEOUT", "90"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "10"))

_lambda_client = None

def get_lambda_client():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    return _lambda_client

# ── Provider Discovery ──

def get_provider_names() -> list[str]:
    """Return list of providers based on available JSON payloads."""
    if not WORKING_PAYLOADS_DIR.exists():
        logger.error("Working payloads directory not found: %s", WORKING_PAYLOADS_DIR)
        return []
    return sorted([f.stem for f in WORKING_PAYLOADS_DIR.glob("*.json")])

def get_enabled_providers() -> list[str]:
    """All payloads in working_payloads are considered enabled."""
    return get_provider_names()

def _provider_display(name: str) -> str:
    mapping = {
        "booking": "Booking.com",
        "agoda": "Agoda",
        "trip": "Trip.com",
        "priceline": "Priceline",
        "cleartrip": "Cleartrip",
        "wego": "Wego",
        "traveloka": "Traveloka",
        "hostelworld": "Hostelworld"
    }
    return mapping.get(name, name.replace("_", " ").title())

# ── Template Engine ──

def fill_template(obj, variables: dict):
    """Recursively replace {{var}} placeholders in any JSON structure."""
    if isinstance(obj, str):
        def replacer(match):
            key = match.group(1)
            # Support both {{var}} and {{var_lower}} etc
            if key.endswith("_lower"):
                base_key = key[:-6]
                val = str(variables.get(base_key, "")).lower()
            elif key.endswith("_upper"):
                base_key = key[:-6]
                val = str(variables.get(base_key, "")).upper()
            else:
                val = variables.get(key, match.group(0))
            return str(val)
        return re.sub(r"\{\{(\w+)\}\}", replacer, obj)
    elif isinstance(obj, dict):
        return {k: fill_template(v, variables) for k, v in obj.items() if k != "_comment"}
    elif isinstance(obj, list):
        return [fill_template(item, variables) for item in obj]
    return obj

def fix_numeric_types(obj):
    """Convert string numbers back to actual numbers for specific keys."""
    NUMERIC_KEYS = {
        "adults", "rooms", "limit", "timeout", "city_id", "dest_id", 
        "nbRooms", "nbAdults", "pageSize", "pageIndex", "numberOfAdults"
    }
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            v = fix_numeric_types(v)
            if k in NUMERIC_KEYS and isinstance(v, str):
                try: v = int(v) if "." not in v else float(v)
                except: pass
            result[k] = v
        return result
    elif isinstance(obj, list):
        return [fix_numeric_types(item) for item in obj]
    return obj

# ── Lambda Invocation ──

def invoke_provider(
    provider_name: str,
    city: str,
    check_in: str,
    check_out: str,
    adults: int = 2,
    rooms: int = 1,
    currency: str = "USD",
    extra_vars: dict | None = None,
    **kwargs,
) -> dict:
    """Invoke ScraperFlow Lambda for a specific provider."""
    t0 = time.time()
    payload_path = WORKING_PAYLOADS_DIR / f"{provider_name}.json"
    
    if not payload_path.exists():
        return {"status": "error", "hotels": [], "count": 0, "time_ms": 0, "error": f"Payload not found: {provider_name}"}

    try:
        with open(payload_path, "r") as f:
            template = json.load(f)

        # Prepare variables
        vars = {
            "city": city,
            "check_in": check_in,
            "check_out": check_out,
            "adults": adults,
            "rooms": rooms,
            "currency": currency,
            "check_in_compact": check_in.replace("-", ""),
            "check_out_compact": check_out.replace("-", ""),
            "check_in_dots": check_in.replace("-", "."),
            "check_out_dots": check_out.replace("-", "."),
        }
        if extra_vars:
            vars.update(extra_vars)
            # Specific mappings for consistency across payloads
            if "agoda_city_id" in extra_vars: vars["city_id"] = extra_vars["agoda_city_id"]
            if "trip_city_id" in extra_vars: vars["city_id"] = extra_vars["trip_city_id"]

        # Run template engine
        concrete_payload = fill_template(template, vars)
        concrete_payload = fix_numeric_types(concrete_payload)

        # Invoke Lambda
        client = get_lambda_client()
        response = client.invoke(
            FunctionName=LAMBDA_FUNCTION,
            InvocationType="RequestResponse",
            Payload=json.dumps(concrete_payload)
        )
        
        res_payload = json.loads(response["Payload"].read())
        elapsed_ms = int((time.time() - t0) * 1000)

        if "errors" in res_payload and res_payload["errors"]:
            error_msg = res_payload["errors"][0].get("message", "Unknown Lambda error")
            return {"status": "error", "hotels": [], "count": 0, "time_ms": elapsed_ms, "error": error_msg}

        hotels = res_payload.get("extracted", {}).get("hotels", [])
        return {
            "status": "success",
            "hotels": hotels,
            "count": len(hotels),
            "time_ms": elapsed_ms,
            "error": None
        }

    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        logger.error("[%s] Lambda invocation failed: %s", provider_name, e)
        return {"status": "error", "hotels": [], "count": 0, "time_ms": elapsed_ms, "error": str(e)}

# ── Normalization ──

def normalize_hotel(raw: dict, provider: str, city: str, check_in: str, check_out: str, currency: str) -> dict | None:
    """Ensure consistent output format across all providers."""
    name = raw.get("hotel_name") or raw.get("name")
    if not name: return None

    try:
        ci = datetime.strptime(check_in, "%Y-%m-%d")
        co = datetime.strptime(check_out, "%Y-%m-%d")
        nights = max(1, (co - ci).days)
    except: nights = 1

    # Extract price (handle strings like "$123")
    price_raw = str(raw.get("price", "0"))
    price = 0.0
    price_match = re.search(r"(\d+[\d,.]*)", price_raw.replace(",", ""))
    if price_match:
        price = float(price_match.group(1))

    return {
        "source": provider,
        "hotel_name": name,
        "hotel_address": raw.get("hotel_address") or raw.get("address") or "",
        "city": raw.get("city", city),
        "star_rating": int(raw.get("star_rating") or raw.get("stars") or 0),
        "guest_rating": float(raw.get("guest_rating", 0)),
        "review_count": int(raw.get("review_count", 0)),
        "price": price,
        "price_per_night": round(price / nights, 2) if nights > 0 else price,
        "currency": currency,
        "image_url": raw.get("image_url", ""),
        "deep_link": raw.get("deep_link", ""),
        "booking_provider": _provider_display(provider)
    }

# ── Batch Search ──

def search_all_providers(city: str, check_in: str, check_out: str, adults: int = 2, rooms: int = 1, currency: str = "USD", sources: list[str] | None = None, extra_vars: dict | None = None) -> dict:
    all_providers = get_provider_names()
    target_providers = [s for s in (sources or all_providers) if s in all_providers]
    
    if not target_providers:
        return {"status": "ok", "total": 0, "hotels": [], "search_time_ms": 0, "providers_queried": 0, "providers_with_results": 0, "provider_results": []}

    t0 = time.time()
    all_hotels = []
    provider_results = []

    def _run_one(p):
        return p, invoke_provider(p, city, check_in, check_out, adults, rooms, currency, extra_vars=extra_vars)

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = {executor.submit(_run_one, p): p for p in target_providers}
        for future in as_completed(futures):
            p_name, res = future.result()
            normalized = []
            for h in res.get("hotels", []):
                n = normalize_hotel(h, p_name, city, check_in, check_out, currency)
                if n: normalized.append(n)
            
            provider_results.append({
                "provider": p_name,
                "status": res["status"],
                "count": len(normalized),
                "time_ms": res["time_ms"],
                "error": res.get("error")
            })
            all_hotels.extend(normalized)

    all_hotels.sort(key=lambda h: h["price"])
    total_ms = int((time.time() - t0) * 1000)
    
    return {
        "status": "ok",
        "total": len(all_hotels),
        "hotels": all_hotels,
        "search_time_ms": total_ms,
        "providers_queried": len(target_providers),
        "providers_with_results": sum(1 for r in provider_results if r["count"] > 0),
        "provider_results": provider_results
    }
