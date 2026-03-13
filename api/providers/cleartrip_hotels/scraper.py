"""
Cleartrip Hotel Search Scraper
==============================
High-speed scraper fetching hotel data from Cleartrip's SSR.
Controlled entirely via hotels_config.json.
"""

import json
import re
import time
import csv
import sys
import uuid
from typing import Optional
from curl_cffi import requests as cffi_requests
from pathlib import Path
CONFIG_FILE = Path(__file__).parent / "config.json"
AUTOSUGGEST_URL = "https://www.cleartrip.com/prefixy/ui/autoSuggest/getSuggestions"

BASE_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "upgrade-insecure-requests": "1",
}

class CleartripHotelScraper:
    BASE_URL = "https://www.cleartrip.com"

    def __init__(self, proxy: Optional[str] = None):
        self.session = cffi_requests.Session(verify=False, impersonate="chrome142")
        self.session.headers.update(BASE_HEADERS)
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    @staticmethod
    def _slugify(text: str) -> str:
        text = re.sub(r"[^a-zA-Z0-9]+", "-", str(text or "").strip().lower()).strip("-")
        return text or ""

    def _autocomplete_headers(self) -> dict:
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "channel": "desktop",
            "content-type": "application/json",
            "origin": "https://www.cleartrip.com",
            "priority": "u=1, i",
            "referer": "https://www.cleartrip.com/hotels",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "x-client-id": "cleartrip",
            "x-source-type": "Desktop",
            "x-unified-header": json.dumps({
                "platform": "desktop",
                "trackingId": str(uuid.uuid4()),
                "source": "CLEARTRIP"
            }),
        }

    def resolve_city_slugs(self, query: str) -> tuple[str, str] | None:
        q = str(query or "").strip().lower()
        if not q:
            return None
        try:
            payload = {"prefix": q, "useCaseContext": "HOTEL_HOME_PAGE"}
            resp = self.session.post(
                AUTOSUGGEST_URL,
                headers=self._autocomplete_headers(),
                json=payload,
                timeout=20,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            suggestions = (
                data.get("suggestions")
                or (data.get("data") or {}).get("suggestions")
                or data.get("result")
                or []
            )
            if not suggestions:
                return None

            top = None
            for item in suggestions:
                if str(item.get("suggestionType") or "").upper() == "CITY" and bool(item.get("clickable", True)):
                    top = item
                    break
            if not top:
                top = suggestions[0]

            city_name = top.get("cityName") or top.get("entityName") or query
            country_name = top.get("countryName") or top.get("country") or "india"
            city_slug = self._slugify(city_name)
            country_slug = self._slugify(country_name)
            if city_slug and country_slug:
                print(f"[*] City resolved: '{query}' -> /hotels/{country_slug}/{city_slug}/")
                return country_slug, city_slug
        except Exception as e:
            print(f"[!] City autocomplete failed for '{query}': {e}")
        return None

    def search(self, cfg: dict) -> list[dict]:
        currency = cfg.get("currency", "USD")
        params = {
            "checkIn": cfg.get("checkin", ""),
            "checkOut": cfg.get("checkout", ""),
            "adults1": str(cfg.get("adults", 2)),
            "children1": str(cfg.get("children", 0)),
            "numRooms": str(cfg.get("rooms", 1)),
            "curr": currency,
        }

        country_slug = cfg.get("country_slug", "india")
        city_slug = cfg.get("city_slug", "new-delhi")
        city_query = cfg.get("city") or city_slug
        resolved = self.resolve_city_slugs(city_query)
        if resolved:
            country_slug, city_slug = resolved

        search_url = f"{self.BASE_URL}/hotels/{country_slug}/{city_slug}/"
        print(f"[*] Fetching SSR Data: {search_url} (CheckIn: {cfg.get('checkin')}, Currency: {currency})")

        resp = self.session.get(search_url, params=params, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            print(f"[!] HTTP {resp.status_code}. Failed to fetch.")
            return []

        match = re.search(r'__NEXT_DATA__.*?>(.*?)</script>', resp.text)
        if not match:
            print("[!] Could not find __NEXT_DATA__ payload in SSR HTML.")
            return []

        try:
            next_data = json.loads(match.group(1))
            return self._extract_hotels_from_slots(next_data, currency)
        except json.JSONDecodeError:
            print("[!] Failed to parse JSON.")
            return []

    def _extract_hotels_from_slots(self, next_data: dict, currency: str = "USD") -> list[dict]:
        state = (next_data.get('props', {})
                 .get('pageProps', {})
                 .get('initialState', {})
                 .get('results', {})
                 .get('data', {}))
        
        all_hotels = []
        for slot in state.get('slotsData', []):
            if slot.get('slotData', {}).get('type') == 'HOTEL_CARD_LIST':
                all_hotels.extend(slot.get('slotData', {}).get('data', {}).get('hotelCardList', []))

        # Dynamic Currency Conversion (Strategy 1)
        target_currency = currency.strip().upper()
        base_currency = "INR"
        exchange_rate = 1.0

        currency_map = {
            "₹": "INR",
            "AED": "AED",
            "$": "USD",
            "£": "GBP",
            "€": "EUR"
        }

        # Detect base currency from first hotel in payload (SSR forces local currency)
        for raw in all_hotels:
            p_info = raw.get('data', {}).get('priceInfo', {}).get('data', {})
            if p_info:
                sym = p_info.get('currencySymbol', '₹')
                base_currency = currency_map.get(sym, 'INR')
                break

        if base_currency != target_currency:
            print(f"[*] Fetching live exchange rate {base_currency} -> {target_currency}...")
            try:
                rate_resp = cffi_requests.get(f"https://api.exchangerate-api.com/v4/latest/{base_currency}", verify=False, timeout=5)
                exchange_rate = rate_resp.json().get("rates", {}).get(target_currency, 1.0)
                print(f"[*] Exchange rate: 1 {base_currency} = {exchange_rate} {target_currency}")
            except Exception as e:
                print(f"[!] Failed to fetch exchange rate, falling back to 1.0. Error: {e}")

        parsed_hotels = []
        for raw in all_hotels:
            info = raw.get('data', {})
            name = info.get('name') or raw.get('ravenTracking', {}).get('eventData', {}).get('h_hotel_name', 'Unknown')
            p_info = info.get('priceInfo', {}).get('data', {})
            
            base_price = p_info.get('price', 0)
            base_slashed = p_info.get('slashedPrice', 0)

            converted_price = round(base_price * exchange_rate, 2) if base_price else 0
            converted_slashed = round(base_slashed * exchange_rate, 2) if base_slashed else 0

            parsed_hotels.append({
                "name": name,
                "stars": float(info.get('starRating')) if info.get('starRating') else None,
                "user_rating": float(info.get('clearTripRatingAndReview', {}).get('data', {}).get('rating', 0)) or None,
                "price": converted_price,
                "slashed_price": converted_slashed,
                "currency": target_currency,
                "free_breakfast": info.get('freeBreakfast', {}).get('available', False),
                "free_cancellation": info.get('freeCancellation', {}).get('available', False),
                "locality": info.get('locality', ''),
                "city": info.get('city', ''),
            })
        return parsed_hotels

def export_results(results: list[dict], export_fmt: str, output_path: str):
    if export_fmt == "json":
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
    elif export_fmt == "csv":
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=results[0].keys())
            w.writeheader()
            w.writerows(results)
    print(f"[✓] Exported successfully to {output_path}")

def main():
    import os
    config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILE)
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            # Depending on if it's the new standard wrapper or the old flat one
            cfg_data = json.load(f)
            cfg = cfg_data.get("default_params", cfg_data)
    except FileNotFoundError:
        print(f"[!] {config_path} not found. Please create it.")
        sys.exit(1)

    t0 = time.perf_counter()
    scraper = CleartripHotelScraper(proxy=cfg.get("proxy"))
    hotels = scraper.search(cfg)
    
    elapsed = time.perf_counter() - t0
    
    if not hotels:
        print("[!] No hotels found.")
        return
        
    print(f"\n[✓] Found {len(hotels)} hotels in {elapsed:.2f}s")
    
    # Show preview
    print("-" * 80)
    for i, h in enumerate(hotels[:5], 1):
        score = f"{h['user_rating']}/5" if h['user_rating'] else "N/A"
        print(f" {i:2d}. {h['name'][:35]:<35} | {h['currency']}{h['price']} (Score: {score})")
    print("-" * 80)

    # Export
    out_path = cfg.get("output", f"cleartrip_hotels.{cfg.get('export', 'json')}")
    export_results(hotels, cfg.get("export", "json"), out_path)

if __name__ == "__main__":
    main()


"""Cleartrip Hotels provider wrapper using hotels_scraper.py (SSR + curl_cffi)."""



import logging
from datetime import datetime


logger = logging.getLogger(__name__)


class CleartripHotelsScraper:
    """Adapter-facing scraper wrapper built on CleartripHotelScraper."""

    @staticmethod
    def _nights(check_in: str, check_out: str) -> int:
        try:
            ci = datetime.strptime(check_in, "%Y-%m-%d")
            co = datetime.strptime(check_out, "%Y-%m-%d")
            return max(1, (co - ci).days)
        except Exception:
            return 1

    def _normalize_rows(self, raw_rows: list[dict], city: str, check_in: str, check_out: str, currency: str) -> list[dict]:
        nights = self._nights(check_in, check_out)
        rows: list[dict] = []

        for item in raw_rows or []:
            try:
                price = float(item.get("price", 0) or 0)
            except Exception:
                price = 0.0
            if price <= 0:
                continue

            try:
                stars = int(float(item.get("stars", 0) or 0))
            except Exception:
                stars = 0
            try:
                rating = float(item.get("user_rating", 0) or 0)
            except Exception:
                rating = 0.0

            cancellation = "Free cancellation" if bool(item.get("free_cancellation")) else ""
            board = "Breakfast included" if bool(item.get("free_breakfast")) else ""
            locality = str(item.get("locality", "") or "").strip()
            row_city = str(item.get("city", city) or city).strip()
            address = ", ".join([x for x in (locality, row_city) if x])

            rows.append(
                {
                    "hotel_name": str(item.get("name", "") or "").strip(),
                    "hotel_address": address,
                    "city": row_city,
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "star_rating": max(0, min(stars, 5)),
                    "guest_rating": rating,
                    "review_count": 0,
                    "check_in": check_in,
                    "check_out": check_out,
                    "nights": nights,
                    "room_type": "",
                    "board_type": board,
                    "price": price,
                    "price_per_night": round(price / nights, 2) if nights else price,
                    "currency": str(item.get("currency", currency) or currency),
                    "booking_provider": "Cleartrip",
                    "cancellation": cancellation,
                    "amenities": "",
                    "deep_link": "",
                    "image_url": "",
                }
            )

        rows.sort(key=lambda r: float(r.get("price", 0) or 0))
        return rows

    def search(
        self,
        city,
        check_in,
        check_out,
        adults=2,
        rooms=1,
        currency="USD",
        proxy=None,
        proxy_manager=None,
        **kw,
    ):
        logger.info("[cleartrip_hotels] Mode: hotels_scraper.py SSR + curl_cffi")

        try:
            from core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure
        except Exception:
            from metasearch.core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure

        proxy_configs = build_curl_cffi_proxy_list(proxy_manager=proxy_manager, proxy=proxy)
        if not proxy_configs:
            proxy_configs = [("none", None)]
        elif all(purl is not None for _, purl in proxy_configs):
            proxy_configs.append(("direct_fallback", None))

        cfg = {
            "city": city,
            "checkin": check_in,
            "checkout": check_out,
            "adults": int(adults or 2),
            "children": 0,
            "rooms": int(rooms or 1),
            "currency": currency,
        }

        for proxy_name, proxy_url in proxy_configs:
            logger.info("[cleartrip_hotels] Trying proxy: %s", proxy_name)
            try:
                scraper = CleartripHotelScraper(proxy=proxy_url)
                raw = scraper.search(dict(cfg))
                rows = self._normalize_rows(raw, city=city, check_in=check_in, check_out=check_out, currency=currency)
                if rows:
                    logger.info("[cleartrip_hotels] Parsed %d hotels via %s", len(rows), proxy_name)
                    return rows
            except Exception as exc:
                if is_proxy_failure(str(exc)):
                    logger.warning("[cleartrip_hotels] %s failed: %s", proxy_name, str(exc)[:120])
                    continue
                logger.error("[cleartrip_hotels] search failed via %s: %s", proxy_name, exc, exc_info=True)

        return []
