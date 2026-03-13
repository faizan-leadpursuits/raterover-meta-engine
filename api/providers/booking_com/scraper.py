"""
Booking.com hotel scraper (curl_cffi only).

Flow:
1) Warm session cookies via Booking homepage/search page.
2) Resolve destination id/type (static map + search page fallback parsing).
3) Query Booking GraphQL endpoint with pagination.
4) Normalize to internal hotel row schema.

No Playwright/browser automation is used.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from math import inf
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse

from curl_cffi.requests import Session

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent / "config.json"

try:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as _f:
        _cfg = json.load(_f)
except FileNotFoundError:
    _cfg = {}

_REQ = _cfg.get("request", {})
IMPERSONATE = _REQ.get("impersonate_browser", "chrome120")
USER_AGENT = _REQ.get(
    "user_agent",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
PAGE_DELAY = float(_REQ.get("delay_between_pages_seconds", 1.0) or 1.0)

BASE_URL = "https://www.booking.com"
_GRAPHQL_URL = f"{BASE_URL}/dml/graphql"

# Common city mappings to avoid an extra resolve request.
CITY_DEST_IDS = {
    "london": ("-2601889", "city"),
    "paris": ("-1456928", "city"),
    "new york": ("20088325", "city"),
    "dubai": ("-782831", "city"),
    "tokyo": ("-246227", "city"),
    "rome": ("-126693", "city"),
    "barcelona": ("-372490", "city"),
    "istanbul": ("-755070", "city"),
    "bangkok": ("-3414440", "city"),
    "amsterdam": ("-2140479", "city"),
    "berlin": ("-1746443", "city"),
    "madrid": ("-390625", "city"),
    "singapore": ("-73635", "city"),
    "lisbon": ("-2167973", "city"),
    "prague": ("-553173", "city"),
    "vienna": ("-1995499", "city"),
    "milan": ("-121726", "city"),
    "los angeles": ("20014181", "city"),
    "chicago": ("20033173", "city"),
    "miami": ("20023181", "city"),
    "san francisco": ("20015732", "city"),
    "sydney": ("-1603135", "city"),
    "melbourne": ("-1596893", "city"),
    "toronto": ("-574890", "city"),
    "vancouver": ("-575268", "city"),
    "hong kong": ("-1353149", "city"),
    "seoul": ("-716583", "city"),
    "cairo": ("-290692", "city"),
    "mumbai": ("-2092174", "city"),
}

HEADERS_COMMON = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "apollographql-client-name": "b-search-web-searchresults",
    "apollographql-client-version": "aYYKSObc",
    "content-type": "application/json",
    "origin": BASE_URL,
    "referer": f"{BASE_URL}/searchresults.html",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": USER_AGENT,
}

GRAPHQL_QUERY = """
query FullSearch($input: SearchQueryInput!) {
  searchQueries {
    search(input: $input) {
      ... on SearchQueryOutput {
        pagination {
          nbResultsPerPage
          nbResultsTotal
        }
        results {
          displayName {
            text
          }
          basicPropertyData {
            id
            pageName
            accommodationTypeId
            isClosed
            starRating {
              value
              symbol
            }
            reviews {
              totalScore
              showScore
              secondaryScore
              secondaryTextTag {
                translation
              }
              totalScoreTextTag {
                translation
              }
              reviewsCount
            }
            location {
              address
              city
              countryCode
            }
            photos {
              main {
                highResUrl {
                  relativeUrl
                }
              }
            }
          }
          priceDisplayInfoIrene {
            displayPrice {
              amountPerStay {
                amountRounded
                amountUnformatted
                currency
              }
            }
          }
          blocks {
            freeCancellationUntil
          }
        }
      }
    }
  }
}
"""


def _proxy_dict(proxy_url: str | None) -> dict[str, str] | None:
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    txt = str(v).strip().replace(",", "")
    m = re.search(r"\d+(?:\.\d+)?", txt)
    return float(m.group(0)) if m else 0.0


def _to_int(v: Any) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0


class BookingComScraper:
    BASE_URL = BASE_URL

    def __init__(self):
        self.rows_per_page = int((_cfg.get("search") or {}).get("rows_per_page", 25) or 25)
        self.max_pages_default = int((_cfg.get("search") or {}).get("max_pages", 20) or 20)

    @staticmethod
    def _normalize_city_key(city: str) -> str:
        return str(city or "").strip().lower()

    def _resolve_dest_static(self, city: str) -> tuple[str | None, str | None]:
        key = self._normalize_city_key(city)
        if key in CITY_DEST_IDS:
            return CITY_DEST_IDS[key]
        for name, val in CITY_DEST_IDS.items():
            if key in name or name in key:
                return val
        return None, None

    def _resolve_dest_online(self, session: Session, city: str, check_in: str, check_out: str, adults: int, rooms: int, currency: str) -> tuple[str | None, str | None]:
        params = {
            "ss": city,
            "checkin": check_in,
            "checkout": check_out,
            "group_adults": adults,
            "group_children": 0,
            "no_rooms": rooms,
            "selected_currency": currency,
        }
        url = f"{BASE_URL}/searchresults.html?{urlencode(params)}"
        resp = session.get(url, headers=HEADERS_COMMON, timeout=30, allow_redirects=True)

        # 1) Parse final URL query
        try:
            q = parse_qs(urlparse(str(resp.url)).query)
            dest_id = (q.get("dest_id") or [None])[0]
            dest_type = (q.get("dest_type") or [None])[0]
            if dest_id and dest_type:
                return str(dest_id), str(dest_type)
        except Exception:
            pass

        # 2) Parse HTML
        html = resp.text or ""
        patterns = [
            r'data-dest-id="([^"]+)"[^>]*data-dest-type="([^"]+)"',
            r'"dest_id"\s*:\s*"?(-?\d+)"?\s*,\s*"dest_type"\s*:\s*"?([a-zA-Z_]+)"?',
            r'"destType"\s*:\s*"([a-zA-Z_]+)"\s*,\s*"destId"\s*:\s*"?(-?\d+)"?',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if not m:
                continue
            if pat.startswith('"destType"'):
                return m.group(2), m.group(1)
            return m.group(1), m.group(2)

        return None, None

    def _build_search_url(self, city: str, check_in: str, check_out: str, adults: int, rooms: int, currency: str, dest_id: str | None, dest_type: str | None, filters=None) -> str:
        params = {
            "ss": city,
            "checkin": check_in,
            "checkout": check_out,
            "group_adults": adults,
            "group_children": 0,
            "no_rooms": rooms,
            "selected_currency": currency,
        }
        if dest_id:
            params["dest_id"] = dest_id
        if dest_type:
            params["dest_type"] = dest_type

        if filters:
            try:
                from core.hotel_filter_params import booking_filter_nflt, booking_sort_param

                nflt = booking_filter_nflt(filters)
                if nflt:
                    params["nflt"] = nflt
                params["order"] = booking_sort_param(filters)
            except Exception:
                pass

        return f"{BASE_URL}/searchresults.html?{urlencode(params)}"

    def _build_api_url(self, city: str, check_in: str, check_out: str, adults: int, rooms: int, dest_id: str | None, dest_type: str | None) -> str:
        # Keep API URL compatible with both city and airport searches.
        dt = str(dest_type or "").strip().lower()
        if dt not in {"city", "airport", "district", "landmark"}:
            dt = str((_cfg.get("search") or {}).get("dest_type") or "city").strip().lower()
            if dt not in {"city", "airport", "district", "landmark"}:
                dt = "city"
        params = {
            "ss": city,
            "dest_id": str(dest_id or (_cfg.get("search") or {}).get("dest_id") or ""),
            "dest_type": dt,
            "checkin": check_in,
            "checkout": check_out,
            "group_adults": adults,
            "no_rooms": rooms,
        }
        return f"{_GRAPHQL_URL}?{urlencode(params)}"

    def _build_payload(self, city: str, check_in: str, check_out: str, adults: int, rooms: int, dest_id: str | None, dest_type: str | None, offset: int, currency: str) -> dict:
        # GraphQL expects enum-style destination type values.
        payload_dest_type = str(dest_type or (_cfg.get("search") or {}).get("dest_type") or "CITY").strip().upper()
        if payload_dest_type not in {"CITY", "AIRPORT", "DISTRICT", "LANDMARK"}:
            payload_dest_type = "CITY"
        payload_dest_id = _to_int(
            dest_id if dest_id is not None else (_cfg.get("search") or {}).get("dest_id")
        )
        return {
            "operationName": "FullSearch",
            "variables": {
                "input": {
                    "dates": {
                        "checkin": check_in,
                        "checkout": check_out,
                    },
                    "location": {
                        "searchString": city,
                        "destType": payload_dest_type,
                        "destId": payload_dest_id,
                    },
                    "nbRooms": rooms,
                    "nbAdults": adults,
                    "nbChildren": 0,
                    "pagination": {
                        "rowsPerPage": self.rows_per_page,
                        "offset": offset,
                    },
                    "travelPurpose": 2,
                    "useSearchParamsFromSession": True,
                }
            },
            "query": GRAPHQL_QUERY,
        }

    def _parse_results(self, results: list[dict], city: str, check_in: str, check_out: str, default_currency: str) -> list[dict]:
        try:
            ci = datetime.strptime(check_in, "%Y-%m-%d")
            co = datetime.strptime(check_out, "%Y-%m-%d")
            nights = max(1, (co - ci).days)
        except Exception:
            nights = 1

        out: list[dict] = []
        for item in results:
            if not isinstance(item, dict):
                continue

            prop = item.get("basicPropertyData", {}) or {}
            display = item.get("displayName", {}) or {}
            price_info = (item.get("priceDisplayInfoIrene", {}) or {}).get("displayPrice", {}) or {}
            amount = price_info.get("amountPerStay", {}) or {}

            name = str(display.get("text") or prop.get("name") or "").strip()
            if not name:
                continue

            # Prefer rounded value first (same behavior as booking_scraper.py).
            total_price = _to_float(amount.get("amountRounded") or amount.get("amountUnformatted"))
            if total_price <= 0:
                continue

            currency = str(amount.get("currency") or default_currency)
            loc = prop.get("location", {}) or {}
            reviews = prop.get("reviews", {}) or {}
            star = (prop.get("starRating") or {}).get("value", 0)

            page_name = str(prop.get("pageName") or "").strip()
            deep_link = f"{BASE_URL}/hotel/{page_name}.html" if page_name else ""
            rel_img = (((prop.get("photos") or {}).get("main") or {}).get("highResUrl") or {}).get("relativeUrl")
            image_url = f"https://cf.bstatic.com{rel_img}" if rel_img else ""

            cancellation = ""
            for block in item.get("blocks") or []:
                if isinstance(block, dict) and block.get("freeCancellationUntil"):
                    cancellation = "Free cancellation"
                    break

            out.append(
                {
                    "hotel_name": name,
                    "hotel_address": str(loc.get("address") or ""),
                    "city": str(loc.get("city") or city),
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "star_rating": _to_int(star),
                    "guest_rating": _to_float(reviews.get("totalScore")),
                    "review_count": _to_int(reviews.get("reviewsCount")),
                    "check_in": check_in,
                    "check_out": check_out,
                    "nights": nights,
                    "room_type": "",
                    "board_type": "",
                    "price": total_price,
                    "price_per_night": round(total_price / nights, 2) if nights else total_price,
                    "currency": currency,
                    "booking_provider": "Booking.com",
                    "cancellation": cancellation,
                    "amenities": "",
                    "deep_link": deep_link,
                    "image_url": image_url,
                }
            )

        return out

    def _search_once(self, city: str, check_in: str, check_out: str, adults: int, rooms: int, currency: str, proxy_url: str | None, filters=None, max_results: int = 0) -> list[dict]:
        session = Session(impersonate=IMPERSONATE)
        px = _proxy_dict(proxy_url)
        if px:
            session.proxies = px

        # Warm cookies
        try:
            session.get(
                f"{BASE_URL}/",
                headers={
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "accept-language": "en-US,en;q=0.9",
                    "upgrade-insecure-requests": "1",
                    "user-agent": USER_AGENT,
                },
                timeout=20,
            )
        except Exception:
            pass

        dest_id, dest_type = self._resolve_dest_static(city)
        if not dest_id:
            dest_id, dest_type = self._resolve_dest_online(session, city, check_in, check_out, adults, rooms, currency)

        search_url = self._build_search_url(city, check_in, check_out, adults, rooms, currency, dest_id, dest_type, filters=filters)
        logger.info("[booking_com] Opening %s", search_url)
        try:
            session.get(search_url, headers=HEADERS_COMMON, timeout=30, allow_redirects=True)
        except Exception:
            pass

        api_url = self._build_api_url(city, check_in, check_out, adults, rooms, dest_id, dest_type)
        page = 0
        offset = 0
        all_rows: list[dict] = []
        total_results = None
        max_pages = self.max_pages_default
        limit = max_results if max_results and max_results > 0 else inf

        while page < max_pages and len(all_rows) < limit:
            payload = self._build_payload(
                city=city,
                check_in=check_in,
                check_out=check_out,
                adults=adults,
                rooms=rooms,
                dest_id=dest_id,
                dest_type=dest_type,
                offset=offset,
                currency=currency,
            )
            resp = session.post(api_url, headers=HEADERS_COMMON, json=payload, timeout=30)
            if resp.status_code != 200:
                body = (resp.text or "")[:280].replace("\n", " ")
                logger.warning("[booking_com] GraphQL HTTP %s (offset=%d) body=%s", resp.status_code, offset, body)
                break

            data = resp.json()
            if isinstance(data, dict) and data.get("errors"):
                first = data.get("errors", [{}])[0]
                msg = str(first.get("message") or "")[:220]
                code = str((first.get("extensions") or {}).get("code") or "")[:80]
                logger.warning(
                    "[booking_com] GraphQL errors at offset=%d msg=%s code=%s",
                    offset,
                    msg,
                    code,
                )
                break

            try:
                search_data = data["data"]["searchQueries"]["search"]
                results = search_data.get("results", [])
                pagination = search_data.get("pagination", {}) or {}
                total_results = _to_int(pagination.get("nbResultsTotal")) or total_results
            except Exception:
                logger.warning("[booking_com] Unexpected GraphQL schema at offset=%d", offset)
                break

            if not results:
                break

            parsed = self._parse_results(results, city, check_in, check_out, currency)
            all_rows.extend(parsed)

            if len(results) < self.rows_per_page:
                break
            if total_results and len(all_rows) >= total_results:
                break

            page += 1
            offset += self.rows_per_page
            if PAGE_DELAY > 0:
                time.sleep(PAGE_DELAY)

        # Deduplicate by hotel name, keep cheapest row.
        best: dict[str, dict] = {}
        for row in all_rows:
            key = row.get("hotel_name", "").strip().lower()
            if not key:
                continue
            old = best.get(key)
            if old is None or _to_float(row.get("price")) < _to_float(old.get("price")):
                best[key] = row

        rows = list(best.values())
        rows.sort(key=lambda r: _to_float(r.get("price", inf)))
        if max_results and max_results > 0:
            rows = rows[:max_results]
        logger.info("[booking_com] Parsed %d hotels", len(rows))
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
        filters=None,
        **kw,
    ):
        max_results = int(kw.get("max_results", 0) or 0)

        try:
            from core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure
        except Exception:
            from metasearch.core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure

        proxy_configs = build_curl_cffi_proxy_list(proxy_manager=proxy_manager, proxy=proxy)
        if not proxy_configs:
            proxy_configs = [("none", None)]
        elif all(purl is not None for _, purl in proxy_configs):
            proxy_configs.append(("direct_fallback", None))

        for proxy_name, proxy_url in proxy_configs:
            logger.info("[booking_com] Mode: curl_cffi GraphQL (no browser), proxy=%s", proxy_name)
            try:
                rows = self._search_once(
                    city=city,
                    check_in=check_in,
                    check_out=check_out,
                    adults=adults,
                    rooms=rooms,
                    currency=currency,
                    proxy_url=proxy_url,
                    filters=filters,
                    max_results=max_results,
                )
                if rows:
                    return rows
            except Exception as exc:
                if is_proxy_failure(str(exc)):
                    logger.warning("[booking_com] %s failed: %s", proxy_name, str(exc)[:120])
                    continue
                logger.error("[booking_com] search failed via %s: %s", proxy_name, exc, exc_info=True)

        return []


if __name__ == "__main__":
    scraper = BookingComScraper()
    rows = scraper.search(
        city="London",
        check_in="2026-04-08",
        check_out="2026-04-12",
        adults=2,
        rooms=1,
        currency="USD",
    )
    print(rows)
