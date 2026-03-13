"""
Priceline Hotels Scraper.
Uses curl_cffi to bypass Cloudflare/PerimeterX bot protection without Playwright.
Flow: 1) Hit homepage to harvest cookies  2) Fetch paginated GraphQL API concurrently.
"""
from __future__ import annotations

import asyncio
import sys

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except AttributeError:
        pass

import json
import logging
import random
import uuid
from pathlib import Path
from urllib.parse import quote

from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent / "config.json"

BASE_URL = "https://www.priceline.com"
_GRAPHQL_URL = f"{BASE_URL}/pws/v0/pcln-graph/?gqlOp=getStandaloneHotelListings"

# ---------------------------------------------------------------------------
# Browser profiles for rotation
# ---------------------------------------------------------------------------
_BROWSER_PROFILES = [
    {
        "impersonate": "chrome120",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "impersonate": "chrome124",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "impersonate": "chrome131",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
]

_GQL_HEADERS_BASE = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "apollographql-client-name": "relax-ui-browser",
    "apollographql-client-version": "main-0.0.136",
    "content-type": "application/json",
    "origin": BASE_URL,
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

_GQL_QUERY = """
fragment filters on HotelListingSearchSummary {
  filters(filterOptions: $filterOptions) {
    filterType flexibleFilter
    options { count description label placeholder value __typename }
    title subTitle __typename
  }
  appliedFilterSummary { label value filterType __typename }
  dynamicFilters { filterType value label isSelected count __typename }
  __typename
}
fragment sort on HotelListingSearchSummary {
  sort {
    options { isSelected label value __typename }
    title __typename
  }
  __typename
}
fragment location on HotelListingSearchSummary {
  location {
    id city stateCode countryCode countryName name googleMapsStaticImageUrl
    centerGeoCoordinate { latitude longitude __typename }
    area { id __typename }
    __typename
  }
  __typename
}
fragment pagination on HotelListingSearchSummary {
  pagination { offset pageSize searchLabel totalAvailableHotels __typename }
  __typename
}
query getStandaloneHotelListings(
  $appCode: String!, $cguid: ID!, $context: RequestContext,
  $filters: [HotelListingsSearchFilterInput!],
  $locationSearch: HotelListingsLocationSearchInput!,
  $pageType: String!, $isWhiteLabel: Boolean,
  $pagination: HotelListingsPaginationInput,
  $requestDetails: HotelListingInput!, $sort: HotelListingSortType,
  $filterOptions: HotelListingsSearchFilterOptions,
  $selectedAmenities: [String!], $firstName: String,
  $listingType: ListingType
) {
  standaloneHotelListings(
    context: $context filters: $filters locationSearch: $locationSearch
    pagination: $pagination requestDetails: $requestDetails
    sort: $sort listingType: $listingType
  ) {
    listings {
      ... on RtlHotelListing {
        hotelInfo {
          id
          amenities(filter: LIST) { name code __typename }
          brand { name ownerName brandId __typename }
          geoCoordinate { latitude longitude __typename }
          images(imageLimit: 5, imageFilterParams: {amenities: $selectedAmenities}) {
            fastlyUrl source alt __typename
          }
          isFavorite(cguid: $cguid)
          location { id city stateCode countryCode name timeZone __typename }
          name
          neighborhood { name __typename }
          propertyInfo { hotelThemes __typename }
          starLevelText
          traitBadges(appCode: $appCode, pageType: $pageType, isWhiteLabel: $isWhiteLabel) {
            code label category __typename
          }
          reviewInfo {
            reviewSummary {
              scores { label score __typename }
              total { label __typename }
              __typename
            }
            __typename
          }
          __typename
        }
        distanceFromLocation { label __typename }
        sponsoredInfo { clickTrackingUrl impressionTrackingUrl label __typename }
        minRateSummary {
          pclnId
          cancellationPolicy { label __typename }
          merchandisingOptions(clientDisplayCopy: {firstName: $firstName}) {
            description label subType type __typename
          }
          paymentDisclaimer
          upsellDisclaimer(clientDisplayCopy: {firstName: $firstName})
          rtlRateBookingDetails { gid programName __typename }
          minPrice: price {
            amount ctaText currencyCode currencyPrefix description
            formattedAmount formattedDisplayAmount priceDisplayRegulation
            savings savingsPercentage strikePrice __typename
          }
          grandTotalExcludingTax: price(priceType: GRAND_TOTAL_EXCLUDING_TAX) {
            amount currencyCode currencyPrefix description
            formattedAmount formattedDisplayAmount strikePrice savings savingsPercentage __typename
          }
          nightlyAllRoomsPriceExcludingTax: price(priceType: NIGHTLY_ALL_ROOMS_PRICE_EXCLUDING_TAX) {
            amount currencyCode currencyPrefix description
            formattedDisplayAmount strikePrice savings savingsPercentage __typename
          }
          preAndPostPaidTaxes: price(priceType: TOTAL_PRE_AND_POST_PAID_TAXES_PER_STAY) {
            description amount currencyPrefix formattedAmount __typename
          }
          grandTotal: price(priceType: TOTAL) {
            currencyPrefix amount description formattedAmount
            formattedDisplayAmount savings strikePrice __typename
          }
          urgencyMessaging __typename
        }
        __typename
      }
      ... on SopqHotelListing {
        hotelInfo {
          id name
          productDescription { label description __typename }
          images { alt source fastlyUrl __typename }
          amenities(filter: LIST) {
            category { id text __typename }
            code name __typename
          }
          location { id name city stateCode countryCode timeZone __typename }
          neighborhood { id name description __typename }
          guaranteedBrands {
            brands { logo { alt source __typename } __typename }
            __typename
          }
          starLevelText
          reviewInfo {
            reviewSummary {
              scores { label score __typename }
              total { label __typename }
              __typename
            }
            __typename
          }
          __typename
        }
        minRateSummary {
          pclnId
          merchandisingOptions { description label subType type __typename }
          minPrice: price {
            amount currencyCode currencyPrefix description
            formattedAmount formattedDisplayAmount priceDisplayRegulation
            savings ctaText savingsPercentage strikePrice __typename
          }
          grandTotalExcludingTax: price(priceType: GRAND_TOTAL_EXCLUDING_TAX) {
            amount currencyCode currencyPrefix description
            formattedAmount formattedDisplayAmount strikePrice savings savingsPercentage __typename
          }
          nightlyAllRoomsPriceExcludingTax: price(priceType: NIGHTLY_ALL_ROOMS_PRICE_EXCLUDING_TAX) {
            amount currencyCode currencyPrefix description
            formattedDisplayAmount strikePrice savings savingsPercentage __typename
          }
          preAndPostPaidTaxes: price(priceType: TOTAL_PRE_AND_POST_PAID_TAXES_PER_STAY) {
            description amount currencyPrefix formattedAmount __typename
          }
          grandTotal: price(priceType: TOTAL) {
            amount currencyCode currencyPrefix description
            formattedAmount formattedDisplayAmount savings strikePrice __typename
          }
          upsellDisclaimer __typename
        }
        productType __typename
      }
      __typename
    }
    searchSummary {
      ...filters ...sort ...location ...pagination __typename
    }
    __typename
  }
}
"""


def _build_listings_url(destination_id: str, check_in: str, check_out: str, rooms: int, adults: int) -> str:
    dest_q = quote(str(destination_id))
    return (
        f"{BASE_URL}/relax-ui/listings"
        f"?destination={dest_q}"
        f"&checkIn={check_in.replace('-', '')}"
        f"&checkOut={check_out.replace('-', '')}"
        f"&rooms={rooms}"
        f"&adults={adults}"
    )


def _build_payload(
    cguid: str,
    destination_id: str,
    check_in: str,
    check_out: str,
    rooms: int,
    adults: int,
    offset: int = 0,
    page_size: int = 30,
) -> dict:
    # Priceline expects YYYYMMDD dates (no dashes)
    ci = check_in.replace("-", "")
    co = check_out.replace("-", "")
    return {
        "operationName": "getStandaloneHotelListings",
        "variables": {
            "appCode": "DESKTOP",
            "cguid": cguid,
            "context": {"cguid": cguid, "appCode": "DESKTOP"},
            "isWhiteLabel": False,
            "filters": [],
            "filterOptions": {
                "includeAttractions": True,
                "includePriceRegulationBasedBudgetFilters": False,
                "personalizedFilterOrderEnabled": True,
            },
            "listingType": "CLASSIC",
            "pageType": "LISTINGS",
            "requestDetails": {
                "checkIn": ci,
                "checkOut": co,
                "countryData": {"countryCode": "SC", "locale": "en-us", "currencyCode": "USD"},
                "roomCount": rooms,
                "occupancy": {"adults": str(adults), "children": []},
                "referral": {"clickId": "", "sourceId": "PL", "refId": "PLDIRECT"},
                "preferredHotelIds": [],
            },
            "pagination": {"offset": offset, "pageSize": page_size},
            "selectedAmenities": [],
            "firstName": "",
            "locationSearch": {"searchType": "DEFAULT", "searchString": destination_id},
        },
        "query": _GQL_QUERY,
    }


def _proxy_dict(proxy_url: str | None) -> dict:
    if not proxy_url:
        return {}
    return {"http": proxy_url, "https": proxy_url}


class PricelineScraper:
    """curl_cffi-based async scraper for Priceline hotel listings."""

    def __init__(self):
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {"scraper": {}}

        scfg = self.config.get("scraper", {})
        self.timeout = scfg.get("timeout", 60)
        self.max_concurrent = scfg.get("max_concurrent_requests", 15)
        self.max_pages = scfg.get("max_pages", 100)
        self.page_size = scfg.get("page_size", 30)

        impersonate_pref = scfg.get("impersonate", "chrome120")
        self.profile = self._select_profile(impersonate_pref)
        self.impersonate = self.profile["impersonate"]
        self.semaphore: asyncio.Semaphore | None = None

        # Populated at runtime
        self._cguid: str = ""
        self._cookies: dict = {}

    # ------------------------------------------------------------------
    # Profile helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _select_profile(name: str) -> dict:
        for p in _BROWSER_PROFILES:
            if p["impersonate"] == name:
                return p
        return random.choice(_BROWSER_PROFILES)

    def _rotate_profile(self) -> None:
        candidates = [p for p in _BROWSER_PROFILES if p["impersonate"] != self.impersonate]
        if candidates:
            self.profile = random.choice(candidates)
            self.impersonate = self.profile["impersonate"]
            logger.info("[priceline] rotated profile → %s", self.impersonate)

    # ------------------------------------------------------------------
    # Header builders
    # ------------------------------------------------------------------
    def _nav_headers(self, referer: str | None = None) -> dict:
        h = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": self.profile["sec-ch-ua"],
            "sec-ch-ua-mobile": self.profile["sec-ch-ua-mobile"],
            "sec-ch-ua-platform": self.profile["sec-ch-ua-platform"],
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "upgrade-insecure-requests": "1",
            "user-agent": self.profile["user-agent"],
        }
        if referer:
            h["referer"] = referer
            h["sec-fetch-site"] = "same-origin"
        return h

    def _gql_headers(self, listings_url: str) -> dict:
        h = dict(_GQL_HEADERS_BASE)
        h.update({
            "priority": "u=1, i",
            "sec-ch-ua": self.profile["sec-ch-ua"],
            "sec-ch-ua-mobile": self.profile["sec-ch-ua-mobile"],
            "sec-ch-ua-platform": self.profile["sec-ch-ua-platform"],
            "user-agent": self.profile["user-agent"],
            "referer": listings_url or BASE_URL + "/",
        })
        if self._cookies:
            h["cookie"] = "; ".join(f"{k}={v}" for k, v in self._cookies.items())
        return h

    # ------------------------------------------------------------------
    # Cookie harvest (synchronous to avoid event-loop issues)
    # ------------------------------------------------------------------
    async def _harvest_cookies(self, session: AsyncSession, check_in: str, check_out: str, rooms: int, adults: int, destination: str) -> str:
        """Visit homepage + listings page via AsyncSession to collect bypass cookies.
        Returns the listings URL for use as referer."""
        logger.info("[priceline] Harvesting cookies via homepage (%s)…", self.impersonate)

        await session.get(BASE_URL, headers=self._nav_headers(), timeout=30, allow_redirects=True)

        listings_url = _build_listings_url(destination, check_in, check_out, rooms, adults)
        await session.get(listings_url, headers=self._nav_headers(referer=BASE_URL + "/"), timeout=30, allow_redirects=True)

        self._cookies = {k: v for k, v in session.cookies.items()}

        # Extract cguid from SITESERVER cookie
        ss = self._cookies.get("SITESERVER", "")
        if "ID=" in ss:
            self._cguid = ss.split("ID=")[-1].split(";")[0].strip()
        if not self._cguid:
            self._cguid = uuid.uuid4().hex

        logger.info("[priceline] cguid=%s  cookies=%d", self._cguid, len(self._cookies))
        return listings_url

    # ------------------------------------------------------------------
    # Async page fetcher
    # ------------------------------------------------------------------
    async def _fetch_page(
        self,
        session: AsyncSession,
        listings_url: str,
        destination: str,
        check_in: str,
        check_out: str,
        rooms: int,
        adults: int,
        offset: int,
    ) -> list:
        assert self.semaphore is not None
        async with self.semaphore:
            payload = _build_payload(
                self._cguid, destination, check_in, check_out, rooms, adults, offset, self.page_size
            )
            headers = self._gql_headers(listings_url)
            for attempt in range(3):
                try:
                    resp = await session.post(
                        _GRAPHQL_URL, headers=headers, json=payload, timeout=self.timeout
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        listings = self._extract_listings(data)
                        if listings:
                            logger.debug("[priceline] offset=%d → %d results", offset, len(listings))
                        return listings
                    elif resp.status_code == 429:
                        logger.warning("[priceline] rate-limited at offset=%d, retrying…", offset)
                        await asyncio.sleep(2)
                    elif resp.status_code == 403 and attempt < 2:
                        await asyncio.sleep(1.2 + random.random())
                    else:
                        logger.warning("[priceline] HTTP %d at offset=%d", resp.status_code, offset)
                        break
                except Exception as exc:
                    logger.warning("[priceline] request error offset=%d: %s (attempt %d)", offset, exc, attempt + 1)
                    await asyncio.sleep(1)
            return []

    # ------------------------------------------------------------------
    # GraphQL payload helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_container(payload: dict) -> dict | None:
        if not isinstance(payload, dict):
            return None
        data = payload.get("data")
        if not isinstance(data, dict):
            return None
        c = data.get("standaloneHotelListings")
        return c if isinstance(c, dict) else None

    @classmethod
    def _extract_listings(cls, payload: dict) -> list:
        c = cls._extract_container(payload)
        if not c:
            return []
        lst = c.get("listings")
        return lst if isinstance(lst, list) else []

    @classmethod
    def _extract_total(cls, payload: dict) -> int | None:
        c = cls._extract_container(payload)
        if not c:
            return None
        try:
            return int(c["searchSummary"]["pagination"]["totalAvailableHotels"])
        except (KeyError, TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Public search entry-point
    # ------------------------------------------------------------------
    def search(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int = 2,
        rooms: int = 1,
        currency: str = "USD",
        proxy_manager=None,
        **kw,
    ) -> list:
        """Synchronous wrapper with proxy failover across configured providers."""
        max_results = kw.get("max_results", 0)
        try:
            from core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure
        except Exception:
            from metasearch.core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure

        proxy_configs = build_curl_cffi_proxy_list(proxy_manager=proxy_manager, proxy=kw.get("proxy"))
        if not proxy_configs:
            proxy_configs = [("none", None)]
        elif all(purl is not None for _, purl in proxy_configs):
            # If every configured proxy fails, try one final direct attempt.
            proxy_configs.append(("direct_fallback", None))

        for proxy_name, proxy_url in proxy_configs:
            for attempt in range(2):
                if attempt > 0:
                    self._rotate_profile()
                logger.info("[priceline] Trying proxy: %s (attempt=%d, profile=%s)", proxy_name, attempt + 1, self.impersonate)
                try:
                    coro = self._search_async(
                        city, check_in, check_out, adults, rooms, currency,
                        max_results=max_results, proxy_url=proxy_url
                    )
                    loop = None
                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        loop = None

                    if loop and loop.is_running():
                        import concurrent.futures
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            result = pool.submit(asyncio.run, coro).result()
                    elif loop:
                        result = loop.run_until_complete(coro)
                    else:
                        result = asyncio.run(coro)
                except Exception as exc:
                    if is_proxy_failure(str(exc)):
                        logger.warning("[priceline] proxy %s failed: %s", proxy_name, str(exc)[:120])
                        break
                    logger.error("[priceline] search failed via %s: %s", proxy_name, exc)
                    continue

                if result:
                    return result

        return []

    async def _search_async(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int,
        rooms: int,
        currency: str,
        max_results: int = 0,
        proxy_url: str | None = None,
    ) -> list:
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        destination = city.strip()
        all_listings: list = []
        effective_limit = max_results if max_results > 0 else 99999

        # A single AsyncSession handles both cookie harvest and data fetching.
        proxy_cfg = _proxy_dict(proxy_url)
        async with AsyncSession(impersonate=self.impersonate, proxies=proxy_cfg or None) as session:
            if proxy_cfg:
                session.proxies = proxy_cfg
            listings_url = await self._harvest_cookies(session, check_in, check_out, rooms, adults, destination)

            # --- page 0: discover total ---
            payload_0 = _build_payload(
                self._cguid, destination, check_in, check_out, rooms, adults, 0, self.page_size
            )
            resp = None
            for attempt in range(4):
                resp = await session.post(
                    _GRAPHQL_URL,
                    headers=self._gql_headers(listings_url),
                    json=payload_0,
                    timeout=self.timeout,
                )
                if resp.status_code == 200:
                    break
                if resp.status_code == 403 and attempt < 3:
                    # Refresh fingerprint + cookies to recover from anti-bot challenge.
                    self._rotate_profile()
                    listings_url = await self._harvest_cookies(
                        session, check_in, check_out, rooms, adults, destination
                    )
                await asyncio.sleep(1.0 + random.random())

            if not resp or resp.status_code != 200:
                logger.error("[priceline] failed initial page: %s", resp.status_code if resp else "no response")
                return []

            data_0 = resp.json()
            if isinstance(data_0, dict) and data_0.get("errors"):
                first = (data_0.get("errors") or [{}])[0]
                logger.warning(
                    "[priceline] GraphQL errors on initial page: %s",
                    str(first.get("message") or "")[:240],
                )
                return []
            all_listings.extend(self._extract_listings(data_0))

            total = self._extract_total(data_0)
            if total is None:
                logger.warning("[priceline] could not read totalAvailableHotels")
                return all_listings

            logger.info("[priceline] total available: %d  (limit: %s)", total,
                        max_results if max_results > 0 else "none")

            # Early exit — already have enough from page 0
            if len(all_listings) >= effective_limit:
                logger.info("[priceline] limit reached after page 0 (%d >= %d)", len(all_listings), effective_limit)
                return all_listings[:effective_limit]

            if total <= self.page_size:
                return all_listings

            # Only fetch enough pages to hit the limit
            pages_needed = min(
                self.max_pages,
                (effective_limit // self.page_size) + 1,  # +1 to cover remainder
            )
            max_offset = min(total, pages_needed * self.page_size)
            offsets = list(range(self.page_size, max_offset, self.page_size))

            logger.info("[priceline] fetching %d pages (limit=%s)", len(offsets),
                        max_results if max_results > 0 else "all")

            tasks = [
                self._fetch_page(session, listings_url, destination, check_in, check_out, rooms, adults, off)
                for off in offsets
            ]
            results = await asyncio.gather(*tasks)
            for page in results:
                all_listings.extend(page)
                if len(all_listings) >= effective_limit:
                    logger.info("[priceline] limit reached (%d >= %d), stopping", len(all_listings), effective_limit)
                    break

        logger.info("[priceline] scraped %d listings (capped from %d available)", min(len(all_listings), effective_limit), total or len(all_listings))
        return all_listings[:effective_limit] if max_results > 0 else all_listings
