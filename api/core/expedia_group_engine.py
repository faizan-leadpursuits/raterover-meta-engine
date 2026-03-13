"""
Shared GraphQL engine for Expedia Group sites (Expedia, Hotels.com, Vrbo, etc.).
Anti-bot hardened with curl_cffi + strict header ordering + proxy rotation.
"""

import json
import time
import random
import uuid
import logging
import re
from typing import Any, List, Dict, Optional
from curl_cffi import requests

logger = logging.getLogger(__name__)

# ── Brand Configurations ──────────────────────────────────────────
BRANDS = {
    "expedia": {
        "base_url": "https://www.expedia.com",
        "site_id": 1,
        "tpid": 1,
        "eapid": 0,
        "brand_name": "expedia",
        "origin_id": "page.Hotel-Search,H,20",
    },
    "hotels_com": {
        "base_url": "https://www.hotels.com",
        "site_id": 300000036,
        "tpid": 3202,
        "eapid": 36,
        "brand_name": "hotels",
        "origin_id": "page.Hotel-Search,H,20",
    },
    "orbitz": {
        "base_url": "https://www.orbitz.com",
        "site_id": 2,
        "tpid": 2,
        "eapid": 0,
        "brand_name": "orbitz",
        "origin_id": "page.Hotel-Search,H,20",
    },
    "hotwire": {
        "base_url": "https://www.hotwire.com",
        "site_id": 5,
        "tpid": 5,
        "eapid": 0,
        "brand_name": "hotwire",
        "origin_id": "page.Hotel-Search,H,20",
    }
}

# ── GraphQL Persisted Query Hashes ───────────────────────────────
HASHES = {
    "PropertyListingQuery": "908ef1ccd58a146e59da0e09b4bbda870fb041608a9215ccf939605f0cb43a31",
    "VoiceOfTheCustomerQuery": "fcafb0121d6ca4232ccb88aa4296d060323dbb701f06534d6bc537eb652511ab",
}

# Chrome impersonation profiles to cycle through
IMPERSONATION_PROFILES = ["chrome131", "chrome142", "chrome124", "chrome120"]

class ExpediaGroupScraper:
    """Unified scraper for Expedia Group GraphQL API."""

    def __init__(self, brand: str = "expedia", proxy: str = None, impersonate: str = None):
        self.brand_id = brand if brand in BRANDS else "expedia"
        self.cfg = BRANDS[self.brand_id]
        self.proxy = proxy
        self.impersonate = impersonate or random.choice(IMPERSONATION_PROFILES)
        
        self.session = self._create_session()
        self.base_url = self.cfg["base_url"]
        self.duaid = str(uuid.uuid4())
        self.search_id = str(uuid.uuid4())

    def _create_session(self) -> requests.Session:
        s = requests.Session(verify=False, impersonate=self.impersonate)
        if self.proxy:
            s.proxies = {"http": self.proxy, "https": self.proxy}
        return s

    def _rotate_proxy(self):
        """Reset session with fresh impersonation to get a fresh IP from the residential pool."""
        if not self.proxy:
            return
        cookies = self.session.cookies.get_dict()
        self.session.close()
        # Cycle to a different impersonation profile
        self.impersonate = random.choice(IMPERSONATION_PROFILES)
        self.session = self._create_session()
        for k, v in cookies.items():
            self.session.cookies.set(k, v)

    def _get_headers(self, type: str = "browse", referer: str = None) -> dict:
        if type == "browse":
            return {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "max-age=0",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
            }
        else: # api
            h = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "client-info": "shopping-pwa",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "x-page-id": self.cfg["origin_id"],
            }
            if referer:
                h["referer"] = referer
            return h

    def search(self, params: dict) -> List[dict]:
        """
        Execute search and return raw property listings.
        
        Args:
            params: {city, regionId, lat, lng, check_in, check_out, adults, rooms, currency}
        """
        try:
            # 1. Cookie Harvest (Home Page)
            logger.info("[%s] Harvesting cookies...", self.brand_id)
            self.session.get(self.base_url + "/", headers=self._get_headers("browse"), timeout=20)
            time.sleep(random.uniform(1.5, 3.0))

            # 2. Search Page Visit (Trigger session token)
            region_id = params.get("regionId")
            if not region_id:
                return []
                
            search_url = (
                f"{self.base_url}/Hotel-Search"
                f"?destination={params['city']}"
                f"&regionId={region_id}"
                f"&startDate={params['check_in']}"
                f"&endDate={params['check_out']}"
                f"&adults={params['adults']}"
            )
            self.session.get(search_url, headers=self._get_headers("browse"), timeout=25)
            time.sleep(random.uniform(2.0, 4.0))

            # 3. GraphQL Fetch with retries
            return self._fetch_page(params)

        except Exception as e:
            logger.error("[%s] Search failed: %s", self.brand_id, e)
            return []

    def _fetch_page(self, params: dict, offset: int = 0) -> List[dict]:
        payload = self._build_payload(params, offset)
        headers = self._get_headers("api", referer=self.base_url + "/Hotel-Search")
        
        for attempt in range(3):
            try:
                resp = self.session.post(
                    f"{self.base_url}/graphql",
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                if resp.status_code == 200:
                    data = resp.json()
                    # Handle batch list or single object
                    if isinstance(data, list):
                        for item in data:
                            if "data" in item and "propertySearch" in item["data"]:
                                return self._extract_listings(item)
                    elif "data" in data and "propertySearch" in data["data"]:
                        return self._extract_listings(data)
                
                if resp.status_code in (429, 403, 503):
                    logger.warning("[%s] Blocked (%d) attempt %d/3, rotating proxy...", 
                                   self.brand_id, resp.status_code, attempt + 1)
                    self._rotate_proxy()
                    wait = random.uniform(3.0, 6.0) * (attempt + 1)
                    time.sleep(wait)
                    
                    # Re-harvest cookies on retry
                    if attempt < 2:
                        try:
                            self.session.get(self.base_url + "/", 
                                           headers=self._get_headers("browse"), timeout=20)
                            time.sleep(random.uniform(1.0, 2.0))
                        except:
                            pass
                        
            except Exception as e:
                logger.warning("[%s] Request error (attempt %d): %s", self.brand_id, attempt + 1, e)
                self._rotate_proxy()
                time.sleep(random.uniform(2.0, 4.0))
                
        return []

    def _build_payload(self, params: dict, offset: int) -> List[dict]:
        # Simplified payload structure matching Expedia/Hotels.com requirements
        ci = params["check_in"].split("-")
        co = params["check_out"].split("-")
        
        context = {
            "siteId": self.cfg["site_id"],
            "locale": "en_US",
            "eapid": self.cfg["eapid"],
            "tpid": self.cfg["tpid"],
            "currency": params.get("currency", "USD"),
            "device": {"type": "DESKTOP"},
            "identity": {"duaid": self.duaid, "authState": "ANONYMOUS"},
            "privacyTrackingState": "CAN_TRACK"
        }

        listing_query = {
            "operationName": "PropertyListingQuery",
            "variables": {
                "context": context,
                "criteria": {
                    "primary": {
                        "dateRange": {
                            "checkInDate": {"day": int(ci[2]), "month": int(ci[1]), "year": int(ci[0])},
                            "checkOutDate": {"day": int(co[2]), "month": int(co[1]), "year": int(co[0])}
                        },
                        "destination": {
                            "regionId": params["regionId"],
                            "coordinates": {"latitude": params["lat"], "longitude": params["lng"]}
                        },
                        "rooms": [{"adults": params["adults"], "children": []}]
                    },
                    "secondary": {
                        "counts": [{"id": "resultsStartingIndex", "value": offset}, {"id": "resultsSize", "value": 50}],
                        "selections": [{"id": "sort", "value": "RECOMMENDED"}]
                    }
                }
            },
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": HASHES["PropertyListingQuery"]}}
        }
        
        return [listing_query]

    def _extract_listings(self, resp_data: dict) -> List[dict]:
        try:
            return resp_data["data"]["propertySearch"]["propertySearchListings"]
        except (KeyError, TypeError):
            try:
                return resp_data["data"]["propertySearch"]["properties"]["elements"]
            except (KeyError, TypeError):
                return []
