"""
Wego.pk Hotel Search Scraper
==============================
Uses curl_cffi to impersonate Chrome and bypass Cloudflare TLS fingerprinting.
"""

import json
import time
import uuid
import logging
import urllib.parse
import random
from datetime import datetime, timezone
from curl_cffi import requests as cf_requests

logger = logging.getLogger(__name__)

class WegoScraper:
    def __init__(self, proxy: str = None, impersonate: str = "chrome120"):
        self.proxy = proxy
        self.impersonate = impersonate
        self.session = cf_requests.Session(verify=False, impersonate=self.impersonate)
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
        
        self.base_url = "https://www.wego.pk"
        self.site_code = "PK"
        self.locale = "en"
        self.currency = "USD"
        
        self.headers_common = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "content-type": "application/json",
            "origin": self.base_url,
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="120", "Chromium";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def init_session(self, client_id: str, session_id: str, city: str = "DXB", check_in: str = "2026-06-01", check_out: str = "2026-06-05", adults: int = 2):
        logger.info("[wego] Initializing session...")
        self.session.cookies.set("wego_analytics_client_id", client_id, domain="www.wego.pk")
        self.session.cookies.set("wego_analytics_client_session_id", session_id, domain="www.wego.pk")
        
        # 1. Visit home page
        r1 = self.session.get(f"{self.base_url}/", headers={"accept": "text/html"}, timeout=20)
        logger.info("[wego] Home page status: %d", r1.status_code)
        
        # 2. Visit search page to trigger more cookies
        search_page = f"{self.base_url}/hotels/searches/{city.lower()}/{check_in}/{check_out}?guests={adults}&locale={self.locale}&currencyCode={self.currency}"
        r2 = self.session.get(search_page, headers={"accept": "text/html", "referer": f"{self.base_url}/"}, timeout=25)
        logger.info("[wego] Search page status: %d", r2.status_code)
        
        time.sleep(random.uniform(1.0, 2.0))
        
    def create_search(self, client_id: str, session_id: str, params: dict) -> str:
        city = params["city_code"].upper()
        check_in = params["check_in"]
        check_out = params["check_out"]
        guests = params.get("adults", 2)
        rooms = params.get("rooms", 1)
        
        logger.info("[wego] Creating search for %s (%s -> %s)", city, check_in, check_out)
        
        rooms_array = [{"adultsCount": guests, "childrenCount": params.get("children", 0)} for _ in range(rooms)]
        
        payload = {
            "search": {
                "cityCode": city,
                "roomsCount": rooms,
                "guestsCount": guests * rooms,
                "adultsCount": guests,
                "childrenCount": params.get("children", 0),
                "rooms": rooms_array,
                "checkIn": check_in,
                "checkOut": check_out,
                "locale": self.locale,
                "currencyCode": self.currency,
                "siteCode": self.site_code,
                "deviceType": "DESKTOP",
                "appType": "WEB_APP",
                "userLoggedIn": False,
                "clientCreatedAt": self._now_iso(),
            }
        }
        
        url = f"{self.base_url}/kong/v3/metasearch/hotels/searches?locale={self.locale}&currencyCode={self.currency}"
        
        headers = self.headers_common.copy()
        headers["referer"] = f"{self.base_url}/hotels/searches/{city.lower()}/{check_in}/{check_out}"
        
        resp = self.session.post(url, json=payload, headers=headers, timeout=20)
        if resp.status_code not in (200, 201):
            raise Exception(f"Failed to create search: {resp.status_code} {resp.text}")
            
        data = resp.json()
        return data.get("search", {}).get("id") or data.get("id")

    def poll_results(self, session_id: str, search_id: str, max_polls=10) -> dict:
        url = f"{self.base_url}/kong/v3/metasearch/hotels/searches/{search_id}/results"
        referer = f"{self.base_url}/hotels/results/{search_id}"
        
        all_hotels = {}
        all_rates = []
        seen_rates = set()
        
        offset = 0
        for i in range(max_polls):
            params = {
                "locale": self.locale,
                "currencyCode": self.currency,
                "amountType": "NIGHTLY",
                "offset": offset,
                "isLastPolling": "true" if i == max_polls - 1 else "false",
                "moreRates": "true"
            }
            
            headers = self.headers_common.copy()
            headers["referer"] = referer
            headers["x-wego-session-id"] = session_id
            
            resp = self.session.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code != 200:
                logger.warning("[wego] Poll %d failed: %d", i+1, resp.status_code)
                time.sleep(2)
                continue
                
            data = resp.json()
            hotels = data.get("hotels", [])
            rates = data.get("rates", [])
            completed = data.get("searchCompleted", False)
            
            logger.info("[wego] Poll %d: %d hotels, %d rates (completed: %s)", i+1, len(hotels), len(rates), completed)
            
            for h in hotels:
                all_hotels[str(h.get("id"))] = h
            
            for r in rates:
                r_id = r.get("id")
                if r_id not in seen_rates:
                    seen_rates.add(r_id)
                    all_rates.append(r)
            
            if completed:
                break
            
            offset += len(rates)
            time.sleep(2)
            
        return {"hotels": list(all_hotels.values()), "rates": all_rates}

def parse_results(poll_data: dict, params: dict) -> list[dict]:
    hotels_map = {str(h["id"]): h for h in poll_data["hotels"]}
    results = []
    
    nights = 1
    try:
        ci = datetime.strptime(params["check_in"], "%Y-%m-%d")
        co = datetime.strptime(params["check_out"], "%Y-%m-%d")
        nights = max(1, (co - ci).days)
    except: pass

    for rate in poll_data["rates"]:
        h_id = str(rate.get("hotelId"))
        hotel = hotels_map.get(h_id)
        if not hotel: continue
        
        price = float(rate.get("price", {}).get("totalAmount") or rate.get("price", {}).get("amount", 0))
        if price <= 0: continue
        
        results.append({
            "hotel_name": hotel.get("name"),
            "address": hotel.get("address", ""),
            "city": hotel.get("cityCode", ""),
            "star_rating": hotel.get("star", 0),
            "guest_rating": hotel.get("score", 0),
            "review_count": hotel.get("reviewCount", 0),
            "latitude": hotel.get("latitude", 0),
            "longitude": hotel.get("longitude", 0),
            "price_total": price,
            "price_nightly": price / nights,
            "currency": rate.get("price", {}).get("currencyCode", "USD"),
            "handoff_url": rate.get("handoffUrl", ""),
            "room_type": rate.get("description", ""),
            "board_type": rate.get("params", {}).get("board_basis", ""),
            "cancellation": str(rate.get("params", {}).get("cancellations", "")),
            "image_url": hotel.get("image", ""),
        })
        
    return sorted(results, key=lambda x: x["price_total"])
