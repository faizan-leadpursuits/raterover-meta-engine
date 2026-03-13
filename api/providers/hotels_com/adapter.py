"""
Hotels.com adapter — uses the Expedia GraphQL backend via expedia.com domain.

Hotels.com's own domain blocks aggressively with 429/DataDome.
Since Hotels.com and Expedia share the same GraphQL backend (Expedia Group),
we route through expedia.com with Hotels.com siteId/tpid/eapid context.
"""

import logging
from datetime import datetime
import pandas as pd
import re
import uuid

from core.hotel_base_adapter import HotelBaseAdapter
from core.hotel_schemas import make_hotel_row, HOTEL_COMMON_COLUMNS
from core.city_resolver import get_resolver

logger = logging.getLogger(__name__)

def _parse_price(formatted: str) -> float:
    if not formatted: return 0.0
    digits = re.sub(r"[^\d.]", "", formatted.replace(",", ""))
    try: return float(digits) if digits else 0.0
    except ValueError: return 0.0


class HotelsComAdapter(HotelBaseAdapter):
    NAME = "hotels_com"
    DOMAIN = "hotels"
    NEEDS_PROXY = True
    SUPPORTED_PARAMS = ["city", "check_in", "check_out", "adults", "rooms", "currency"]

    def search(self, city, check_in, check_out, adults=2, children=0, rooms=1, currency="USD", **kw):
        from core.expedia_group_engine import ExpediaGroupScraper
        
        logger.info("[%s] Searching %s...", self.NAME, city)
        
        # 1. Resolve Region
        resolver = get_resolver()
        region = resolver.resolve_hotels_com_region(city)
        if not region:
            logger.warning("[%s] Could not resolve region for %s", self.NAME, city)
            return self.empty_result()
            
        # 2. Scrape — Use 'expedia' brand to avoid hotels.com 429 blocks
        #    but with hotels_com siteId context in the GraphQL payload
        provider = self.proxy_manager.get_provider("dataimpulse") if self.proxy_manager else None
        proxy_url = provider.get_proxy_url() if provider else None
        
        # Route through Expedia's domain (which doesn't block as hard)
        scraper = ExpediaGroupScraper(brand="expedia", proxy=proxy_url)
        
        # Override the context to use Hotels.com branding
        raw_hotels = self._search_as_hotels_com(scraper, {
            "city": city,
            "regionId": region["regionId"],
            "lat": region["lat"],
            "lng": region["lng"],
            "check_in": check_in,
            "check_out": check_out,
            "adults": adults,
            "rooms": rooms,
            "currency": currency,
        })
        
        return self._normalize(raw_hotels, city, check_in, check_out, currency)

    def _search_as_hotels_com(self, scraper, params):
        """Use the Expedia scraper but override the GraphQL context to Hotels.com branding."""
        import time, random
        from curl_cffi import requests
        
        try:
            # 1. Cookie Harvest via Expedia
            logger.info("[%s] Harvesting cookies via Expedia...", self.NAME)
            scraper.session.get(scraper.base_url + "/", headers=scraper._get_headers("browse"), timeout=20)
            time.sleep(random.uniform(1.0, 2.0))
            
            # 2. Build Hotels.com-branded GraphQL payload
            ci = params["check_in"].split("-")
            co = params["check_out"].split("-")
            
            # Hotels.com context (different site_id/tpid/eapid from Expedia)
            context = {
                "siteId": 300000036,
                "locale": "en_US",
                "eapid": 36,
                "tpid": 3202,
                "currency": params.get("currency", "USD"),
                "device": {"type": "DESKTOP"},
                "identity": {"duaid": scraper.duaid, "authState": "ANONYMOUS"},
                "privacyTrackingState": "CAN_TRACK"
            }
            
            payload = [{
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
                            "counts": [
                                {"id": "resultsStartingIndex", "value": 0},
                                {"id": "resultsSize", "value": 50}
                            ],
                            "selections": [{"id": "sort", "value": "RECOMMENDED"}]
                        }
                    }
                },
                "extensions": {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "908ef1ccd58a146e59da0e09b4bbda870fb041608a9215ccf939605f0cb43a31"
                    }
                }
            }]
            
            headers = scraper._get_headers("api", referer=scraper.base_url + "/Hotel-Search")
            headers["x-page-id"] = "page.Hotel-Search,H,20"
            
            resp = scraper.session.post(
                f"{scraper.base_url}/graphql",
                json=payload,
                headers=headers,
                timeout=30
            )
            
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    for item in data:
                        if "data" in item and "propertySearch" in item["data"]:
                            return self._extract_listings(item)
                elif "data" in data and "propertySearch" in data["data"]:
                    return self._extract_listings(data)
            
            logger.warning("[%s] GraphQL returned %d", self.NAME, resp.status_code)
            return []
            
        except Exception as e:
            logger.error("[%s] Search failed: %s", self.NAME, e)
            return []

    def _extract_listings(self, resp_data):
        try:
            return resp_data["data"]["propertySearch"]["propertySearchListings"]
        except (KeyError, TypeError):
            try:
                return resp_data["data"]["propertySearch"]["properties"]["elements"]
            except (KeyError, TypeError):
                return []

    def _normalize(self, hotels, city, check_in, check_out, currency="USD"):
        if not hotels:
            return self.empty_result()
            
        try:
            ci = datetime.strptime(check_in, "%Y-%m-%d")
            co = datetime.strptime(check_out, "%Y-%m-%d")
            nights = max((co - ci).days, 1)
        except:
            nights = 1
            
        rows = []
        for raw in hotels:
            try:
                # Extract from GraphQL property listing format
                name = ""
                price = 0.0
                image_url = ""
                deep_link = ""
                star_rating = 0
                guest_rating = 0.0
                review_count = 0
                
                if isinstance(raw, dict):
                    # Property name
                    name = raw.get("name", "")
                    if not name:
                        name_obj = raw.get("headingSection", {})
                        if isinstance(name_obj, dict):
                            name = name_obj.get("heading", "")
                    
                    # Price
                    price_info = raw.get("price", raw.get("priceSection", {}))
                    if isinstance(price_info, dict):
                        lead = price_info.get("lead", price_info.get("priceSummary", {}))
                        if isinstance(lead, dict):
                            price = float(lead.get("amount", 0) or 0)
                            if price == 0:
                                formatted = lead.get("formatted", "")
                                price = _parse_price(formatted)
                    
                    # Image
                    gallery = raw.get("gallery", raw.get("propertyImage", {}))
                    if isinstance(gallery, dict):
                        img = gallery.get("images", [{}])[0] if gallery.get("images") else gallery.get("image", {})
                        if isinstance(img, dict):
                            image_url = img.get("url", img.get("imageUrl", ""))
                    
                    # Rating
                    reviews_info = raw.get("reviews", raw.get("reviewsSummary", {}))
                    if isinstance(reviews_info, dict):
                        guest_rating = float(reviews_info.get("score", 0) or 0)
                        review_count = int(reviews_info.get("total", reviews_info.get("count", 0)) or 0)
                    
                    # Star rating
                    star_rating = int(raw.get("star", raw.get("starRating", 0)) or 0)
                    
                    # Deep link
                    deep_link = raw.get("deeplink", raw.get("mapMarker", {}).get("deeplink", ""))
                
                if not name:
                    continue
                
                ppn = round(price / nights, 2) if price > 0 else 0.0
                
                rows.append(make_hotel_row(
                    source=self.NAME,
                    hotel_name=name,
                    city=city,
                    check_in=check_in,
                    check_out=check_out,
                    nights=nights,
                    price=price,
                    price_per_night=ppn,
                    currency=currency,
                    star_rating=star_rating,
                    guest_rating=guest_rating,
                    review_count=review_count,
                    image_url=image_url,
                    deep_link=deep_link,
                    booking_provider="Hotels.com",
                ))
            except Exception as e:
                logger.debug("[%s] Normalize error: %s", self.NAME, e)
                
        logger.info("[%s] Found %d hotels", self.NAME, len(rows))
        return pd.DataFrame(rows, columns=HOTEL_COMMON_COLUMNS)
