"""
Trip.com Hotels Scraper Integration
Implements purely API-driven requests via `curl_cffi` utilizing the internal `fetchHotelList` endpoint
"""

import logging
import time
import json
import random
import csv
import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import concurrent.futures

from curl_cffi import requests

# Optional support for dynamic paths if run independently
try:
    from core.proxy_helpers import build_proxy_list
except ImportError:
    build_proxy_list = lambda: []

logger = logging.getLogger(__name__)
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

class TripHotelsScraper:
    NAME = "trip_hotels"
    BASE_URL = "https://www.trip.com"
    API_URL = "https://www.trip.com/restapi/soa2/34951/fetchHotelList"
    
    def __init__(self, proxies: list = None):
        self.proxies = proxies or build_proxy_list()
        self.session = self._create_session()
        # These state tokens are typically used to help track requests across paginations
        self.cid = f"17721766{random.randint(10000, 99999)}.5836RO2aIXaX"
        self.sid = str(random.randint(1000000, 9999999))
        self.page_id = "10320668148"

    def _create_session(self) -> requests.Session:
        return requests.Session(verify=False,
            impersonate="chrome124",
            timeout=15
        )
        
    def _make_headers(self, currency: str, locale: str) -> dict:
        return {
            "accept": "application/json",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "cookieorigin": "https://www.trip.com",
            "currency": currency,
            "locale": locale,
            "origin": "https://www.trip.com",
            # Standard random generation logic to mock token handling for Trip without causing hard validation constraints
            "phantom-token": "1004-common-" + "".join(random.choices("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=64)),
            "referer": "https://www.trip.com/hotels/",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="124", "Chromium";v="124"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "x-ctx-currency": currency,
            "x-ctx-locale": locale,
            "x-ctx-ubt-pageid": self.page_id,
            "x-ctx-ubt-pvid": "3",
            "x-ctx-ubt-sid": "1",
            "x-ctx-ubt-vid": self.cid,
            "x-ctx-user-recognize": "NON_EU",
        }

    def _build_payload(self, city_id: int, city_name: str, checkin: str, checkout: str,
                       adults: int, rooms: int, page_index: int, currency: str, locale: str) -> dict:
        # Trip.com dates are unhyphenated in payload (e.g. 2026-02-27 -> 20260227)
        c_in_raw = checkin.replace("-", "")
        c_out_raw = checkout.replace("-", "")
        
        return {
            "date": {
                "dateType": 1,
                "dateInfo": {
                    "checkInDate": c_in_raw,
                    "checkOutDate": c_out_raw
                }
            },
            "destination": {
                "type": 1,
                "geo": {
                    "cityId": city_id,
                    "countryId": 0
                },
                "keyword": {
                    "word": city_name
                }
            },
            "extraFilter": {
                "childInfoItems": [],
                "ctripMainLandBDCoordinate": True,
                "sessionId": "",
                "extendableParams": {
                    "tripWalkDriveSwitch": "T",
                    "isUgcSentenceB": "",
                    "multiLangHotelNameVersion": "E"
                }
            },
            "filters": [
                {"type": "19", "title": "", "value": str(city_id), "filterId": f"19|{city_id}"},
                {"type": "80", "title": "Price per room per night (excl. taxes & fees)", "value": "0", "filterId": "80|0|1"}
            ],
            "roomQuantity": rooms,
            "marketInfo": {
                "received": False,
                "isRechargeSuccessful": False,
                "authInfo": {"isLogin": False, "isMember": False},
                "extraInfo": {"SpecialActivityId": "T"}
            },
            "paging": {
                "pageIndex": page_index,
                "pageSize": 20,
                "pageCode": self.page_id
            },
            "hotelIdFilter": {
                "hotelAldyShown": []
            },
            "head": {
                "platform": "PC",
                "cver": "0",
                "cid": self.cid,
                "bu": "IBU",
                "group": "trip",
                "aid": "742331",
                "sid": self.sid,
                "ouid": "",
                "locale": locale,
                "timezone": "5",
                "currency": currency,
                "pageId": self.page_id,
                "vid": self.cid,
                "guid": "",
                "isSSR": False,
                "extension": [
                    {"name": "cityId", "value": str(city_id)},
                    {"name": "checkIn", "value": checkin},
                    {"name": "checkOut", "value": checkout},
                    {"name": "region", "value": "XX"}
                ]
            }
        }

    def search(self,
               city_id: int,
               city_name: str,
               checkin: str,
               checkout: str,
               adults: int = 2,
               rooms: int = 1,
               pages: int = 5,
               currency: str = "USD",
               locale: str = "en-XX") -> List[Dict]:
        
        all_results = []
        hdrs = self._make_headers(currency, locale)

        def _poll_page(page: int) -> list[dict]:
            logger.info("[%s] Polling page %d...", self.NAME, page)
            payload = self._build_payload(
                city_id=city_id,
                city_name=city_name,
                checkin=checkin,
                checkout=checkout,
                adults=adults,
                rooms=rooms,
                page_index=page,
                currency=currency,
                locale=locale
            )

            proxy = random.choice(self.proxies) if self.proxies else None
            proxy_dict = {"http": proxy, "https": proxy} if proxy else None

            for attempt in range(1, 3):
                try:
                # Add base cookie headers for trip payload structure
                    cookies = {
                    "GUID": str(random.randint(10000, 99999)),
                    "UBT_VID": self.cid,
                    "ibulanguage": "EN",
                    "ibulocale": locale.lower(),
                    "cookiePricesDisplayed": currency,
                    "ibu_cookie_strict": "0"
                }

                    r = self.session.post(
                    self.API_URL,
                    headers=hdrs,
                    json=payload,
                    cookies=cookies,
                    proxies=proxy_dict,
                    timeout=30
                )
                
                    if r.status_code != 200:
                        logger.warning("[%s] HTTP %s on page %d (attempt %d): %s", self.NAME, r.status_code, page, attempt, r.text[:200])
                        if attempt == 2:
                            return []
                        time.sleep(0.8)
                        continue

                    d = r.json()
                    data = d.get("data", {})
                    hotels_array = data.get("hotelList", [])
                    # Some responses return nested list object.
                    if isinstance(hotels_array, dict):
                        hotels_array = hotels_array.get("hotelList", []) or hotels_array.get("list", [])
                    logger.info("[%s] Page %d yielded %d results", self.NAME, page, len(hotels_array))
                    return hotels_array

                except Exception as e:
                    logger.error("[%s] Poll error on page %d (attempt %d): %s", self.NAME, page, attempt, e)
                    if attempt == 2:
                        return []
                    time.sleep(0.8)
                return []

        # Utilize concurrent futures exactly like Checkfelix upgrade
        max_threads = min(pages, 5)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_page = {executor.submit(_poll_page, p): p for p in range(1, pages + 1)}
            for future in concurrent.futures.as_completed(future_to_page):
                batch = future.result()
                if batch:
                    all_results.extend(batch)
                
        # Dump for testing
        try:
            with open("debug_trip_hotels.json", "w", encoding="utf-8") as f:
                json.dump(all_results, f, indent=2)
        except:
            pass

        return self._parse_results(all_results, currency)

    def _parse_results(self, raw_hotels: List[Dict], currency: str) -> List[Dict]:
        parsed = []
        seen = set()
        for prop in raw_hotels:
            try:
                hotel_node = prop.get("hotelInfo", {})
                room_node_list = prop.get("roomInfo", [])
                
                # We need at least basic info
                if not hotel_node: continue

                summary = hotel_node.get("summary", {})
                raw_id = summary.get("hotelId", summary.get("masterHotelId", ""))

                names = hotel_node.get("nameInfo", {})
                hotel_name = names.get("enName") or names.get("name", "Unknown Hotel")

                # Stars & ratings
                stars = hotel_node.get("hotelStar", {}).get("star", 0.0)
                comments = hotel_node.get("commentInfo", {})
                guest_score = float(comments.get("commentScore", 0.0) or 0.0)
                reviews = comments.get("commenterNumber", "0")
                if isinstance(reviews, str):
                    reviews = reviews.split()[0].replace(",", "")
                    if reviews:
                        reviews = int(reviews)
                    else:
                        reviews = 0
                        

                rating_category = comments.get("commentDescription", "")

                # Detailed Amenities / Categories from comment extraction and standard node tags
                amenity_list = []
                for sub in comments.get("subScore", []):
                    if sub.get("content"):
                        amenity_list.append(f"{sub['content']} ({sub.get('scoreAvg','')})")

                # Geolocation 
                pos = hotel_node.get("positionInfo", {})
                district = pos.get("positionDesc", pos.get("address", ""))
                city = pos.get("cityNameEn", pos.get("cityName", ""))
                
                lat, lon = "", ""
                coords = pos.get("mapCoordinate", [])
                for coord in coords:
                    if str(coord.get("coordinateType")) == "1":  # WGS84 native
                        lat = str(coord.get("latitude", ""))
                        lon = str(coord.get("longitude", ""))
                        break

                # Descriptions
                desc = comments.get("ugcSentence", "")
                
                # Image thumbnail logic
                images = hotel_node.get("hotelImages", {})
                thumbnail = images.get("url", "")
                
                # Retrieve lowest available price from room options logic
                price_min = 99999.0
                best_room = {}
                for room in room_node_list:
                    p = room.get("priceInfo", {}).get("price", 0)
                    if p and p < price_min:
                        price_min = p
                        best_room = room

                if price_min == 99999.0: price_min = 0.0

                p_layer = best_room.get("priceInfoLayer", {})
                # Extract full total from layer for entire duration
                total_val = p_layer.get("total", {}).get("content", "").replace("US$", "").replace("$","").replace(",","")
                try:
                    total_price = float(total_val)
                except ValueError:
                    total_price = price_min

                # Tax string stripping
                paytax = p_layer.get("payTax", {}).get("content", "0").replace("US$", "").replace("$","").replace(",","")
                try: 
                    tax_val = float(paytax)
                except ValueError: 
                    tax_val = 0.0

                bed_titles = best_room.get("bedInfo", {}).get("contentList", [])
                
                row = {
                    "hotel_name": hotel_name,
                    "raw_id": str(raw_id),
                    "provider": self.NAME,
                    
                    "price_per_night": float(price_min),
                    "total_price": total_price,
                    "taxes_included": False,
                    "currency": currency,
                    "tax_amount": tax_val,
                    
                    "star_rating": float(stars),
                    "guest_rating": float(guest_score),
                    "review_count": int(reviews) if reviews else 0,
                    "rating_category": rating_category,
                    
                    "city": city,
                    "district": district,
                    "latitude": lat,
                    "longitude": lon,
                    
                    "amenities": ", ".join(amenity_list),
                    "room_type": best_room.get("summary", {}).get("physicsName", ""),
                    "bed_options": ", ".join(bed_titles) if bed_titles else "",
                    
                    "description": desc,
                    "thumbnail_url": thumbnail,
                    "booking_url": f"https://www.trip.com/hotels/detail/?hotelId={raw_id}"
                }
                # Keep one best row per (hotel, room, total_price).
                dedupe_key = (row["raw_id"], row["room_type"], row["total_price"], row["currency"])
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                parsed.append(row)
            except Exception as e:
                logger.error("Failed to parse a Trip.com hotel property: %s", e)
                continue
                
        return parsed


def debug_trip_hotels():
    # Isolated runner that reads params from config.json
    logging.basicConfig(level=logging.INFO)

    cfg = {}
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    default_params = cfg.get("default_params", {})
    city_name = default_params.get("city", "Dubai")
    city_id = int(default_params.get("city_id", 220))
    checkin = default_params.get("checkin") or (date.today() + timedelta(days=10)).isoformat()
    checkout = default_params.get("checkout") or (date.today() + timedelta(days=12)).isoformat()
    adults = int(default_params.get("adults", 2))
    rooms = int(default_params.get("rooms", 1))
    pages = int(default_params.get("pages", 2))
    currency = default_params.get("currency", "USD")
    locale = default_params.get("locale", "en-XX")

    scraper = TripHotelsScraper()
    print(f"Testing Trip.com Hotels Scraper: {city_name} | {checkin} to {checkout}")
    results = scraper.search(
        city_id=city_id,
        city_name=city_name,
        checkin=checkin,
        checkout=checkout,
        adults=adults,
        rooms=rooms,
        pages=pages,
        currency=currency,
        locale=locale
    )
    
    print("=" * 60)
    print(f"Yielded {len(results)} fully parsed items.")
    print("=" * 60)
    if results:
        for k,v in results[0].items():
            print(f"{k}: {v}")

    save_cfg = cfg.get("save_output", {})
    if save_cfg.get("enabled", False):
        output_path = save_cfg.get("path", "trip_hotels_results.json")
        output_format = save_cfg.get("format", "json").lower()
        out = Path(output_path)
        if not out.is_absolute():
            out = SCRIPT_DIR / out
        out.parent.mkdir(parents=True, exist_ok=True)

        if output_format == "csv":
            if results:
                with open(out, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
                    writer.writeheader()
                    writer.writerows(results)
            else:
                with open(out, "w", newline="", encoding="utf-8") as f:
                    f.write("")
        else:
            with open(out, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"Saved output -> {out}")
    else:
        print("save_output.enabled is false, skipping file save.")

if __name__ == "__main__":
    debug_trip_hotels()
