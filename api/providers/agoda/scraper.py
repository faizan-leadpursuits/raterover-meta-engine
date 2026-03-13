"""
Agoda Hotels — Pure HTTP GraphQL API Client (no Playwright).

Uses Agoda's internal GraphQL API at /graphql/search with curl_cffi
for Chrome TLS fingerprint impersonation.

Flow:
  1. GET the search page with curl_cffi to establish session cookies
  2. Extract correlationId, cid, origin from embedded page config
  3. POST the full GraphQL query to /graphql/search
  4. Parse the richly structured JSON response (pricing, reviews,
     landmarks, family features, cancellation, promotions, etc.)
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

# ── City ID map (Agoda uses numeric city IDs) ──────────────────────────
CITY_IDS = {
    "london": 233, "paris": 15470, "new york": 1474, "dubai": 2994,
    "tokyo": 5085, "rome": 14958, "barcelona": 13979, "istanbul": 4613,
    "bangkok": 9395, "amsterdam": 14797, "berlin": 14810, "madrid": 14857,
    "singapore": 4064, "lisbon": 14850, "prague": 14898, "vienna": 15014,
    "milan": 14866, "sydney": 5070, "melbourne": 5066, "toronto": 10001,
    "hong kong": 6113, "seoul": 3965, "mumbai": 7742, "kuala lumpur": 14398,
    "athens": 14778, "dublin": 6519, "edinburgh": 6523, "munich": 14871,
    "los angeles": 1438, "chicago": 1505, "miami": 2579, "san francisco": 3049,
    "cairo": 7923, "marrakech": 11524, "abu dhabi": 2994, "doha": 6523,
    "jeddah": 36125, "riyadh": 35684, "lahore": 2298, "islamabad": 16157,
    "karachi": 16158, "bali": 17193, "phuket": 16538, "maldives": 14656,
    "cancun": 2168, "honolulu": 2098, "cape town": 12932, "rio de janeiro": 11013,
    "moscow": 14870, "beijing": 14552, "shanghai": 14690, "delhi": 7761,
    "osaka": 13147, "kyoto": 13128, "taipei": 12688, "hanoi": 16667,
    "ho chi minh": 16706, "jakarta": 14660, "nairobi": 17093,
}

# ── Full GraphQL query (reverse-engineered from Agoda frontend) ────────
SEARCH_QUERY = """
query citySearch($CitySearchRequest: CitySearchRequest!, $ContentSummaryRequest: ContentSummaryRequest!, $PricingSummaryRequest: PricingRequestParameters, $PriceStreamMetaLabRequest: PriceStreamMetaLabRequest) {
  citySearch(CitySearchRequest: $CitySearchRequest) {
    searchResult {
      searchInfo {
        hasSecretDeal
        isComplete
        totalFilteredHotels
        searchStatus {
          searchCriteria { checkIn }
          searchStatus
        }
        objectInfo {
          objectName
          cityName
          cityEnglishName
          countryId
          countryEnglishName
          mapLatitude
          mapLongitude
          mapZoomLevel
          cityId
        }
      }
    }
    properties(ContentSummaryRequest: $ContentSummaryRequest, PricingSummaryRequest: $PricingSummaryRequest, PriceStreamMetaLabRequest: $PriceStreamMetaLabRequest) {
      propertyId
      propertyResultType
      content {
        informationSummary {
          hotelCharacter {
            hotelTag { name symbol }
            hotelView { name symbol }
          }
          propertyLinks { propertyPage }
          atmospheres { id name }
          isSustainableTravel
          localeName
          defaultName
          displayName
          accommodationType
          awardYear
          hasHostExperience
          address {
            countryCode
            country { id name }
            city { id name }
            area { id name }
          }
          propertyType
          rating
          agodaGuaranteeProgram
          remarks {
            renovationInfo { renovationType year }
          }
          spokenLanguages { id }
          geoInfo { latitude longitude }
        }
        propertyEngagement {
          lastBooking
          peopleLooking
          todayBooking
        }
        nonHotelAccommodation {
          masterRooms {
            noOfBathrooms
            noOfBedrooms
            noOfBeds
            roomSizeSqm
            highlightedFacilities
          }
          hostLevel { id name }
          supportedLongStay
        }
        facilities { id }
        images {
          hotelImages {
            id
            caption
            providerId
            urls { key value }
          }
        }
        reviews {
          contentReview {
            isDefault
            providerId
            demographics {
              groups {
                id
                grades { id score }
              }
            }
            summaries {
              recommendationScores { recommendationScore }
              snippets {
                countryId countryCode countryName
                date demographicId demographicName
                reviewer reviewRating snippet
              }
            }
            cumulative { reviewCount score }
          }
          cumulative { reviewCount score }
        }
        familyFeatures {
          hasChildrenFreePolicy
          isFamilyRoom
          hasMoreThanOneBedroom
          isInterConnectingRoom
          isInfantCottageAvailable
          hasKidsPool
          hasKidsClub
        }
        personalizedInformation {
          childrenFreePolicy { fromAge toAge }
        }
        localInformation {
          landmarks {
            transportation { landmarkName distanceInM }
            topLandmark { landmarkName distanceInM }
            beach { landmarkName distanceInM }
          }
          hasAirportTransfer
        }
        highlight {
          cityCenter { distanceFromCityCenter }
          favoriteFeatures {
            features { id title category }
          }
          hasNearbyPublicTransportation
        }
        rateCategories {
          escapeRateCategories { rateCategoryId localizedRateCategoryName }
        }
      }
      pricing {
        pulseCampaignMetadata {
          promotionTypeId
          campaignBadgeText
          campaignBadgeDescText
          dealExpiryTime
          showPulseMerchandise
        }
        isAvailable
        isReady
        benefits
        isEasyCancel
        isInsiderDeal
        isMultiHotelEligible
        payment {
          cancellation { cancellationType freeCancellationDate }
          payLater { isEligible }
          payAtHotel { isEligible }
          noCreditCard { isEligible }
          taxReceipt { isEligible }
        }
        offers {
          roomOffers {
            room {
              availableRooms
              supplierId
              pricing {
                currency
                price {
                  perNight {
                    exclusive { display cashbackPrice displayAfterCashback originalPrice }
                    inclusive { display cashbackPrice displayAfterCashback originalPrice }
                  }
                  perRoomPerNight {
                    exclusive { display crossedOutPrice cashbackPrice displayAfterCashback rebatePrice originalPrice }
                    inclusive { display crossedOutPrice cashbackPrice displayAfterCashback rebatePrice originalPrice }
                  }
                  perBook {
                    exclusive { display cashbackPrice displayAfterCashback rebatePrice originalPrice autoAppliedPromoDiscount }
                    inclusive { display cashbackPrice displayAfterCashback rebatePrice originalPrice autoAppliedPromoDiscount }
                  }
                  totalDiscount
                }
              }
              payment {
                paymentModel
                cancellation { cancellationType }
              }
              discount { deals channelDiscount }
              benefits { id targetType }
              channel { id }
              cashback {
                cashbackGuid showPostCashbackPrice cashbackVersion
                percentage earnId dayToEarn expiryDay cashbackType appliedCampaignName
              }
              agodaCash { showBadge giftcardGuid dayToEarn expiryDay percentage }
              capacity { extraBedsAvailable }
              stayPackageType
            }
          }
        }
        supplierInfo { id name isAgodaBand }
        childPolicy { freeChildren }
      }
      enrichment {
        topSellingPoint { tspType value }
        pricingBadges { badges }
        uniqueSellingPoint { rank segment uspType uspPropertyType }
        bookingHistory {
          bookingCount { count timeFrame }
        }
        showReviewSnippet
        isPopular
        roomInformation {
          cheapestRoomSizeSqm
          facilities { id propertyFacilityName symbol }
        }
      }
    }
    searchEnrichment { pageToken }
  }
}
""".strip()


class AgodaHotelsScraper:
    """
    Pure HTTP client for Agoda hotel search via GraphQL.
    Uses curl_cffi with Chrome TLS fingerprint impersonation.
    Extracts comprehensive hotel data including pricing, reviews,
    landmarks, family features, cancellation, and promotions.
    """

    BASE_URL = "https://www.agoda.com"
    GRAPHQL_URL = "https://www.agoda.com/graphql/search"

    def __init__(self):
        self._session = cffi_requests.Session(verify=False, impersonate="chrome124")

    # ══════════════════════════════════════════════════════════════════
    # Public API
    # ══════════════════════════════════════════════════════════════════

    def search(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int = 2,
        rooms: int = 1,
        currency: str = "USD",
        page: int = 1,
        page_size: int = 45,
        max_pages: int = 10,
        proxy: str | None = None,
        proxy_manager=None,
        **kw,
    ) -> list[dict]:
        """
        Search hotels on Agoda via the internal GraphQL API.

        Args:
            city: City name (e.g. "London", "Dubai")
            check_in: Check-in date "YYYY-MM-DD"
            check_out: Check-out date "YYYY-MM-DD"
            adults: Number of adults per room
            rooms: Number of rooms
            currency: ISO 4217 currency code
            page: Page number (1-indexed)
            page_size: Results per page (max ~45)
            max_pages: Maximum number of pages to fetch (0 = all)
            proxy: Optional proxy URL string
            proxy_manager: Optional ProxyManager instance

        Returns:
            List of normalized hotel dicts with comprehensive data.
        """
        from core.proxy_helpers import build_curl_cffi_proxy_list, is_proxy_failure
        proxy_configs = build_curl_cffi_proxy_list(proxy_manager, proxy)

        for proxy_name, proxy_url in proxy_configs:
            logger.info("[agoda_hotels] Trying proxy: %s", proxy_name)
            try:
                result = self._search_with_proxy(
                    city, check_in, check_out, adults, rooms,
                    currency, page, page_size, max_pages, proxy_url,
                    max_results=int(kw.get("max_results", 0)),
                )
                if result:
                    logger.info("[agoda_hotels] Got %d hotels via %s", len(result), proxy_name)
                    return result
            except Exception as e:
                if is_proxy_failure(str(e)):
                    logger.warning("[agoda_hotels] Proxy %s failed: %s", proxy_name, str(e)[:80])
                    continue
                logger.error("[agoda_hotels] Error: %s", e)
                raise
        return []

    # ══════════════════════════════════════════════════════════════════
    # Internal methods
    # ══════════════════════════════════════════════════════════════════

    def _resolve_city_id(self, city: str, proxy: str | None = None) -> int:
        """Resolve city name to Agoda city ID."""
        key = city.lower().strip()
        if key in CITY_IDS:
            return CITY_IDS[key]
        # Fuzzy match
        for k, v in CITY_IDS.items():
            if key in k or k in key:
                return v

        # Dynamic fallback: Use Agoda's Autocomplete API
        logger.info("[agoda_hotels] City '%s' not found locally, fetching dynamically...", city)
        return self._fetch_dynamic_city_id(city, proxy)

    def _fetch_dynamic_city_id(self, city: str, proxy: str | None = None) -> int:
        """Use Agoda's UnifiedSuggestResult API to find the city ID."""
        proxies = {"http": proxy, "https": proxy} if proxy else None
        
        # We need another session un-attached to avoid interfering with later requests
        temp_session = cffi_requests.Session(verify=False, impersonate="chrome124")
        url = f"https://www.agoda.com/api/cronos/search/GetUnifiedSuggestResult/3/1/1/0/en-us/?searchText={city}&pageTypeId=1&isHotelLandSearch=true"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        try:
            r = temp_session.get(url, headers=headers, proxies=proxies, timeout=15)
            if r.status_code == 200:
                data = r.json()
                results = data if isinstance(data, list) else data.get("ViewModelList", data.get("SuggestionList", data.get("results", [])))
                if isinstance(results, list) and results:
                    for item in results:
                        if isinstance(item, dict):
                            cid = item.get("CityId", item.get("cityId", item.get("id", 0)))
                            if cid:
                                logger.info("[agoda_hotels] Dynamically resolved '%s' to cityId %s", city, cid)
                                return int(cid)
            logger.warning("[agoda_hotels] Dynamic lookup failed for city '%s' - API returned irregular format or no results", city)
        except Exception as e:
            logger.warning("[agoda_hotels] Dynamic lookup failed for city '%s': %s", city, e)
        
        return 15064  # Default to London if everything fails

    def _get_session_context(
        self, city_id: int, check_in: str, check_out: str,
        currency: str, proxy_url: str | None,
    ) -> dict:
        """
        Fetch the search page to establish session cookies and
        extract correlationId, cid, origin from the embedded page config.
        """
        search_url = (
            f"{self.BASE_URL}/search?city={city_id}"
            f"&checkIn={check_in}&checkOut={check_out}"
            f"&rooms=1&adults=2&currency={currency}"
        )
        proxies = {"https": proxy_url, "http": proxy_url} if proxy_url else None

        r = self._session.get(search_url, headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }, proxies=proxies, timeout=25)

        html = r.text

        # Extract context values from pageConfig / inline JS
        correlation_match = re.search(r'"correlationId":"([^"]+)"', html)
        correlation_id = correlation_match.group(1) if correlation_match else str(uuid.uuid4())

        origin_match = re.search(r'"origin":"([^"]+)"', html)
        origin = origin_match.group(1) if origin_match else "US"

        cid_match = re.search(r'"cid":(\d+)', html)
        cid = int(cid_match.group(1)) if cid_match else 1922890

        user_id_match = re.search(r'"userId":"([^"]+)"', html)
        user_id = user_id_match.group(1) if user_id_match else str(uuid.uuid4())

        storefront_match = re.search(r'"storefrontId":(\d+)', html)
        storefront_id = int(storefront_match.group(1)) if storefront_match else 3

        analytics_match = re.search(r'"analyticsSessionId":"(\d+)"', html)
        analytics_id = analytics_match.group(1) if analytics_match else "0"

        return {
            "correlation_id": correlation_id,
            "origin": origin,
            "cid": cid,
            "user_id": user_id,
            "storefront_id": storefront_id,
            "analytics_id": analytics_id,
            "search_url": search_url,
        }

    def _build_variables(
        self,
        city_id: int,
        check_in: str,
        check_out: str,
        adults: int,
        rooms: int,
        currency: str,
        page: int,
        page_size: int,
        ctx: dict,
        page_token: str = "",
    ) -> dict:
        """Build the full GraphQL variables payload."""
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        ci_dt = f"{check_in}T00:00:00.000Z"
        co_dt = f"{check_out}T00:00:00.000Z"

        # Calculate length of stay
        try:
            cin = datetime.strptime(check_in, "%Y-%m-%d")
            cout = datetime.strptime(check_out, "%Y-%m-%d")
            los = max(1, (cout - cin).days)
        except Exception:
            los = 1

        search_id = str(uuid.uuid4())

        return {
            "CitySearchRequest": {
                "cityId": city_id,
                "searchRequest": {
                    "searchCriteria": {
                        "isAllowBookOnRequest": True,
                        "bookingDate": now_iso,
                        "checkInDate": ci_dt,
                        "localCheckInDate": check_in,
                        "los": los,
                        "rooms": rooms,
                        "adults": adults,
                        "children": 0,
                        "childAges": [],
                        "ratePlans": [],
                        "featureFlagRequest": {
                            "fetchNamesForTealium": True,
                            "fiveStarDealOfTheDay": True,
                            "isAllowBookOnRequest": False,
                            "showUnAvailable": True,
                            "showRemainingProperties": True,
                            "isMultiHotelSearch": False,
                            "enableAgencySupplyForPackages": True,
                            "flags": [
                                {"feature": "FamilyChildFriendlyPopularFilter", "enable": True},
                                {"feature": "FamilyChildFriendlyPropertyTypeFilter", "enable": True},
                                {"feature": "FamilyMode", "enable": False},
                            ],
                            "enablePageToken": True,
                            "enableDealsOfTheDayFilter": False,
                            "isEnableSupplierFinancialInfo": False,
                            "isFlexibleMultiRoomSearch": True,
                            "enableLuxuryHotelTSP": True,
                        },
                        "isUserLoggedIn": False,
                        "currency": currency,
                        "travellerType": "Couple",
                        "isAPSPeek": False,
                        "enableOpaqueChannel": False,
                        "isEnabledPartnerChannelSelection": None,
                        "sorting": {
                            "sortField": "Ranking",
                            "sortOrder": "Desc",
                            "sortParams": None,
                        },
                        "requiredBasis": "PRPN",
                        "requiredPrice": "Exclusive",
                        "suggestionLimit": 0,
                        "synchronous": False,
                        "supplierPullMetadataRequest": None,
                        "isRoomSuggestionRequested": False,
                        "isAPORequest": False,
                        "hasAPOFilter": False,
                    },
                    "searchContext": {
                        "userId": ctx["user_id"],
                        "memberId": 0,
                        "locale": "en-us",
                        "cid": ctx["cid"],
                        "origin": ctx["origin"],
                        "platform": 1,
                        "deviceTypeId": 1,
                        "experiments": {
                            "forceByVariant": None,
                            "forceByExperiment": [{"id": "JGCW-204", "variant": "B"}],
                        },
                        "isRetry": False,
                        "showCMS": False,
                        "storeFrontId": ctx["storefront_id"],
                        "pageTypeId": 103,
                        "whiteLabelKey": None,
                        "ipAddress": "0.0.0.0",
                        "endpointSearchType": "CitySearch",
                        "trackSteps": None,
                        "searchId": search_id,
                    },
                    "matrix": None,
                    "matrixGroup": [
                        {"matrixGroup": "AccommodationType", "size": 100},
                        {"matrixGroup": "StarRatingWithLuxury", "size": 20},
                        {"matrixGroup": "HotelFacilities", "size": 100},
                        {"matrixGroup": "RoomAmenities", "size": 100},
                        {"matrixGroup": "RoomBenefits", "size": 100},
                        {"matrixGroup": "PaymentOptions", "size": 100},
                        {"matrixGroup": "ReviewScore", "size": 100},
                        {"matrixGroup": "HotelAreaId", "size": 100},
                        {"matrixGroup": "ProductType", "size": 100},
                        {"matrixGroup": "GroupedBedTypes", "size": 100},
                        {"matrixGroup": "PopularForFamily", "size": 5},
                        {"matrixGroup": "KidsStayForFree", "size": 5},
                    ],
                    "filterRequest": {
                        "idsFilters": [],
                        "rangeFilters": [],
                        "textFilters": [],
                    },
                    "page": {
                        "pageSize": page_size,
                        "pageNumber": page,
                        "pageToken": page_token,
                    },
                    "apoRequest": {"apoPageSize": 10},
                    "searchHistory": [],
                    "searchDetailRequest": {"priceHistogramBins": 50},
                    "isTrimmedResponseRequested": False,
                    "featuredAgodaHomesRequest": None,
                    "featuredLuxuryHotelsRequest": None,
                    "highlyRatedAgodaHomesRequest": {
                        "numberOfAgodaHomes": 30,
                        "minimumReviewScore": 7.5,
                        "minimumReviewCount": 3,
                        "accommodationTypes": [28, 29, 30, 102, 103, 106, 107, 108, 109, 110, 114, 115, 120, 131],
                        "sortVersion": 0,
                    },
                    "extraAgodaHomesRequest": None,
                    "extraHotels": {"extraHotelIds": [], "enableFiltersForExtraHotels": False},
                    "rankingRequest": {"isNhaKeywordSearch": False},
                    "rocketmilesRequestV2": None,
                    "featuredPulsePropertiesRequest": {"numberOfPulseProperties": 15},
                },
            },
            "ContentSummaryRequest": {
                "context": {
                    "rawUserId": ctx["user_id"],
                    "memberId": 0,
                    "userOrigin": ctx["origin"],
                    "locale": "en-us",
                    "forceExperimentsByIdNew": [{"key": "JGCW-204", "value": "B"}],
                    "apo": False,
                    "searchCriteria": {"cityId": city_id},
                    "platform": {"id": 1},
                    "storeFrontId": ctx["storefront_id"],
                    "cid": str(ctx["cid"]),
                    "occupancy": {
                        "numberOfAdults": adults,
                        "numberOfChildren": 0,
                        "travelerType": 2,
                        "checkIn": ci_dt,
                    },
                    "deviceTypeId": 1,
                    "whiteLabelKey": "",
                    "correlationId": "",
                },
                "summary": {
                    "highlightedFeaturesOrderPriority": None,
                    "includeHotelCharacter": True,
                },
                "reviews": {
                    "commentary": None,
                    "demographics": {
                        "providerIds": None,
                        "filter": {"defaultProviderOnly": True},
                    },
                    "summaries": {
                        "providerIds": None,
                        "apo": True,
                        "limit": 1,
                        "travellerType": 2,
                    },
                    "cumulative": {"providerIds": None},
                    "filters": None,
                },
                "images": {
                    "page": None,
                    "maxWidth": 0,
                    "maxHeight": 0,
                    "imageSizes": None,
                    "indexOffset": None,
                },
                "rooms": {
                    "images": None,
                    "featureLimit": 0,
                    "filterCriteria": None,
                    "includeMissing": False,
                    "includeSoldOut": False,
                    "includeDmcRoomId": False,
                    "soldOutRoomCriteria": None,
                    "showRoomSize": True,
                    "showRoomFacilities": True,
                    "showRoomName": False,
                },
                "nonHotelAccommodation": True,
                "engagement": True,
                "highlights": {
                    "maxNumberOfItems": 0,
                    "images": {"imageSizes": [{"key": "full", "size": {"width": 0, "height": 0}}]},
                },
                "personalizedInformation": True,
                "localInformation": {"images": None},
                "features": None,
                "rateCategories": True,
                "contentRateCategories": {"escapeRateCategories": {}},
                "synopsis": True,
            },
            "PricingSummaryRequest": {
                "cheapestOnly": True,
                "context": {
                    "isAllowBookOnRequest": True,
                    "abTests": [
                        {"testId": 9021, "abUser": "B"},
                        {"testId": 9023, "abUser": "B"},
                        {"testId": 9024, "abUser": "B"},
                        {"testId": 9025, "abUser": "B"},
                        {"testId": 9027, "abUser": "B"},
                        {"testId": 9029, "abUser": "B"},
                    ],
                    "clientInfo": {
                        "cid": ctx["cid"],
                        "languageId": 1,
                        "languageUse": 1,
                        "origin": ctx["origin"],
                        "platform": 1,
                        "searchId": search_id,
                        "storefront": ctx["storefront_id"],
                        "userId": ctx["user_id"],
                        "ipAddress": "0.0.0.0",
                    },
                    "experiment": [{"name": "JGCW-204", "variant": "B"}],
                    "sessionInfo": {
                        "isLogin": False,
                        "memberId": 0,
                        "sessionId": 1,
                    },
                },
                "isSSR": True,
                "pricing": {
                    "bookingDate": now_iso,
                    "checkIn": ci_dt,
                    "checkout": co_dt,
                    "localCheckInDate": check_in,
                    "localCheckoutDate": check_out,
                    "currency": currency,
                    "details": {
                        "cheapestPriceOnly": False,
                        "itemBreakdown": False,
                        "priceBreakdown": False,
                    },
                    "featureFlag": [
                        "ClientDiscount", "PriceHistory", "VipPlatinum",
                        "RatePlanPromosCumulative", "PromosCumulative", "MixAndSave",
                        "APSPeek", "StackChannelDiscount", "AutoApplyPromos",
                        "EnableAgencySupplyForPackages", "EnableCashback",
                        "CreditCardPromotionPeek", "EnableCofundedCashback",
                        "DispatchGoLocalForInternational", "EnableGoToTravelCampaign",
                        "EnableCashbackMildlyAggressiveDisplay", "EnablePriceTrend",
                    ],
                    "features": {
                        "crossOutRate": False,
                        "isAPSPeek": False,
                        "isAllOcc": False,
                        "isApsEnabled": False,
                        "isIncludeUsdAndLocalCurrency": False,
                        "isMSE": True,
                        "isRPM2Included": True,
                        "maxSuggestions": 0,
                        "isEnableSupplierFinancialInfo": False,
                        "isLoggingAuctionData": False,
                        "newRateModel": False,
                        "overrideOccupancy": False,
                        "filterCheapestRoomEscapesPackage": False,
                        "priusId": 0,
                        "synchronous": False,
                        "enableRichContentOffer": True,
                        "showCouponAmountInUserCurrency": False,
                        "disableEscapesPackage": False,
                        "enablePushDayUseRates": False,
                        "enableDayUseCor": False,
                        "ignoreRoomsCountForNha": False,
                        "enableSuggestPriceExclusiveWithFees": True,
                    },
                    "filters": {
                        "cheapestRoomFilters": [],
                        "filterAPO": False,
                        "ratePlans": [1],
                        "secretDealOnly": False,
                        "suppliers": [],
                        "nosOfBedrooms": [],
                    },
                    "includedPriceInfo": False,
                    "occupancy": {
                        "adults": adults,
                        "children": 0,
                        "childAges": [],
                        "rooms": rooms,
                        "childrenTypes": [],
                    },
                    "supplierPullMetadata": {"requiredPrecheckAccuracyLevel": 0},
                    "mseHotelIds": [],
                    "mseClicked": "",
                    "ppLandingHotelIds": [],
                    "searchedHotelIds": [],
                    "paymentId": -1,
                    "externalLoyaltyRequest": None,
                },
                "suggestedPrice": "Exclusive",
            },
            "PriceStreamMetaLabRequest": {"attributesId": [8, 1, 18, 7, 11, 2, 3]},
        }

    def _search_with_proxy(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int,
        rooms: int,
        currency: str,
        page: int,
        page_size: int,
        max_pages: int,
        proxy_url: str | None,
        max_results: int = 0,
    ) -> list[dict]:
        """Execute search with pagination support."""
        import time
        start_time = time.time()

        # Resolve city
        city_id = self._resolve_city_id(city, proxy_url)
        proxies = {"https": proxy_url, "http": proxy_url} if proxy_url else None

        logger.info("[agoda_hotels] Step 1: Getting session context for city_id=%s", city_id)
        ctx = self._get_session_context(city_id, check_in, check_out, currency, proxy_url)

        # Build persistent headers for all pages
        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": self.BASE_URL,
            "Referer": ctx["search_url"],
            "ag-correlation-id": ctx["correlation_id"],
            "ag-language-locale": "en-us",
            "ag-page-type-id": "103",
            "ag-request-attempt": "1",
            "ag-retry-attempt": "0",
            "ag-cid": str(ctx["cid"]),
            "ag-analytics-session-id": ctx["analytics_id"],
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

        all_hotels = []
        page_token = ""
        current_page = page
        total_available = 0
        effective_max = max_pages if max_pages > 0 else 9999  # 0 = unlimited

        timeout_limit = 15  # Stop before hitting global 20s API timeout

        while current_page <= effective_max + (page - 1):
            if time.time() - start_time > timeout_limit:
                logger.warning("[agoda_hotels] Internal timeout limit (15s) reached. Stopping pagination.")
                break
            
            headers["ag-request-id"] = str(uuid.uuid4())

            variables = self._build_variables(
                city_id, check_in, check_out, adults, rooms,
                currency, current_page, page_size, ctx, page_token,
            )
            payload = {
                "operationName": "citySearch",
                "variables": variables,
                "query": SEARCH_QUERY,
            }

            logger.info("[agoda_hotels] Fetching page %d (token=%s)...",
                        current_page, page_token[:20] if page_token else "none")

            try:
                r = self._session.post(
                    self.GRAPHQL_URL, json=payload, headers=headers,
                    proxies=proxies, timeout=30,
                )
            except Exception as e:
                logger.warning("[agoda_hotels] Request failed on page %d: %s", current_page, e)
                break

            if r.status_code != 200:
                logger.warning("[agoda_hotels] Page %d returned %d: %s",
                               current_page, r.status_code, r.text[:200])
                break

            try:
                data = r.json()
            except Exception:
                logger.warning("[agoda_hotels] Invalid JSON on page %d", current_page)
                break

            if "errors" in data and data.get("data") is None:
                for err in data.get("errors", [])[:3]:
                    logger.warning("[agoda_hotels] GraphQL error: %s", err.get("message", "")[:200])
                break

            # Parse this page
            city_search = data.get("data", {}).get("citySearch", {})
            if not city_search:
                break

            # Get total on first page
            if current_page == page:
                search_info = city_search.get("searchResult", {}).get("searchInfo", {})
                total_available = search_info.get("totalFilteredHotels", 0)
                logger.info("[agoda_hotels] Total available hotels: %d", total_available)

            properties = city_search.get("properties", [])
            if not properties:
                logger.info("[agoda_hotels] No more properties on page %d", current_page)
                break

            page_hotels = self._parse_response(data, city, check_in, check_out, currency)
            all_hotels.extend(page_hotels)
            logger.info("[agoda_hotels] Page %d: +%d hotels (total so far: %d / %d)",
                        current_page, len(page_hotels), len(all_hotels), total_available)

            # Early-stop if max_results reached
            if max_results > 0 and len(all_hotels) >= max_results:
                logger.info("[agoda_hotels] Limit %d reached — stopping pagination", max_results)
                all_hotels = all_hotels[:max_results]
                break

            # Get next page token
            next_token = city_search.get("searchEnrichment", {}) or {}
            page_token = next_token.get("pageToken", "") or ""

            # Stop if we've fetched all available hotels
            if len(all_hotels) >= total_available:
                logger.info("[agoda_hotels] Fetched all available hotels")
                break

            # Stop if page was short (no more data)
            if len(properties) < page_size:
                logger.info("[agoda_hotels] Last page was partial (%d < %d)",
                            len(properties), page_size)
                break

            current_page += 1
            time.sleep(0.3)  # Minimal rate-limit between pages

        all_hotels.sort(key=lambda x: x.get("price", 9999))
        logger.info("[agoda_hotels] Pagination complete: %d hotels across %d pages",
                    len(all_hotels), current_page - page + 1)
        return all_hotels

    # ══════════════════════════════════════════════════════════════════
    # Response parsing — extracts ALL available data
    # ══════════════════════════════════════════════════════════════════

    def _parse_response(
        self,
        data: dict,
        city: str,
        check_in: str,
        check_out: str,
        currency: str,
    ) -> list[dict]:
        """Parse the full GraphQL response into comprehensive hotel dicts."""
        hotels = []

        try:
            cin = datetime.strptime(check_in, "%Y-%m-%d")
            cout = datetime.strptime(check_out, "%Y-%m-%d")
            nights = max(1, (cout - cin).days)
        except Exception:
            nights = 1

        city_search = data.get("data", {}).get("citySearch", {})
        if not city_search:
            logger.warning("[agoda_hotels] No citySearch in response")
            return []

        # Search metadata
        search_info = city_search.get("searchResult", {}).get("searchInfo", {})
        total_hotels = search_info.get("totalFilteredHotels", 0)
        logger.info("[agoda_hotels] Total available hotels: %d", total_hotels)

        properties = city_search.get("properties", [])
        if not properties:
            logger.warning("[agoda_hotels] No properties in response")
            return []

        for prop in properties:
            try:
                hotel = self._parse_property(prop, city, check_in, check_out, nights, currency)
                if hotel:
                    hotels.append(hotel)
            except Exception as e:
                logger.debug("[agoda_hotels] Parse error for property: %s", e)
                continue

        hotels.sort(key=lambda x: x.get("price", 9999))
        logger.info("[agoda_hotels] Parsed %d hotels from GraphQL response", len(hotels))
        return hotels

    def _parse_property(
        self, prop: dict, city: str, check_in: str, check_out: str,
        nights: int, currency: str,
    ) -> dict | None:
        """Parse a single property from the GraphQL response."""
        content = prop.get("content") or {}
        pricing = prop.get("pricing") or {}
        enrichment = prop.get("enrichment") or {}
        info = content.get("informationSummary") or {}

        # ── Basic info ──────────────────────────────────────────────
        name = info.get("displayName", "") or info.get("defaultName", "") or info.get("localeName", "")
        if not name or len(name) < 3:
            return None

        property_id = prop.get("propertyId", "")
        property_type = info.get("propertyType", "")
        accommodation_type = info.get("accommodationType", "")
        is_sustainable = info.get("isSustainableTravel", False)
        award_year = info.get("awardYear", "")

        # ── Location & address ──────────────────────────────────────
        geo = info.get("geoInfo", {})
        lat = float(geo.get("latitude", 0) or 0)
        lng = float(geo.get("longitude", 0) or 0)

        addr = info.get("address", {})
        city_info = addr.get("city", {})
        country_info = addr.get("country", {})
        area_info = addr.get("area", {})

        city_name = city_info.get("name", city)
        country_name = country_info.get("name", "")
        country_code = addr.get("countryCode", "")
        area_name = area_info.get("name", "")
        address_parts = [p for p in [area_name, city_name, country_name] if p]
        hotel_address = ", ".join(address_parts)

        # ── Star rating ─────────────────────────────────────────────
        stars = int(info.get("rating", 0) or 0)
        stars = min(stars, 5)

        # ── Reviews ─────────────────────────────────────────────────
        reviews_data = content.get("reviews") or {}
        cumulative = reviews_data.get("cumulative") or {}
        guest_rating = float(cumulative.get("score", 0) or 0)
        review_count = int(cumulative.get("reviewCount", 0) or 0)

        # Review snippets
        review_snippets = []
        content_reviews = reviews_data.get("contentReview") or []
        if isinstance(content_reviews, list):
            for cr in content_reviews:
                if not isinstance(cr, dict):
                    continue
                summaries = cr.get("summaries") or {}
                snippets_list = summaries.get("snippets") or [] if isinstance(summaries, dict) else []
                for summary in snippets_list:
                    if not isinstance(summary, dict):
                        continue
                    snippet = summary.get("snippet", "")
                    if snippet:
                        review_snippets.append({
                            "reviewer": summary.get("reviewer", ""),
                            "rating": summary.get("reviewRating", 0),
                            "snippet": snippet,
                            "country": summary.get("countryName", ""),
                            "date": summary.get("date", ""),
                        })

        # ── Deep link ───────────────────────────────────────────────
        links = info.get("propertyLinks", {})
        deep_link = links.get("propertyPage", "")
        if deep_link and not deep_link.startswith("http"):
            deep_link = f"{self.BASE_URL}{deep_link}"

        # ── Images ──────────────────────────────────────────────────
        images_data = content.get("images", {})
        hotel_images = images_data.get("hotelImages", []) or []
        image_url = ""
        all_images = []
        for img in hotel_images:
            urls = img.get("urls", []) or []
            caption = img.get("caption", "")
            for url_entry in urls:
                val = url_entry.get("value", "") if isinstance(url_entry, dict) else ""
                if val:
                    all_images.append({"url": val, "caption": caption})
                    if not image_url:
                        image_url = val

        # ── Pricing ─────────────────────────────────────────────────
        price_ppn = 0.0
        price_total = 0.0
        price_currency = currency
        original_price = 0.0
        crossed_out_price = 0.0
        discount_percent = 0.0
        cashback_percent = 0.0
        room_available = 0

        is_available = pricing.get("isAvailable", False)

        try:
            # offers is a LIST of offer groups, each containing roomOffers
            offers_list = pricing.get("offers", []) or []
            if isinstance(offers_list, list) and offers_list:
                first_offer = offers_list[0] if isinstance(offers_list[0], dict) else {}
                room_offers = first_offer.get("roomOffers", []) or []
            elif isinstance(offers_list, dict):
                room_offers = offers_list.get("roomOffers", []) or []
            else:
                room_offers = []

            if room_offers:
                room = room_offers[0].get("room", {}) or {}
                room_available = int(room.get("availableRooms", 0) or 0)

                # room.pricing is a LIST of pricing entries (one per currency)
                rp_raw = room.get("pricing", [])
                if isinstance(rp_raw, list) and rp_raw:
                    rp = rp_raw[0]  # First pricing entry
                elif isinstance(rp_raw, dict):
                    rp = rp_raw
                else:
                    rp = {}

                price_currency = rp.get("currency", currency)
                price_data = rp.get("price", {}) or {}

                # Per-room-per-night (preferred)
                prpn = price_data.get("perRoomPerNight", {}) or {}
                exc = prpn.get("exclusive", {}) or {}
                price_ppn = float(exc.get("display", 0) or 0)
                original_price = float(exc.get("originalPrice", 0) or 0)
                crossed_out_price = float(exc.get("crossedOutPrice", 0) or 0)

                # If no PRPN, try perNight
                if price_ppn <= 0:
                    pn = price_data.get("perNight", {}) or {}
                    exc_pn = pn.get("exclusive", {}) or {}
                    price_ppn = float(exc_pn.get("display", 0) or 0)

                # Per-book total
                pb = price_data.get("perBook", {}) or {}
                exc_pb = pb.get("exclusive", {}) or {}
                price_total = float(exc_pb.get("display", 0) or 0)

                # Total discount
                discount_percent = float(price_data.get("totalDiscount", 0) or 0)

                # Cashback
                cb = room.get("cashback") or {}
                if cb and isinstance(cb, dict):
                    cashback_percent = float(cb.get("percentage", 0) or 0)
        except Exception as e:
            logger.debug("[agoda_hotels] Price parse: %s", e)

        if price_ppn <= 0:
            return None

        if price_total <= 0:
            price_total = price_ppn * nights

        # ── Cancellation & payment ──────────────────────────────────
        cancellation = ""
        free_cancellation_date = ""
        pay_later = False
        pay_at_hotel = False
        no_credit_card = False

        payment = pricing.get("payment", {})
        if payment:
            cancel_info = payment.get("cancellation", {})
            cancel_type = cancel_info.get("cancellationType", "")
            free_cancellation_date = cancel_info.get("freeCancellationDate", "")

            if cancel_type and "free" in str(cancel_type).lower():
                cancellation = "Free cancellation"
                if free_cancellation_date:
                    cancellation += f" until {free_cancellation_date}"
            elif cancel_type:
                cancellation = str(cancel_type)

            pay_later = bool((payment.get("payLater", {}) or {}).get("isEligible", False))
            pay_at_hotel = bool((payment.get("payAtHotel", {}) or {}).get("isEligible", False))
            no_credit_card = bool((payment.get("noCreditCard", {}) or {}).get("isEligible", False))

        is_easy_cancel = pricing.get("isEasyCancel", False)
        if is_easy_cancel and not cancellation:
            cancellation = "Easy cancel"

        # ── Room benefits & supplier ────────────────────────────────
        benefits = pricing.get("benefits", []) or []
        supplier_info = pricing.get("supplierInfo", {})
        supplier_name = supplier_info.get("name", "") if supplier_info else ""

        # ── Facilities ──────────────────────────────────────────────
        facility_ids = [f.get("id") for f in (content.get("facilities", []) or []) if f.get("id")]

        # ── Family features ─────────────────────────────────────────
        family = content.get("familyFeatures", {}) or {}
        family_features = {
            "children_free_policy": family.get("hasChildrenFreePolicy", False),
            "family_room": family.get("isFamilyRoom", False),
            "multi_bedroom": family.get("hasMoreThanOneBedroom", False),
            "connecting_rooms": family.get("isInterConnectingRoom", False),
            "infant_cottage": family.get("isInfantCottageAvailable", False),
            "kids_pool": family.get("hasKidsPool", False),
            "kids_club": family.get("hasKidsClub", False),
        }

        # ── Children policy ─────────────────────────────────────────
        personalized = content.get("personalizedInformation", {}) or {}
        children_policy = personalized.get("childrenFreePolicy", {}) or {}

        # ── Local information (landmarks) ───────────────────────────
        local_info = content.get("localInformation", {}) or {}
        landmarks_data = local_info.get("landmarks", {}) or {}
        landmarks = {
            "transportation": [
                {"name": lm.get("landmarkName", ""), "distance_m": lm.get("distanceInM", 0)}
                for lm in (landmarks_data.get("transportation", []) or [])
            ],
            "top_landmarks": [
                {"name": lm.get("landmarkName", ""), "distance_m": lm.get("distanceInM", 0)}
                for lm in (landmarks_data.get("topLandmark", []) or [])
            ],
            "beach": [
                {"name": lm.get("landmarkName", ""), "distance_m": lm.get("distanceInM", 0)}
                for lm in (landmarks_data.get("beach", []) or [])
            ],
        }
        has_airport_transfer = local_info.get("hasAirportTransfer", False)

        # ── Highlight ───────────────────────────────────────────────
        highlight = content.get("highlight", {}) or {}
        city_center = highlight.get("cityCenter", {}) or {}
        city_center_distance = city_center.get("distanceFromCityCenter", 0)
        has_nearby_transport = highlight.get("hasNearbyPublicTransportation", False)

        favorite_features = []
        ff_data = highlight.get("favoriteFeatures", {}) or {}
        for feat in (ff_data.get("features", []) or []):
            favorite_features.append({
                "title": feat.get("title", ""),
                "category": feat.get("category", ""),
            })

        # ── Engagement ──────────────────────────────────────────────
        engagement = content.get("propertyEngagement", {}) or {}
        engagement_data = {
            "last_booking": engagement.get("lastBooking", ""),
            "people_looking": engagement.get("peopleLooking", 0),
            "today_bookings": engagement.get("todayBooking", 0),
        }

        # ── Non-hotel accommodation (apartments, villas, etc.) ──────
        nha = content.get("nonHotelAccommodation", {}) or {}
        nha_rooms = nha.get("masterRooms", []) or []
        nha_info = {}
        if nha_rooms:
            mr = nha_rooms[0] if isinstance(nha_rooms, list) else {}
            nha_info = {
                "bathrooms": mr.get("noOfBathrooms", 0),
                "bedrooms": mr.get("noOfBedrooms", 0),
                "beds": mr.get("noOfBeds", 0),
                "room_size_sqm": mr.get("roomSizeSqm", 0),
                "facilities": mr.get("highlightedFacilities", []),
            }
        host_level = (nha.get("hostLevel", {}) or {}).get("name", "")
        supports_long_stay = nha.get("supportedLongStay", False)

        # ── Hotel character ─────────────────────────────────────────
        character = info.get("hotelCharacter", {}) or {}
        hotel_tags = []
        if character.get("hotelTag"):
            hotel_tags.append(character["hotelTag"].get("name", ""))
        hotel_views = []
        if character.get("hotelView"):
            hotel_views.append(character["hotelView"].get("name", ""))

        # ── Enrichment (selling points, popularity, room info) ──────
        tsp_raw = enrichment.get("topSellingPoint", []) or []
        top_selling_point = ""
        if isinstance(tsp_raw, list) and tsp_raw:
            top_selling_point = tsp_raw[0].get("value", "") if isinstance(tsp_raw[0], dict) else ""
        elif isinstance(tsp_raw, dict):
            top_selling_point = tsp_raw.get("value", "")
        is_popular = enrichment.get("isPopular", False)

        booking_history = enrichment.get("bookingHistory", {}) or {}
        booking_count = 0
        for bc in (booking_history.get("bookingCount", []) or []):
            booking_count = max(booking_count, int(bc.get("count", 0) or 0))

        room_info = enrichment.get("roomInformation", {}) or {}
        cheapest_room_size = room_info.get("cheapestRoomSizeSqm", 0)
        room_facilities = [
            {"name": f.get("propertyFacilityName", ""), "symbol": f.get("symbol", "")}
            for f in (room_info.get("facilities", []) or [])
        ]

        # ── Promotions ──────────────────────────────────────────────
        pulse = pricing.get("pulseCampaignMetadata", {}) or {}
        promotion_badge = pulse.get("campaignBadgeText", "")
        promotion_desc = pulse.get("campaignBadgeDescText", "")
        deal_expiry = pulse.get("dealExpiryTime", "")
        is_insider_deal = pricing.get("isInsiderDeal", False)

        # ── Renovation info ─────────────────────────────────────────
        remarks = info.get("remarks", {}) or {}
        renovation = remarks.get("renovationInfo", {}) or {}
        renovation_year = renovation.get("year", "")

        # ── Build amenities string from favorite features ───────────
        amenities_list = [f["title"] for f in favorite_features if f.get("title")]
        amenities_str = ", ".join(amenities_list[:10])

        # ── Atmospheres ─────────────────────────────────────────────
        atmospheres = [a.get("name", "") for a in (info.get("atmospheres", []) or []) if a.get("name")]

        # ══════════════════════════════════════════════════════════════
        # Build final hotel dict
        # ══════════════════════════════════════════════════════════════
        return {
            # Core fields (matching HOTEL_COMMON_COLUMNS)
            "hotel_name": name,
            "hotel_address": hotel_address,
            "city": city_name,
            "country": country_name,
            "latitude": lat,
            "longitude": lng,
            "star_rating": stars,
            "guest_rating": round(guest_rating, 1),
            "review_count": review_count,
            "check_in": check_in,
            "check_out": check_out,
            "nights": nights,
            "room_type": "",
            "board_type": "",
            "price": round(price_total, 2),
            "price_per_night": round(price_ppn, 2),
            "currency": price_currency,
            "booking_provider": "Agoda",
            "cancellation": cancellation,
            "amenities": amenities_str,
            "deep_link": deep_link,
            "image_url": image_url,

            # Extended data
            "property_id": property_id,
            "property_type": property_type,
            "accommodation_type": accommodation_type,
            "country_code": country_code,
            "area_name": area_name,
            "is_sustainable": is_sustainable,
            "award_year": award_year,
            "original_price": round(original_price, 2),
            "crossed_out_price": round(crossed_out_price, 2),
            "discount_percent": round(discount_percent, 2),
            "cashback_percent": round(cashback_percent, 2),
            "rooms_available": room_available,
            "is_available": is_available,
            "free_cancellation_date": free_cancellation_date,
            "pay_later": pay_later,
            "pay_at_hotel": pay_at_hotel,
            "no_credit_card": no_credit_card,
            "is_easy_cancel": is_easy_cancel,
            "supplier_name": supplier_name,
            "facility_ids": facility_ids,
            "family_features": family_features,
            "children_policy": children_policy,
            "landmarks": landmarks,
            "has_airport_transfer": has_airport_transfer,
            "city_center_distance": city_center_distance,
            "has_nearby_transport": has_nearby_transport,
            "favorite_features": favorite_features,
            "engagement": engagement_data,
            "hotel_tags": hotel_tags,
            "hotel_views": hotel_views,
            "atmospheres": atmospheres,
            "top_selling_point": top_selling_point,
            "is_popular": is_popular,
            "booking_count": booking_count,
            "cheapest_room_size_sqm": cheapest_room_size,
            "room_facilities": room_facilities,
            "promotion_badge": promotion_badge,
            "promotion_desc": promotion_desc,
            "deal_expiry": deal_expiry,
            "is_insider_deal": is_insider_deal,
            "renovation_year": renovation_year,
            "all_images": all_images[:10],
            "review_snippets": review_snippets[:5],
            "nha_info": nha_info,
            "host_level": host_level,
            "supports_long_stay": supports_long_stay,
        }


if __name__ == "__main__":
    import argparse, sys, os, logging

    # Add metasearch dir to path so 'core' module is importable
    metasearch_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if metasearch_dir not in sys.path:
        sys.path.insert(0, metasearch_dir)

    parser = argparse.ArgumentParser(description="Agoda Hotels GraphQL Scraper")
    parser.add_argument("--city", default="dubai", help="City name (default: dubai)")
    parser.add_argument("--checkin", default="2026-03-15", help="Check-in date YYYY-MM-DD")
    parser.add_argument("--checkout", default="2026-03-16", help="Check-out date YYYY-MM-DD")
    parser.add_argument("--adults", type=int, default=2, help="Adults per room (default: 2)")
    parser.add_argument("--rooms", type=int, default=1, help="Number of rooms (default: 1)")
    parser.add_argument("--currency", default="USD", help="Currency code (default: USD)")
    parser.add_argument("--output", "-o", default=None,
                        help="Save results to file (supports .xlsx, .csv, .json)")
    parser.add_argument("--pages", type=int, default=1,
                        help="Pages to fetch (default: 1, 0=all available)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    scraper = AgodaHotelsScraper()
    results = scraper.search(
        args.city, args.checkin, args.checkout,
        adults=args.adults, rooms=args.rooms, currency=args.currency,
        max_pages=args.pages,
    )

    # ── Console output ──────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f" Agoda Hotels -- {args.city.title()} | {args.checkin} to {args.checkout} | {len(results)} results")
    print(f"{'='*90}")
    for i, h in enumerate(results[:20]):
        cancel = h.get("cancellation", "")[:35]
        print(
            f"  {i+1:2d}. {h['hotel_name'][:45]:45s} | {h['star_rating']}* "
            f"| {h['guest_rating']:4.1f}/10 "
            f"| {h['currency']} {h['price_per_night']:8.2f}/n | {cancel}"
        )

    # ── Save to file ────────────────────────────────────────────────
    if args.output and results:
        import pandas as pd

        df = pd.DataFrame(results)
        ext = os.path.splitext(args.output)[1].lower()

        if ext == ".xlsx":
            df.to_excel(args.output, index=False, engine="openpyxl")
        elif ext == ".csv":
            df.to_csv(args.output, index=False, encoding="utf-8-sig")
        elif ext == ".json":
            df.to_json(args.output, orient="records", indent=2, force_ascii=False)
        else:
            # Default to CSV if unrecognised extension
            df.to_csv(args.output, index=False, encoding="utf-8-sig")

        abs_path = os.path.abspath(args.output)
        print(f"\n  [OK] Saved {len(results)} hotels to: {abs_path}")
    elif args.output and not results:
        print("\n  [!] No results to save.")
