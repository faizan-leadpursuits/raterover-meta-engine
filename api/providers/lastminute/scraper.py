import json
import time
import random
import uuid
import hashlib
import re
import sys
from pathlib import Path
from curl_cffi import requests

_CONFIG_PATH = Path(__file__).parent / "config.json"

def new_trace_id() -> str:
    return str(uuid.uuid4())

def new_traceparent() -> str:
    trace = hashlib.md5(uuid.uuid4().bytes).hexdigest() + hashlib.md5(uuid.uuid4().bytes).hexdigest()
    span  = hashlib.md5(uuid.uuid4().bytes).hexdigest()[:16]
    return f"00-{trace[:32]}-{span}-01"

def new_seed() -> str:
    return hashlib.md5(uuid.uuid4().bytes).hexdigest()[:8]

def new_search_id() -> str:
    ts = str(int(time.time() * 1000))
    return f"t{ts}{uuid.uuid4().hex[:8]}"

def extract_mpt_token(html: str, cookies: dict) -> str:
    mpt = cookies.get("__mpt", "")
    if mpt and mpt.startswith("eyJ"):
        return mpt
    jwt_pat = r'["\']?(eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)["\']?'
    for m in re.finditer(jwt_pat, html):
        token = m.group(1)
        try:
            import base64
            payload_b64 = token.split(".")[1] + "=="
            payload = base64.urlsafe_b64decode(payload_b64).decode("utf-8", errors="ignore")
            if "LASTMINUTE" in payload or "GUEST" in payload or "sa.br" in payload:
                return token
        except Exception:
            continue
    return ""

def extract_search_ids(html: str) -> tuple[str, str, str]:
    search_id, vc_search_id, seed = "", "", ""
    m = re.search(r'"searchId"\s*:\s*"(t\d{13}[a-f0-9]{8})"', html)
    if m: search_id = m.group(1)
    m = re.search(r'"vcSearchId"\s*:\s*"?(\d{7,12})"?', html)
    if m: vc_search_id = m.group(1)
    m = re.search(r'"seed"\s*:\s*"([0-9a-f]{8})"', html, re.IGNORECASE)
    if m: seed = m.group(1)
    return search_id, vc_search_id, seed


class LastminuteScraper:
    def __init__(self):
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {
                "base_url": "https://www.lastminute.com",
                "search_endpoint": "https://www.lastminute.com/rv/api/v1/hotels/search",
                "headers_browse": {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "accept-language": "en-US,en;q=0.9",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                },
                "headers_api": {
                    "accept": "application/json",
                    "content-type": "application/json",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                },
                "scraper": {"rate_limit_sleep": 1.5, "max_pages": 3, "timeout": 30, "impersonate": "chrome142"},
                "search": {"businessProfileId": "1", "source": "LASTMINUTE_IT", "bfSubSource": "sa.br", "pageType": "SEARCH", "searchMode": "HOTELS", "sort": "price"},
                "places": {"london": "47554", "paris": "47795", "new york": "44417", "dubai": "46447", "rome": "47894", "barcelona": "47162", "berlin": "47252", "amsterdam": "47081", "tokyo": "41758", "istanbul": "47592"},
            }

        self.base_url = self.config["base_url"]
        self.search_endpoint = self.config["search_endpoint"]
        self.headers_browse = self.config["headers_browse"]
        self.headers_api = self.config["headers_api"]
        self.sleep_time = self.config["scraper"]["rate_limit_sleep"]
        self.max_pages = self.config["scraper"]["max_pages"]
        self.timeout = self.config["scraper"]["timeout"]

        impersonate = self.config["scraper"]["impersonate"]
        self.session = requests.Session(verify=False, impersonate=impersonate)

        # Dynamic State
        self.mpt_token = ""
        self.search_id = new_search_id()
        self.vc_search_id = str(random.randint(100000000, 999999999))
        self.seed = new_seed()
        self.trace_id = new_trace_id()

    def _apply_proxy(self, proxy_manager=None, proxy=None):
        try:
            from core.proxy_helpers import build_curl_cffi_proxy_list
        except Exception:
            try:
                from metasearch.core.proxy_helpers import build_curl_cffi_proxy_list
            except Exception:
                root_dir = str(Path(__file__).resolve().parents[3])
                if root_dir not in sys.path:
                    sys.path.insert(0, root_dir)
                from metasearch.core.proxy_helpers import build_curl_cffi_proxy_list

        proxy_configs = build_curl_cffi_proxy_list(proxy_manager=proxy_manager, proxy=proxy)
        _, proxy_url = proxy_configs[0]
        self.session.proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
        print(f"  [lastminute] Proxy route: {proxy_configs[0][0]} -> {proxy_url or 'direct'}")

    def get_destination_id(self, city: str) -> str:
        city = city.lower().strip()
        pid = self.config.get("places", {}).get(city)
        if not pid:
            print(f"[lastminute] Warning: Destination ID for '{city}' not found. Defaulting to Paris.")
            return self.config["places"]["paris"]
        return pid

    def harvest_cookies(self):
        print("  [lastminute] Visiting homepage for tokens...")
        resp = self.session.get(self.base_url + "/", headers=self.headers_browse, timeout=self.timeout, allow_redirects=True)
        print(f"  [lastminute] Homepage Status: {resp.status_code}")
        
        cookies_dict = dict(self.session.cookies)
        print(f"  [lastminute] Cookies: {list(cookies_dict.keys())}")
        
        mpt = extract_mpt_token(resp.text, cookies_dict)
        if mpt: self.mpt_token = mpt
        time.sleep(random.uniform(1.0, 2.0))

    def visit_search_page(self, destination, check_in, check_out, adults):
        url = self._build_search_url(1, destination, check_in, check_out, adults)
        print("  [lastminute] Navigating to search page...")
        headers = dict(self.headers_browse)
        headers["referer"] = self.base_url + "/"
        headers["sec-fetch-site"] = "same-origin"

        resp = self.session.get(url, headers=headers, timeout=self.timeout, allow_redirects=True)
        s_id, vc_id, seed = extract_search_ids(resp.text)
        if s_id: self.search_id = s_id
        if vc_id: self.vc_search_id = vc_id
        if seed: self.seed = seed

        mpt = extract_mpt_token(resp.text, dict(self.session.cookies))
        if mpt: self.mpt_token = mpt
        elif not self.mpt_token:
            self.mpt_token = self.session.cookies.get("__mpt", "")
        time.sleep(random.uniform(1.0, 2.0))

    def _build_search_url(self, page, destination, check_in, check_out, adults):
        sch = self.config["search"]
        return (
            f"{self.base_url}/s/tsx"
            f"?businessProfileId={sch['businessProfileId']}"
            f"&seed={self.seed}"
            f"&source={sch['source']}"
            f"&searchId={self.search_id}"
            f"&bfSubSource={sch['bfSubSource']}"
            f"&pageType={sch['pageType']}"
            f"&dateFrom={check_in}"
            f"&dateTo={check_out}"
            f"&searchMode={sch['searchMode']}"
            f"&sort={sch['sort']}"
            f"&destination={destination}"
            f"&adults={adults}"
            f"&vcSearchId={self.vc_search_id}"
            f"&page={page}"
        )

    def _build_payload(self, page, destination, check_in, check_out, adults):
        sch = self.config["search"]
        return {
            "searchParams": {
                "businessProfileId": sch["businessProfileId"],
                "seed": self.seed,
                "source": sch["source"],
                "searchId": self.search_id,
                "bfSubSource": sch["bfSubSource"],
                "pageType": sch["pageType"],
                "dateFrom": check_in,
                "dateTo": check_out,
                "searchMode": sch["searchMode"],
                "sort": sch["sort"],
                "destination": destination,
                "adults": str(adults),
                "vcSearchId": self.vc_search_id,
                "page": str(page),
            }
        }

    def _fetch_page(self, page, destination, check_in, check_out, adults):
        headers = dict(self.headers_api)
        headers["referer"] = self._build_search_url(page, destination, check_in, check_out, adults)
        headers["traceparent"] = new_traceparent()
        headers["x-bf-tracing-traceid"] = new_trace_id()
        headers["x-bf-tracing-spanid"] = uuid.uuid4().hex[:7]
        payload = self._build_payload(page, destination, check_in, check_out, adults)

        if self.mpt_token:
            headers["x-lm-multipurpose-token"] = self.mpt_token
            
        print(f"  [lastminute] Token present: {bool(self.mpt_token)} | referer: {headers['referer']}")

        import curl_cffi.requests.exceptions
        try:
            resp = self.session.post(self.search_endpoint, headers=headers, json=payload, timeout=self.timeout)
        except curl_cffi.requests.exceptions.Timeout:
            print("  [lastminute] Timeout during API POST request.")
            return {}
        except Exception as e:
            print(f"  [lastminute] Exception during POST: {e}")
            return {}

        if resp.status_code != 200:
            print(f"  [lastminute] HTTP Error {resp.status_code} : {resp.text[:200]}")
            return {}

        new_mpt = self.session.cookies.get("__mpt", "")
        if new_mpt and new_mpt != self.mpt_token:
            self.mpt_token = new_mpt

        try:
            data = resp.json()
        except Exception as e:
            print(f"  [lastminute] Invalid JSON response: {e}")
            return {}
        # print(f"  [lastminute] API response keys: {list(data.keys())}")
        pd = data.get("productsData", {})
        if pd:
            print(f"  [lastminute] productsData keys: {list(pd.keys())}")
            dc = pd.get("dealCards")
            print(f"  [lastminute] dealCards type: {type(dc).__name__}, len: {len(dc) if dc else 0}")
        return data

    def search(self, city: str, check_in: str, check_out: str, adults: int = 2, rooms: int = 1, proxy_manager=None, proxy=None, **kw) -> list:
        max_results = int(kw.get("max_results", 0))
        destination = self.get_destination_id(city)
        self._apply_proxy(proxy_manager=proxy_manager, proxy=proxy)

        self.harvest_cookies()
        self.visit_search_page(destination, check_in, check_out, adults)

        print(f"  [lastminute] Paginating API...")
        import time
        start_time = time.time()
        timeout_limit = 15  # ensure we return before 20s API cutoff
        
        all_hotels = []
        page = 1
        while page <= self.max_pages:
            if time.time() - start_time > timeout_limit:
                print(f"  [lastminute] Internal time limit reached ({timeout_limit}s), aborting further pages.")
                break
                
            data = self._fetch_page(page, destination, check_in, check_out, adults)
            
            hotels = []
            try:
                hotels = data.get("productsData", {}).get("dealCards", [])
            except Exception:
                pass

            if not hotels:
                break
            
            # Decorate for adapter
            for h in hotels:
                h["_search_meta"] = {
                    "city": city,
                    "check_in": check_in,
                    "check_out": check_out,
                    "adults": adults
                }

            all_hotels.extend(hotels)
            total = data.get("productsData", {}).get("productCounters", {}).get("productsCount", 0)
            print(f"  [lastminute] Page {page}: fetched {len(hotels)} / {total or '?'}")

            # Stop early if limit reached
            if max_results > 0 and len(all_hotels) >= max_results:
                print(f"  [lastminute] OK Found {len(all_hotels)} hotels")
                return all_hotels[:max_results]

            if len(all_hotels) >= total or len(hotels) < 10:
                break

            page += 1
            time.sleep(self.sleep_time)

        return all_hotels


if __name__ == "__main__":
    import json as _json

    scraper = LastminuteScraper()
    hotels = scraper.search("paris", "2026-04-01", "2026-04-07", adults=2)

    print(f"\n{'='*60}")
    print(f"  Total hotels collected: {len(hotels)}")
    print(f"{'='*60}")

    for i, card in enumerate(hotels[:5], 1):
        prod = card.get("product", {})
        acc  = prod.get("accommodation", {})
        rate = prod.get("rate", {}).get("price", {})
        print(f"  {i}. {acc.get('name', 'N/A')} | {acc.get('stars','')}* | {rate.get('currency','')}{rate.get('price','N/A')}")

    out = "lastminute_hotels.json"
    with open(out, "w", encoding="utf-8") as f:
        _json.dump(hotels, f, ensure_ascii=False, indent=2)
    print(f"\nFull data saved -> {out}")
