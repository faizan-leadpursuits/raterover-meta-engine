"""
Unified hotel searcher â€” orchestrates parallel/sequential search across all hotel providers.

Includes:
- Circuit breaker: auto-disables providers after consecutive failures
- Result cache: prevents re-scraping within TTL window
- Centralized retry: exponential backoff with fresh proxy per attempt
- Per-provider timeouts: configured in core/resilience.py
"""

import time
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

logger = logging.getLogger(__name__)

from .registry import discover_providers
from .hotel_merger import HotelMerger
from .hotel_filters import apply_hotel_filters
from .hotel_filter_params import HotelFilterParams
from .exporter import ResultExporter
from .resilience import ProviderHealth
from .cache import SearchCache


class HotelSearcher:
    """
    Orchestrates multi-provider hotel search with parallel execution,
    deduplication, post-search filtering, and export.

    Usage:
        searcher = HotelSearcher()
        df = searcher.search(city="London", check_in="2026-04-01", check_out="2026-04-05")
    """

    def __init__(self, sources=None, timeout=90, parallel=True,
                 proxy_manager=None,
                 cache_ttl=900, cache_enabled=False):
        """
        Args:
            sources: List of provider names to use (None = all discovered)
            timeout: Global timeout for parallel execution (max wait)
            parallel: Run providers in parallel (True) or sequential (False)
            proxy_manager: Optional ProxyManager for proxy rotation
            cache_ttl: Cache time-to-live in seconds (default: 15 min)
            cache_enabled: Enable/disable result caching
        """
        all_adapters = discover_providers(domain="hotels")

        if sources:
            # Warn about unknown sources
            unknown = [s for s in sources if s not in all_adapters]
            if unknown:
                logger.warning("Unknown sources (skipped): %s (Available: %s)", 
                               ", ".join(unknown), ", ".join(all_adapters.keys()))

            self.adapters = {
                name: all_adapters[name]()
                for name in sources
                if name in all_adapters
            }
        else:
            self.adapters = {name: cls() for name, cls in all_adapters.items()}

        # Inject proxy manager into all adapters
        self.proxy_manager = proxy_manager
        if proxy_manager:
            for adapter in self.adapters.values():
                adapter.set_proxy_manager(proxy_manager)

        self.timeout = timeout
        self.parallel = parallel
        self.merger = HotelMerger()
        self.exporter = ResultExporter(output_dir="results/hotels")

        # â”€â”€ Resilience infrastructure â”€â”€
        self.health = ProviderHealth()
        self.cache = SearchCache(ttl_seconds=cache_ttl) if cache_enabled else None

    def search(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int = 2,
        children: int = 0,
        rooms: int = 1,
        currency: str = "USD",
        # â”€â”€ Post-search filters â”€â”€
        max_price: float = None,
        max_price_per_night: float = None,
        min_stars: int = None,
        max_stars: int = None,
        min_rating: float = None,
        min_reviews: int = None,
        board_type: str = None,
        free_cancellation_only: bool = False,
        include_amenities: list = None,
        sort_by: str = "price",
        **extra_kwargs,
    ) -> pd.DataFrame:
        """
        Execute unified hotel search across all active providers.

        Core params are passed to each adapter's search().
        Filter params are applied post-search on merged results.
        """
        if not self.adapters:
            print("  [ERROR] No hotel adapters loaded. Check your sources config.")
            return pd.DataFrame()

        # â”€â”€ Header â”€â”€
        available = [n for n in self.adapters if self.health.is_available(n)]
        skipped = [n for n in self.adapters if not self.health.is_available(n)]

        if skipped:
            logger.info("Skipped: %s (circuit open)", ", ".join(skipped))
        logger.info("Search mode: %s", 'Parallel' if self.parallel else 'Sequential')
        if self.cache:
            logger.info("Cache: ON (TTL=%ss, %s entries)", self.cache.ttl, self.cache.size)

        # â”€â”€ Build HotelFilterParams for pre-search filtering â”€â”€
        filters = HotelFilterParams(
            star_rating=list(range(min_stars, 6)) if min_stars else [],
            min_guest_rating=min_rating,
            max_price=max_price,
            max_price_per_night=max_price_per_night,
            free_cancellation=free_cancellation_only,
            breakfast_included=(board_type.lower() == "breakfast" if board_type else False),
            amenities=include_amenities or [],
            sort_by=sort_by,
            min_reviews=min_reviews,
        )

        # â”€â”€ Build search kwargs â”€â”€
        search_kwargs = dict(
            city=city,
            check_in=check_in,
            check_out=check_out,
            adults=adults,
            children=children,
            rooms=rooms,
            currency=currency,
            filters=filters if not filters.is_empty() else None,
            **extra_kwargs,
        )

        # â”€â”€ Execute searches â”€â”€
        if self.parallel:
            results = self._search_parallel(search_kwargs)
        else:
            results = self._search_sequential(search_kwargs)

        # â”€â”€ Report per-source results â”€â”€
        # â”€â”€ Report per-source results â”€â”€
        logger.info("PER-SOURCE RESULTS")
        for name, df in results.items():
            if df.empty:
                state = self.health.circuit_state(name)
                if "OPEN" in state:
                    logger.info("  %s -> Skipped (circuit %s)", name, state)
                else:
                    logger.info("  %s -> No results", name)
            else:
                cheapest = df["price"].min() if "price" in df.columns else 0
                logger.info("  %s -> %d hotels (cheapest: $%s)", name, len(df), f"{cheapest:,.0f}")

        # â”€â”€ Merge â”€â”€
        dfs = [df for df in results.values() if not df.empty]
        merged = self.merger.merge(dfs)

        # â”€â”€ Apply post-search filters â”€â”€
        if not merged.empty:
            before = len(merged)
            merged = apply_hotel_filters(
                merged,
                max_price=max_price,
                max_price_per_night=max_price_per_night,
                min_stars=min_stars,
                max_stars=max_stars,
                min_rating=min_rating,
                min_reviews=min_reviews,
                board_type=board_type,
                free_cancellation_only=free_cancellation_only,
                include_amenities=include_amenities,
                sort_by=sort_by,
            )
            filtered = before - len(merged)
            if filtered > 0:
                logger.info("Post-search filters removed %d hotels (%d remaining)", filtered, len(merged))

        # â”€â”€ Print health after search â”€â”€
        # self.health.print_health()  # Avoid spamming stdout directly

        return merged

    # â”€â”€ Resilient adapter execution â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _run_adapter_resilient(self, name: str, adapter, kwargs: dict) -> tuple[str, pd.DataFrame]:
        """
        Execute a single adapter with:
        1. Circuit breaker check
        2. Cache lookup
        3. Retry with exponential backoff + fresh proxy per attempt
        4. Health recording
        """
        config = self.health.get_config(name)

        # 1. Circuit breaker â€” skip if open
        if not self.health.is_available(name):
            return name, pd.DataFrame()

        # 2. Cache lookup
        if self.cache:
            cache_key = self.cache.make_key(name, kwargs)
            cached = self.cache.get(cache_key)
            if cached is not None:
                logger.info("[%s] âš¡ Cache hit (%d hotels)", name, len(cached))
                self.health.record_success(name, elapsed=0.0)
                return name, cached

        # 3. Execute with retry
        last_error = ""
        for attempt in range(config.max_retries + 1):
            if attempt > 0:
                backoff = min(2 ** attempt, 8)
                logger.info("[%s] Retry %d/%d (backoff %ds)...", name, attempt, config.max_retries, backoff)
                time.sleep(backoff)

                # Rotate proxy IP for retry if configured
                if config.retry_with_fresh_proxy and self.proxy_manager:
                    adapter.set_proxy_manager(self.proxy_manager)

            t0 = time.time()
            try:
                df = adapter.search(**kwargs)
                elapsed = time.time() - t0
                hotels = len(df) if df is not None and not df.empty else 0

                # Always record success if the adapter didn't raise an exception
                self.health.record_success(name, elapsed)
                if hotels > 0:
                    if self.cache:
                        self.cache.set(cache_key, df)
                    return name, df
                else:
                    return name, df  # Return empty df immediately, do not fail

            except Exception as e:
                elapsed = time.time() - t0
                last_error = str(e)[:200]
                self.health.record_failure(name, last_error)
                logger.warning("[%s] âœ— Error (attempt %d): %s", name, attempt + 1, last_error[:80])

        return name, pd.DataFrame()

    # â”€â”€ Parallel execution â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _search_parallel(self, kwargs: dict) -> dict:
        """Run all adapters in parallel using ThreadPoolExecutor."""
        results = {}

        with ThreadPoolExecutor(max_workers=len(self.adapters)) as executor:
            futures = {
                executor.submit(self._run_adapter_resilient, name, adapter, kwargs): name
                for name, adapter in self.adapters.items()
            }
            pending = set(futures.keys())
            try:
                for future in as_completed(futures, timeout=self.timeout):
                    pending.discard(future)
                    try:
                        name, df = future.result()
                        results[name] = df
                    except Exception as e:
                        name = futures[future]
                        logger.warning("[%s] Timeout/Error: %s", name, e)
                        self.health.record_failure(name, str(e))
                        results[name] = pd.DataFrame()
            except TimeoutError:
                logger.warning(
                    "Hotel parallel search timeout after %ss (%d unfinished provider task(s))",
                    self.timeout, len(pending)
                )
                for future in pending:
                    name = futures[future]
                    self.health.record_failure(name, f"Provider timeout after {self.timeout}s")
                    results[name] = pd.DataFrame()
                    future.cancel()

        return results

    # â”€â”€ Sequential execution â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _search_sequential(self, kwargs: dict) -> dict:
        """Run all adapters sequentially."""
        results = {}
        for name, adapter in self.adapters.items():
            name, df = self._run_adapter_resilient(name, adapter, kwargs)
            results[name] = df
        return results