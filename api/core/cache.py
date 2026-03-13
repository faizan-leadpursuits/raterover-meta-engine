"""
In-memory TTL cache for search results.

Prevents re-scraping when the same route+date is searched within a window.
Thread-safe with LRU eviction.

Usage:
    cache = SearchCache(ttl_seconds=900, max_entries=500)
    key = cache.make_key("jetcost", {"origin": "LHR", "destination": "CAI", ...})
    
    cached = cache.get(key)
    if cached is not None:
        return cached
    
    result = adapter.search(...)
    cache.set(key, result)
"""

import time
import json
import io
import threading
import logging
from collections import OrderedDict

import pandas as pd

logger = logging.getLogger(__name__)


class SearchCache:
    """
    Search results cache.
    Uses Redis if redis_url is provided (distributed), otherwise a 
    thread-safe in-memory OrderedDict (single-worker fallback).
    """

    def __init__(self, ttl_seconds: int = 900, max_entries: int = 500, redis_url: str = None):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self.redis = None
        
        # Local stats counters
        self._hits = 0
        self._misses = 0

        if redis_url:
            try:
                import redis
                self.redis = redis.from_url(redis_url, socket_connect_timeout=2)
                self.redis.ping()
                logger.info("SearchCache connected to Redis")
            except Exception as e:
                logger.warning("SearchCache failed to connect to Redis: %s", getattr(e, "message", str(e)))
                self.redis = None

        if self.redis is None:
            self._cache: OrderedDict[str, tuple[float, pd.DataFrame]] = OrderedDict()
            self._lock = threading.Lock()

    @staticmethod
    def make_key(provider: str, params: dict) -> str:
        """
        Build a cache key from provider name and search parameters.
        Only includes core flight-search params (not filter params).
        """
        parts = [
            provider,
            params.get("origin", ""),
            params.get("destination", ""),
            params.get("depart_date", ""),
            params.get("return_date", "") or "",
            str(params.get("adults", 1)),
            str(params.get("children", 0)),
            str(params.get("infants", 0)),
            params.get("currency", "USD"),
            params.get("cabin_class", "COACH"),
        ]
        return "cache:search:" + "|".join(parts)

    def get(self, key: str) -> pd.DataFrame | None:
        """
        Get a cached result if it exists and hasn't expired.
        Returns None on miss or expiry.
        """
        if self.redis:
            try:
                raw = self.redis.get(key)
                if raw:
                    self._hits += 1
                    # Convert parquet bytes back to DataFrame
                    return pd.read_parquet(io.BytesIO(raw))
                self._misses += 1
                return None
            except Exception as e:
                logger.debug("Redis cache GET failed: %s", e)
                # Fallthrough to local stats/miss
                self._misses += 1
                return None

        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            timestamp, df = self._cache[key]
            if time.time() - timestamp > self.ttl:
                # Expired — remove and return miss
                del self._cache[key]
                self._misses += 1
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            self._hits += 1
            return df.copy()

    def set(self, key: str, df: pd.DataFrame):
        """Store a result in the cache. Only caches non-empty results."""
        if df is None or df.empty:
            return

        if self.redis:
            try:
                # Convert DataFrame to parquet bytes for efficient storage
                buf = io.BytesIO()
                df.to_parquet(buf, compression="snappy")
                self.redis.setex(key, self.ttl, buf.getvalue())
                
                # Best-effort eviction bounding in Redis (evicting older keys if over max)
                pool_size = self.redis.dbsize()
                if pool_size > self.max_entries * 2: # Loose bound for Redis
                   pass # Let TTL handle Redis eviction normally
                   
                return
            except Exception as e:
                logger.debug("Redis cache SET failed: %s", e)
                # Fallthrough to memory

        with self._lock:
            # Update existing or add new
            self._cache[key] = (time.time(), df.copy())
            self._cache.move_to_end(key)

            # Evict oldest if over capacity
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def invalidate(self, key: str):
        """Remove a specific entry from the cache."""
        if self.redis:
            try:
                self.redis.delete(key)
                return
            except Exception:
                pass
                
        with self._lock:
            self._cache.pop(key, None)

    def clear(self):
        """Clear the entire cache."""
        if self.redis:
            try:
                # Scan and delete all search cache keys
                cursor = 0
                while True:
                    cursor, keys = self.redis.scan(cursor, match="cache:search:*", count=100)
                    if keys:
                        self.redis.delete(*keys)
                    if cursor == 0:
                        break
            except Exception as e:
                logger.warning("Redis clear failed: %s", e)
                
        if hasattr(self, "_cache"):
            with self._lock:
                self._cache.clear()
        
        self._hits = 0
        self._misses = 0

    @property
    def size(self) -> int:
        """Number of entries currently in cache."""
        if self.redis:
            try:
                # Approximation
                return len(self.redis.keys("cache:search:*"))
            except Exception:
                return 0
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """Cache hit rate as percentage."""
        total = self._hits + self._misses
        if total == 0:
            return 0.0
        return self._hits / total * 100

    def stats(self) -> dict:
        """Get cache statistics."""
        return {
            "entries": self.size,
            "max_entries": self.max_entries,
            "ttl_seconds": self.ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self.hit_rate:.1f}%",
        }


class CacheManager:
    """
    Generic key/value TTL cache for API payloads.

    Compatibility layer used by routes that cache serialized JSON blobs
    (for example flight/search responses), unlike SearchCache which stores
    pandas DataFrames.
    """

    def __init__(self, ttl_seconds: int = 900, max_entries: int = 500, redis_url: str = None):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self.redis = None
        self._hits = 0
        self._misses = 0

        if redis_url:
            try:
                import redis
                self.redis = redis.from_url(redis_url, socket_connect_timeout=2)
                self.redis.ping()
                logger.info("CacheManager connected to Redis")
            except Exception as e:
                logger.warning("CacheManager failed to connect to Redis: %s", getattr(e, "message", str(e)))
                self.redis = None

        if self.redis is None:
            self._cache: OrderedDict[str, tuple[float, str]] = OrderedDict()
            self._lock = threading.Lock()

    @staticmethod
    def make_key(**params) -> str:
        """
        Build a deterministic cache key from query params.
        """
        parts = [f"{k}={params.get(k, '')}" for k in sorted(params.keys())]
        return "cache:api:" + "|".join(parts)

    def get(self, key: str):
        if self.redis:
            try:
                raw = self.redis.get(key)
                if raw is None:
                    self._misses += 1
                    return None
                self._hits += 1
                return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
            except Exception as e:
                logger.debug("Redis generic cache GET failed: %s", e)
                self._misses += 1
                return None

        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            ts, value = self._cache[key]
            if time.time() - ts > self.ttl:
                del self._cache[key]
                self._misses += 1
                return None

            self._cache.move_to_end(key)
            self._hits += 1
            return value

    def set(self, key: str, value):
        if value is None:
            return

        if not isinstance(value, str):
            value = json.dumps(value, default=str)

        if self.redis:
            try:
                self.redis.setex(key, self.ttl, value)
                return
            except Exception as e:
                logger.debug("Redis generic cache SET failed: %s", e)

        with self._lock:
            self._cache[key] = (time.time(), value)
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def clear(self):
        if self.redis:
            try:
                cursor = 0
                while True:
                    cursor, keys = self.redis.scan(cursor, match="cache:api:*", count=100)
                    if keys:
                        self.redis.delete(*keys)
                    if cursor == 0:
                        break
            except Exception as e:
                logger.warning("Redis generic cache clear failed: %s", e)

        if hasattr(self, "_cache"):
            with self._lock:
                self._cache.clear()

        self._hits = 0
        self._misses = 0

    @property
    def size(self) -> int:
        if self.redis:
            try:
                return len(self.redis.keys("cache:api:*"))
            except Exception:
                return 0
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return (self._hits / total * 100) if total else 0.0

    def info(self) -> dict:
        return {
            "type": "redis" if self.redis else "memory",
            "entries": self.size,
            "ttl_seconds": self.ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self.hit_rate:.1f}%",
        }
