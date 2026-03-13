"""
Provider health tracking with circuit breaker pattern.

Tracks per-provider success/failure, response times, and automatically
disables flaky providers after consecutive failures.

Usage:
    health = ProviderHealth()
    health.configure("ita_matrix", timeout=60, max_retries=2)

    if health.is_available("ita_matrix"):
        try:
            result = adapter.search(...)
            health.record_success("ita_matrix", elapsed_seconds=25.3)
        except Exception as e:
            health.record_failure("ita_matrix", str(e))
"""

import time
import threading
from dataclasses import dataclass, field


@dataclass
class ProviderConfig:
    """Per-provider resilience configuration."""
    timeout: float = 30.0          # Max seconds to wait for response
    max_retries: int = 2           # Retry count with exponential backoff
    failure_threshold: int = 3     # Consecutive failures before circuit opens
    cooldown_seconds: float = 300  # Seconds to wait before retrying after circuit opens
    retry_with_fresh_proxy: bool = True  # Get new proxy IP per retry


@dataclass
class ProviderStats:
    """Runtime health statistics for a single provider."""
    total_calls: int = 0
    total_successes: int = 0
    total_failures: int = 0
    consecutive_failures: int = 0
    last_success_time: float = 0.0
    last_failure_time: float = 0.0
    last_error: str = ""
    circuit_open_since: float = 0.0  # 0 = circuit closed
    total_response_time: float = 0.0  # sum of successful response times
    last_response_time: float = 0.0

    @property
    def avg_response_time(self) -> float:
        if self.total_successes == 0:
            return 0.0
        return self.total_response_time / self.total_successes

    @property
    def success_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return self.total_successes / self.total_calls * 100


# ── Default provider configurations ──────────────────────────────────

DEFAULT_CONFIGS = {
    # ── Original providers ──
    "jetcost":        ProviderConfig(timeout=15, max_retries=1),
    "kiwi":           ProviderConfig(timeout=15, max_retries=1),
    "skyscanner":     ProviderConfig(timeout=20, max_retries=2),
    "skiplagged":     ProviderConfig(timeout=15, max_retries=3, retry_with_fresh_proxy=True),
    "trip.com":       ProviderConfig(timeout=20, max_retries=2),
    "ita_matrix":     ProviderConfig(timeout=20, max_retries=2, failure_threshold=5),
    "wego":           ProviderConfig(timeout=20, max_retries=3, retry_with_fresh_proxy=True),
    "aviasales":      ProviderConfig(timeout=20, max_retries=1),
    "google_flights": ProviderConfig(timeout=20, max_retries=1),
    # ── New providers ──
    "kayak":             ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "tripadvisor":       ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "momondo":           ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "cheapflights":      ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "travelsupermarket": ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "liligo":            ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "dohop":             ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "trabber":           ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "flygresor":         ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "swoodoo":           ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "idealo":            ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "netflights":        ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "fly.com":           ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    # ── Hotel comparison providers ──
    "kayak_hotels":      ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "trivago":           ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "hotelscombined":    ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "skyscanner_hotels": ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "wego_hotels":       ProviderConfig(timeout=20, max_retries=2, retry_with_fresh_proxy=True),
    "google_hotels":     ProviderConfig(timeout=20, max_retries=1),
}



import json

class ProviderHealth:
    """
    Circuit breaker + health tracker for all providers.
    Uses Redis backend if redis_url is provided, enabling distributed
    circuit breakers across multi-worker environments. Falls back to
    in-memory tracking if Redis is unavailable.
    """

    def __init__(self, redis_url: str = None):
        self._configs: dict[str, ProviderConfig] = {}
        
        # Memory fallbacks
        self._stats: dict[str, ProviderStats] = {}
        self._lock = threading.Lock()
        
        # Redis setup
        self.redis = None
        if redis_url:
            try:
                import redis
                self.redis = redis.from_url(redis_url, socket_connect_timeout=2)
                self.redis.ping()
            except Exception:
                self.redis = None

        # Load defaults
        for name, config in DEFAULT_CONFIGS.items():
            self._configs[name] = config
            self._stats[name] = ProviderStats()

    def configure(self, provider: str, **kwargs):
        """Override configuration for a specific provider."""
        if provider not in self._configs:
            self._configs[provider] = ProviderConfig(**kwargs)
        else:
            for k, v in kwargs.items():
                setattr(self._configs[provider], k, v)
        if provider not in self._stats:
            self._stats[provider] = ProviderStats()

    def get_config(self, provider: str) -> ProviderConfig:
        """Get configuration for a provider (or defaults)."""
        if provider not in self._configs:
            self._configs[provider] = ProviderConfig()
            self._stats[provider] = ProviderStats()
        return self._configs[provider]

    def get_stats(self, provider: str) -> ProviderStats:
        """Get runtime stats for a provider."""
        if self.redis:
            try:
                raw = self.redis.hgetall(f"health:stats:{provider}")
                if raw:
                    st = ProviderStats()
                    st.total_calls = int(raw.get(b"total_calls", 0))
                    st.total_successes = int(raw.get(b"total_successes", 0))
                    st.total_failures = int(raw.get(b"total_failures", 0))
                    st.consecutive_failures = int(raw.get(b"consecutive_failures", 0))
                    st.last_success_time = float(raw.get(b"last_success_time", 0.0))
                    st.last_failure_time = float(raw.get(b"last_failure_time", 0.0))
                    st.last_error = raw.get(b"last_error", b"").decode("utf-8")
                    st.circuit_open_since = float(raw.get(b"circuit_open_since", 0.0))
                    st.total_response_time = float(raw.get(b"total_response_time", 0.0))
                    st.last_response_time = float(raw.get(b"last_response_time", 0.0))
                    return st
            except Exception:
                pass
                
        # Memory fallback
        if provider not in self._stats:
            self._stats[provider] = ProviderStats()
        return self._stats[provider]

    # ── Circuit Breaker ──────────────────────────────────────────────

    def is_available(self, provider: str) -> bool:
        config = self.get_config(provider)
        stats = self.get_stats(provider)

        if stats.circuit_open_since == 0:
            return True  # Circuit CLOSED

        # Circuit is OPEN — check if cooldown has elapsed
        elapsed = time.time() - stats.circuit_open_since
        if elapsed >= config.cooldown_seconds:
            return True  # Transition to HALF_OPEN

        return False  # Still in cooldown

    def circuit_state(self, provider: str) -> str:
        stats = self.get_stats(provider)
        config = self.get_config(provider)

        if stats.circuit_open_since == 0:
            return "CLOSED"

        elapsed = time.time() - stats.circuit_open_since
        if elapsed >= config.cooldown_seconds:
            return "HALF_OPEN"

        remaining = config.cooldown_seconds - elapsed
        return f"OPEN ({remaining:.0f}s remaining)"

    # ── Record Outcomes ──────────────────────────────────────────────

    def record_success(self, provider: str, elapsed: float = 0.0):
        if self.redis:
            try:
                now = time.time()
                pipe = self.redis.pipeline()
                key = f"health:stats:{provider}"
                pipe.hincrby(key, "total_calls", 1)
                pipe.hincrby(key, "total_successes", 1)
                pipe.hset(key, "consecutive_failures", 0)
                pipe.hset(key, "last_success_time", now)
                pipe.hincrbyfloat(key, "total_response_time", elapsed)
                pipe.hset(key, "last_response_time", elapsed)
                
                # Check if it was open to close it
                curr_open = float(self.redis.hget(key, "circuit_open_since") or 0.0)
                if curr_open > 0:
                    pipe.hset(key, "circuit_open_since", 0.0)
                    
                pipe.execute()
                return
            except Exception:
                pass
                
        with self._lock:
            stats = self.get_stats(provider)
            stats.total_calls += 1
            stats.total_successes += 1
            stats.consecutive_failures = 0
            stats.last_success_time = time.time()
            stats.total_response_time += elapsed
            stats.last_response_time = elapsed
            if stats.circuit_open_since > 0:
                stats.circuit_open_since = 0

    def record_failure(self, provider: str, error: str = ""):
        config = self.get_config(provider)
        
        if self.redis:
            try:
                now = time.time()
                key = f"health:stats:{provider}"
                
                # We need the new consecutive failures count to know if we should trip
                new_fails = self.redis.hincrby(key, "consecutive_failures", 1)
                
                pipe = self.redis.pipeline()
                pipe.hincrby(key, "total_calls", 1)
                pipe.hincrby(key, "total_failures", 1)
                pipe.hset(key, "last_failure_time", now)
                pipe.hset(key, "last_error", error[:200]) # Truncate error
                
                if new_fails >= config.failure_threshold:
                    curr_open = float(self.redis.hget(key, "circuit_open_since") or 0.0)
                    if curr_open == 0:
                        pipe.hset(key, "circuit_open_since", now)
                        
                pipe.execute()
                return
            except Exception:
                pass
                
        with self._lock:
            stats = self.get_stats(provider)
            stats.total_calls += 1
            stats.total_failures += 1
            stats.consecutive_failures += 1
            stats.last_failure_time = time.time()
            stats.last_error = error
            if stats.consecutive_failures >= config.failure_threshold:
                if stats.circuit_open_since == 0:
                    stats.circuit_open_since = time.time()

    # ── Reporting ────────────────────────────────────────────────────

    def reset_circuit(self, provider: str):
        """Force-reset a provider's circuit breaker to CLOSED."""
        if self.redis:
            try:
                key = f"health:stats:{provider}"
                pipe = self.redis.pipeline()
                pipe.hset(key, "consecutive_failures", 0)
                pipe.hset(key, "circuit_open_since", 0.0)
                pipe.execute()
                return
            except Exception:
                pass
                
        with self._lock:
            stats = self.get_stats(provider)
            stats.consecutive_failures = 0
            stats.circuit_open_since = 0

    def summary(self) -> dict:
        """Get health summary for all providers."""
        result = {}
        for name in sorted(self._configs.keys()):
            stats = self.get_stats(name)
            result[name] = {
                "state": self.circuit_state(name),
                "success_rate": f"{stats.success_rate:.0f}%",
                "avg_response_time": f"{stats.avg_response_time:.1f}s",
                "consecutive_failures": stats.consecutive_failures,
                "last_error": stats.last_error[:80] if stats.last_error else "",
                "total_calls": stats.total_calls,
            }
        return result

    def print_health(self):
        """Print a formatted health report."""
        print(f"\n  {'Provider':<16} {'State':<22} {'Success':>8} {'Avg Time':>9} {'Calls':>6}")
        print(f"  {'─'*16} {'─'*22} {'─'*8} {'─'*9} {'─'*6}")
        for name in sorted(self._configs.keys()):
            stats = self.get_stats(name)
            state = self.circuit_state(name)
            icon = "🟢" if state == "CLOSED" else "🟡" if state == "HALF_OPEN" else "🔴"
            print(f"  {name:<16} {icon} {state:<19} {stats.success_rate:>7.0f}%"
                  f" {stats.avg_response_time:>8.1f}s {stats.total_calls:>5}")
