"""
Modular Proxy System — Core Module
====================================
Provides a pluggable proxy abstraction layer with:

 - ProxyProvider ABC: base class for all proxy providers
 - ProxyManager: orchestrates multiple providers with rotation strategies
 - Rotation strategies: round-robin, random, weighted

Usage:
    from metasearch.core.proxy import ProxyManager

    # From config dict
    pm = ProxyManager.from_config({
        "strategy": "round_robin",
        "providers": {
            "rayobyte": {"enabled": True, "user": "u", "password": "p"},
        }
    })

    # Get a proxy URL for curl_cffi
    proxy_url = pm.get_curl_cffi_proxy(country="US")

    # Get a proxy dict for requests/httpx
    proxy_dict = pm.get_proxy_dict()

    # Get a Playwright proxy config
    pw_proxy = pm.get_playwright_proxy()

    # Toggle providers on/off
    pm.disable("rayobyte")
    pm.enable("rayobyte")
"""

from __future__ import annotations

import os
import random
import logging
import threading
from abc import ABC, abstractmethod
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# PROXY PROVIDER — Abstract Base Class
# ═══════════════════════════════════════════════════════════════════════════

class ProxyProvider(ABC):
    """
    Base class for all proxy providers.

    Subclasses must set NAME and implement get_proxy_url().
    The base class provides convenience methods to format the proxy
    for every HTTP client type used in the project.
    """

    NAME: str = ""
    PRICING_MODEL: str = "bandwidth"  # "bandwidth", "per_request", "flat"
    WEIGHT: int = 1  # Higher = more traffic in weighted strategy

    def __init__(self, enabled: bool = True, **credentials):
        self.enabled = enabled
        self.credentials = credentials

    @abstractmethod
    def get_proxy_url(
        self,
        country: str | None = None,
        sticky: bool = False,
        session_id: str | None = None,
    ) -> str:
        """
        Return a proxy URL string for this provider.

        Args:
            country: ISO 2-letter country code for geo-targeting (e.g. "US", "GB")
            sticky: If True, maintain the same IP across requests
            session_id: Optional session ID for sticky sessions

        Returns:
            Proxy URL like "http://user:pass@host:port"
        """
        pass

    def get_proxy_dict(self, **kwargs) -> dict:
        """
        Return a proxy dict for `requests` and `httpx`:
            {"http": "http://...", "https": "http://..."}
        """
        url = self.get_proxy_url(**kwargs)
        return {"http": url, "https": url}

    def get_curl_cffi_proxy(self, **kwargs) -> str:
        """Return a single URL string for curl_cffi's proxy= parameter."""
        return self.get_proxy_url(**kwargs)

    def get_playwright_proxy(self, **kwargs) -> dict:
        """
        Return a Playwright-compatible proxy config:
            {"server": "http://host:port", "username": "...", "password": "..."}
        """
        url = self.get_proxy_url(**kwargs)
        parsed = urlparse(url)
        result = {
            "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
        }
        if parsed.username:
            result["username"] = parsed.username
        if parsed.password:
            result["password"] = parsed.password
        return result

    def info(self) -> dict:
        """Return a summary dict for display/logging."""
        return {
            "name": self.NAME,
            "enabled": self.enabled,
            "pricing": self.PRICING_MODEL,
            "weight": self.WEIGHT,
        }

    def __repr__(self):
        status = "✓" if self.enabled else "✗"
        return f"<{self.__class__.__name__} [{status}] {self.NAME}>"


# ═══════════════════════════════════════════════════════════════════════════
# ROTATION STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════

class RotationStrategy(ABC):
    """Base class for proxy rotation strategies."""

    @abstractmethod
    def select(self, providers: list[ProxyProvider]) -> ProxyProvider | None:
        pass


class RoundRobinStrategy(RotationStrategy):
    """Cycle through providers in order. Thread-safe."""

    def __init__(self):
        self._index = 0
        self._lock = threading.Lock()

    def select(self, providers: list[ProxyProvider]) -> ProxyProvider | None:
        if not providers:
            return None
        with self._lock:
            provider = providers[self._index % len(providers)]
            self._index += 1
        return provider


class RandomStrategy(RotationStrategy):
    """Pick a random provider each time."""

    def select(self, providers: list[ProxyProvider]) -> ProxyProvider | None:
        return random.choice(providers) if providers else None


class WeightedStrategy(RotationStrategy):
    """
    Pick providers proportional to their WEIGHT attribute.
    Higher weight = more likely to be selected.
    """

    def select(self, providers: list[ProxyProvider]) -> ProxyProvider | None:
        if not providers:
            return None
        weights = [p.WEIGHT for p in providers]
        return random.choices(providers, weights=weights, k=1)[0]


_STRATEGIES = {
    "round_robin": RoundRobinStrategy,
    "random": RandomStrategy,
    "weighted": WeightedStrategy,
}


# ═══════════════════════════════════════════════════════════════════════════
# PROXY MANAGER — Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

class ProxyManager:
    """
    Manages multiple proxy providers and selects one per request
    using a configurable rotation strategy.

    Usage:
        pm = ProxyManager()
        pm.add_provider(RayobyteProvider(user="u", password="p"))

        # Get a proxy (rotates across enabled providers)
        proxy_url = pm.get_curl_cffi_proxy(country="US")

        # Toggle providers
        pm.disable("rayobyte")
        pm.enable("rayobyte")
    """

    def __init__(
        self,
        providers: list[ProxyProvider] | None = None,
        strategy: str = "round_robin",
    ):
        self._providers: dict[str, ProxyProvider] = {}
        self._strategy = _STRATEGIES.get(strategy, RoundRobinStrategy)()

        if providers:
            for p in providers:
                self.add_provider(p)

    # ── Provider Management ──

    def add_provider(self, provider: ProxyProvider):
        """Register a proxy provider."""
        self._providers[provider.NAME] = provider
        logger.info("Proxy provider added: %s (enabled=%s)", provider.NAME, provider.enabled)

    def remove_provider(self, name: str):
        """Unregister a proxy provider by name."""
        removed = self._providers.pop(name, None)
        if removed:
            logger.info("Proxy provider removed: %s", name)

    def enable(self, name: str):
        """Enable a registered provider."""
        if name in self._providers:
            self._providers[name].enabled = True
            logger.info("Proxy provider enabled: %s", name)

    def disable(self, name: str):
        """Disable a registered provider (it won't be used for requests)."""
        if name in self._providers:
            self._providers[name].enabled = False
            logger.info("Proxy provider disabled: %s", name)

    def list_providers(self) -> list[dict]:
        """Return status info for all registered providers."""
        return [p.info() for p in self._providers.values()]

    @property
    def has_enabled_providers(self) -> bool:
        return any(p.enabled for p in self._providers.values())

    # ── Proxy Selection ──

    def _get_active_providers(self) -> list[ProxyProvider]:
        return [p for p in self._providers.values() if p.enabled]

    def _select_provider(self) -> ProxyProvider | None:
        active = self._get_active_providers()
        if not active:
            return None
        return self._strategy.select(active)

    def get_proxy(self, **kwargs) -> str | None:
        """
        Get a proxy URL from the next provider in rotation.
        Returns None if no providers are enabled (direct mode).
        """
        provider = self._select_provider()
        if not provider:
            return None
        try:
            url = provider.get_proxy_url(**kwargs)
            # Mask the password to avoid leaking credentials in logs
            from urllib.parse import urlparse
            p = urlparse(url)
            safe_url = f"{p.scheme}://{p.username}:***@{p.hostname}:{p.port}" if p.password else url
            logger.debug("Proxy selected: %s → %s", provider.NAME, safe_url)
            return url
        except Exception as e:
            logger.warning("Proxy provider %s failed: %s", provider.NAME, e)
            return None

    def get_proxy_dict(self, **kwargs) -> dict | None:
        """Get proxy dict for requests/httpx. Returns None if no proxy."""
        provider = self._select_provider()
        if not provider:
            return None
        try:
            return provider.get_proxy_dict(**kwargs)
        except Exception as e:
            logger.warning("Proxy provider %s failed: %s", provider.NAME, e)
            return None

    def get_curl_cffi_proxy(self, **kwargs) -> str | None:
        """Get proxy string for curl_cffi. Returns None if no proxy."""
        provider = self._select_provider()
        if not provider:
            return None
        try:
            return provider.get_curl_cffi_proxy(**kwargs)
        except Exception as e:
            logger.warning("Proxy provider %s failed: %s", provider.NAME, e)
            return None

    def get_playwright_proxy(self, **kwargs) -> dict | None:
        """Get proxy config for Playwright. Returns None if no proxy."""
        provider = self._select_provider()
        if not provider:
            return None
        try:
            return provider.get_playwright_proxy(**kwargs)
        except Exception as e:
            logger.warning("Proxy provider %s failed: %s", provider.NAME, e)
            return None

    # ── Named Provider Access (for adapters that need a specific proxy type) ──

    def get_provider(self, name: str) -> ProxyProvider | None:
        """Get a specific proxy provider by name."""
        return self._providers.get(name)

    def get_residential_playwright_proxy(self, **kwargs) -> dict | None:
        """Get Playwright proxy config from a residential provider (dataimpulse)."""
        for name in ("dataimpulse",):
            p = self._providers.get(name)
            if p and p.enabled:
                try:
                    return p.get_playwright_proxy(**kwargs)
                except Exception as e:
                    logger.warning("Residential proxy %s failed: %s", name, e)
        # Fallback to any provider
        return self.get_playwright_proxy(**kwargs)

    def get_datacenter_playwright_proxy(self, **kwargs) -> dict | None:
        """Get Playwright proxy config from a datacenter provider (rayobyte)."""
        for name in ("rayobyte",):
            p = self._providers.get(name)
            if p and p.enabled:
                try:
                    return p.get_playwright_proxy(**kwargs)
                except Exception as e:
                    logger.warning("DC proxy %s failed: %s", name, e)
        return self.get_playwright_proxy(**kwargs)

    # ── Factory Methods ──

    @classmethod
    def from_config(cls, config: dict) -> "ProxyManager":
        """
        Build a ProxyManager from a config dict.

        Expected format:
            {
                "enabled": true,
                "strategy": "round_robin",
                "providers": {
                    "rayobyte": {"enabled": true, "user": "u", "password": "p"},
                }
            }
        """
        from .proxy_providers import PROVIDER_REGISTRY

        if not config.get("enabled", True):
            return cls(strategy=config.get("strategy", "round_robin"))

        strategy = config.get("strategy", "round_robin")
        providers = []

        for name, provider_cfg in config.get("providers", {}).items():
            provider_cls = PROVIDER_REGISTRY.get(name)
            if not provider_cls:
                logger.warning("Unknown proxy provider '%s', skipping", name)
                continue

            enabled = provider_cfg.pop("enabled", True)
            try:
                provider = provider_cls(enabled=enabled, **provider_cfg)
                providers.append(provider)
            except Exception as e:
                logger.warning("Failed to create proxy provider '%s': %s", name, e)

        return cls(providers=providers, strategy=strategy)

    @classmethod
    def from_env(cls) -> "ProxyManager":
        """
        Build a ProxyManager from environment variables.

        Reads PROXY_ENABLED, PROXY_STRATEGY, and per-provider vars like
        RAYOBYTE_ENABLED, RAYOBYTE_USER, RAYOBYTE_PASS, etc.
        """
        from .proxy_providers import PROVIDER_REGISTRY

        if not os.getenv("PROXY_ENABLED", "false").lower() in ("true", "1", "yes"):
            return cls()

        strategy = os.getenv("PROXY_STRATEGY", "round_robin")
        providers = []

        # Map env var prefixes to provider names
        env_mappings = {
            "rayobyte": {
                "enabled_var": "RAYOBYTE_ENABLED",
                "kwargs": {
                    "user": "RAYOBYTE_USER",
                    "password": "RAYOBYTE_PASS",
                    "host": "RAYOBYTE_HOST",
                    "port": "RAYOBYTE_PORT",
                },
            },
            "dataimpulse": {
                "enabled_var": "DATAIMPULSE_ENABLED",
                "kwargs": {
                    "user": "DATAIMPULSE_USER",
                    "password": "DATAIMPULSE_PASS",
                    "host": "DATAIMPULSE_HOST",
                    "port": "DATAIMPULSE_PORT",
                },
            },
        }

        for name, mapping in env_mappings.items():
            enabled_val = os.getenv(mapping["enabled_var"], "false")
            if enabled_val.lower() not in ("true", "1", "yes"):
                continue

            provider_cls = PROVIDER_REGISTRY.get(name)
            if not provider_cls:
                continue

            kwargs = {}
            for kwarg_name, env_var in mapping["kwargs"].items():
                val = os.getenv(env_var, "")
                if val:
                    kwargs[kwarg_name] = val

            try:
                provider = provider_cls(enabled=True, **kwargs)
                providers.append(provider)
                logger.info("Loaded proxy provider from env: %s", name)
            except Exception as e:
                logger.warning("Failed to load proxy provider '%s' from env: %s", name, e)

        return cls(providers=providers, strategy=strategy)

    def __repr__(self):
        active = len(self._get_active_providers())
        total = len(self._providers)
        return f"<ProxyManager {active}/{total} active, strategy={self._strategy.__class__.__name__}>"
