"""
Shared proxy utilities for hotel scrapers.

Provides proxy cycling: tries each proxy provider in order, falling back
to the next one on tunnel/timeout failures. Never goes proxyless.
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


def build_proxy_list(proxy_manager=None, proxy=None):
    """
    Build an ordered list of (name, playwright_proxy_config) tuples
    from a ProxyManager or a legacy single proxy.

    Returns:
        List of (provider_name, proxy_config_dict) tuples.
        proxy_config_dict is a Playwright proxy dict like
        {"server": "http://host:port", "username": "...", "password": "..."} or None.
    """
    configs = []

    if proxy_manager and hasattr(proxy_manager, '_providers'):
        for provider in proxy_manager._providers.values():
            if provider.enabled:
                try:
                    pw_proxy = provider.get_playwright_proxy()
                    if pw_proxy:
                        configs.append((provider.NAME, pw_proxy))
                except Exception:
                    pass

    elif proxy:
        pc = (proxy if isinstance(proxy, dict)
              else {"server": proxy} if isinstance(proxy, str) and proxy
              else None)
        if pc:
            configs.append(("direct", pc))

    if not configs:
        logger.warning("No proxy available — running direct")
        configs.append(("none", None))

    return configs


def _playwright_to_url(pw_proxy: dict | None) -> str | None:
    """Convert a Playwright proxy dict to a URL string for curl_cffi / requests."""
    if not pw_proxy or not isinstance(pw_proxy, dict):
        return None
    server = pw_proxy.get("server", "")
    user = pw_proxy.get("username", "")
    pw = pw_proxy.get("password", "")
    if server and user and pw:
        # Insert credentials: http://user:pass@host:port
        from urllib.parse import urlparse, urlunparse
        p = urlparse(server)
        netloc = f"{user}:{pw}@{p.hostname}:{p.port}" if p.port else f"{user}:{pw}@{p.hostname}"
        return urlunparse((p.scheme, netloc, p.path, p.params, p.query, p.fragment))
    return server or None


def build_curl_cffi_proxy_list(proxy_manager=None, proxy=None):
    """
    Like build_proxy_list but returns URL strings suitable for curl_cffi's
    ``proxies=`` parameter, instead of Playwright-style dicts.

    Returns:
        List of (provider_name, proxy_url_str_or_None) tuples.
    """
    pw_list = build_proxy_list(proxy_manager, proxy)
    return [
        (name, _playwright_to_url(cfg) if isinstance(cfg, dict) else cfg)
        for name, cfg in pw_list
    ]


def is_proxy_failure(error_str: str) -> bool:
    """Check if an error indicates a proxy tunnel/connection failure."""
    err = error_str.upper()
    return "TUNNEL" in err or "ERR_TUNNEL" in err or \
           "TIMEOUT" in err or "ERR_PROXY" in err or \
           "CONNECTION_FAILED" in err
