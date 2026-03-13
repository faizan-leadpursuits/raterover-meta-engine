"""
Provider auto-discovery registry.
Scans the providers/ directory and registers all adapters automatically.
Respects provider_config.json for enable/disable toggles per provider.
"""

import json
import importlib
import pkgutil
from pathlib import Path

# ── Config loader ─────────────────────────────────────────────────
_CONFIG_CACHE: dict | None = None


def load_provider_config() -> dict:
    """Load and cache provider config from the metasearch root."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    root = Path(__file__).resolve().parent.parent
    # Try config.json first, fall back to provider_config.json
    config_path = root / "config.json"
    if not config_path.exists():
        config_path = root / "provider_config.json"
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                _CONFIG_CACHE = json.load(f)
        except Exception as e:
            print(f"  [registry] Warning: failed to load {config_path.name}: {e}")
            _CONFIG_CACHE = {}
    else:
        _CONFIG_CACHE = {}
    return _CONFIG_CACHE


def reload_provider_config() -> dict:
    """Force-reload the config (useful after editing the JSON at runtime)."""
    global _CONFIG_CACHE
    _CONFIG_CACHE = None
    return load_provider_config()


def get_provider_setting(domain: str, provider_name: str, key: str, default=None):
    """Read a single setting for a provider, e.g. get_provider_setting('hotels', 'booking_com', 'timeout', 90)."""
    cfg = load_provider_config()
    return cfg.get(domain, {}).get(provider_name, {}).get(key, default)


def get_defaults() -> dict:
    """Return the 'defaults' section of provider_config.json."""
    return load_provider_config().get("defaults", {})


# ── Discovery ─────────────────────────────────────────────────────

def discover_providers(domain="flights"):
    """
    Auto-discover all provider adapters in the providers/ directory.

    Each provider package must export an `Adapter` class attribute.
    Only adapters matching the requested domain are returned.
    Providers set to enabled=false in provider_config.json are skipped.

    Args:
        domain: The search domain to filter by (default: "flights")

    Returns:
        dict: {provider_name: AdapterClass}
    """
    config = load_provider_config()
    domain_cfg = config.get(domain, {})

    adapters = {}
    providers_dir = Path(__file__).resolve().parent.parent / "providers"

    if not providers_dir.exists():
        print(f"  [registry] Warning: providers directory not found at {providers_dir}")
        return adapters

    for finder, name, is_pkg in pkgutil.iter_modules([str(providers_dir)]):
        if not is_pkg:
            continue
        try:
            mod = importlib.import_module(f"providers.{name}")
            if hasattr(mod, "Adapter"):
                adapter_cls = mod.Adapter
                if getattr(adapter_cls, "DOMAIN", "flights") == domain:
                    adapter_name = adapter_cls.NAME

                    # ── Check config: skip disabled providers ──
                    prov_cfg = domain_cfg.get(adapter_name, {})
                    if prov_cfg.get("enabled") is False:
                        print(f"  [registry] Skipping disabled provider: {adapter_name}")
                        continue

                    # Attach config-driven timeout & priority onto the class
                    adapter_cls._CFG_TIMEOUT  = prov_cfg.get("timeout", 90)
                    adapter_cls._CFG_PRIORITY = prov_cfg.get("priority", 99)

                    adapters[adapter_name] = adapter_cls
        except Exception as e:
            print(f"  [registry] Warning: failed to load provider '{name}': {e}")

    return adapters


def list_providers(domain="flights"):
    """Print a formatted list of all discovered providers."""
    adapters = discover_providers(domain)
    if not adapters:
        print(f"  No {domain} providers found.")
        return

    print(f"\n  Available {domain} providers ({len(adapters)}):")
    print(f"  {'─' * 40}")
    for name, cls in sorted(adapters.items()):
        supported = ", ".join(cls.SUPPORTED_PARAMS[:5])
        print(f"  {name:20s} ({supported}...)")
    print()
