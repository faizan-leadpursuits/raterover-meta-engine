"""
Built-in proxy provider implementations.

Each provider class extends ProxyProvider and knows how to format
its proxy URL with the correct authentication and geo-targeting syntax.

PROVIDER_REGISTRY maps lowercase names to classes for factory methods.
"""

from .rayobyte import RayobyteProvider
from .dataimpulse import DataImpulseProvider

# Registry used by ProxyManager.from_config() and from_env()
PROVIDER_REGISTRY = {
    "rayobyte": RayobyteProvider,
    "dataimpulse": DataImpulseProvider,
}

__all__ = [
    "PROVIDER_REGISTRY",
    "RayobyteProvider",
    "DataImpulseProvider",
]
