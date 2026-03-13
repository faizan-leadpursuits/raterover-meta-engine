"""
Base adapter class for hotel search providers.
Defines the contract for the hotel plugin system.
"""

from abc import ABC, abstractmethod
import pandas as pd
from .hotel_schemas import HOTEL_COMMON_COLUMNS


class HotelBaseAdapter(ABC):
    """
    Abstract base class for all hotel search provider adapters.

    Every hotel provider (Kayak Hotels, Trivago, etc.) must implement
    this interface. The adapter handles:
      1. Calling the underlying scraper/API
      2. Normalizing results into the universal hotel schema (HOTEL_COMMON_COLUMNS)

    To create a new hotel provider:
      1. Create a class extending HotelBaseAdapter
      2. Set NAME and DOMAIN = "hotels"
      3. Implement search() returning a DataFrame with HOTEL_COMMON_COLUMNS
    """

    # ── Provider Identity ──
    NAME: str = ""                   # e.g. "kayak_hotels", "trivago"
    DOMAIN: str = "hotels"           # Always "hotels" for hotel providers

    # ── Capability Declaration ──
    SUPPORTED_PARAMS: list = [
        "city", "check_in", "check_out", "adults", "rooms",
    ]

    # ── Proxy Declaration ──
    NEEDS_PROXY: bool = False

    def __init__(self, proxy_manager=None):
        """Initialise adapter, optionally injecting a ProxyManager."""
        self._proxy_manager = proxy_manager

    def set_proxy_manager(self, pm):
        """Inject a ProxyManager instance for this adapter to use."""
        self._proxy_manager = pm

    @property
    def proxy_manager(self):
        """Access the injected ProxyManager (or None if not set)."""
        return getattr(self, "_proxy_manager", None)

    @abstractmethod
    def search(
        self,
        city: str,
        check_in: str,
        check_out: str,
        adults: int = 2,
        children: int = 0,
        rooms: int = 1,
        currency: str = "USD",
        **kwargs,
    ) -> pd.DataFrame:
        """
        Execute a hotel search and return normalized results.

        Args:
            city: City name or code (e.g. "London", "New York")
            check_in: Check-in date "YYYY-MM-DD"
            check_out: Check-out date "YYYY-MM-DD"
            adults: Number of adult guests (default: 2)
            children: Number of children
            rooms: Number of rooms (default: 1)
            currency: ISO 4217 currency code
            **kwargs: Additional provider-specific parameters

        Returns:
            pd.DataFrame with HOTEL_COMMON_COLUMNS columns
        """
        pass

    def supports(self, param: str) -> bool:
        """Check if this adapter natively supports a parameter."""
        return param in self.SUPPORTED_PARAMS

    def empty_result(self) -> pd.DataFrame:
        """Return an empty DataFrame with the correct columns."""
        return pd.DataFrame(columns=HOTEL_COMMON_COLUMNS)

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.NAME} domain={self.DOMAIN}>"
