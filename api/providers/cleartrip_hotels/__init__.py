"""Cleartrip Hotels provider (SSR + curl_cffi implementation)."""
from .adapter import CleartripHotelsAdapter as Adapter
from .scraper import CleartripHotelsScraper as Scraper

__all__ = ["Adapter", "Scraper"]
