"""
Rayobyte Proxy Provider.

Pricing: Pay-as-you-go bandwidth (Rotating DC / Residential)
Pool: Residential and Datacenter options
Docs: https://rayobyte.com/

URL format:
  http://USER:PASS@la.residential.rayobyte.com:8000

Geo-targeting (appended to username):
  -country-US
  -country-US-region-california
  -country-US-region-california-city-los_angeles

Sessions (appended to username, after geo):
  -session-SESSIONID            (soft sticky — same IP while available)
  -hardsession-SESSIONID        (hard sticky — locked IP for duration)
  -session-SESSIONID-duration-10 (sticky for 10 minutes, max 60)
"""

import uuid
from ..proxy import ProxyProvider


class RayobyteProvider(ProxyProvider):
    NAME = "rayobyte"
    PRICING_MODEL = "bandwidth"
    WEIGHT = 2

    def __init__(
        self,
        enabled: bool = True,
        user: str = "",
        password: str = "",
        host: str = "la.residential.rayobyte.com",
        port: int = 8000,
        session_duration: int = 10,
        **kwargs,
    ):
        super().__init__(enabled=enabled, user=user, password=password, **kwargs)
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._session_duration = session_duration  # minutes (1-60)

    def get_proxy_url(
        self,
        country: str | None = None,
        sticky: bool = False,
        session_id: str | None = None,
    ) -> str:
        """
        Build Rayobyte proxy URL with optional geo and session params.

        Args:
            country: ISO 2-letter code (e.g. "US", "GB", "SA")
            sticky: If True, add a soft sticky session
            session_id: Custom session ID (implies sticky=True)

        Returns:
            URL like http://user-country-US-session-abc:pass@host:port
        """
        username = self._user

        # ── Geo-targeting ──
        if country:
            username += f"-country-{country.upper()}"

        # ── Sticky sessions ──
        if session_id or sticky:
            sid = session_id or uuid.uuid4().hex[:12]
            username += f"-session-{sid}-duration-{self._session_duration}"

        return f"http://{username}:{self._password}@{self._host}:{self._port}"
