"""
DataImpulse Proxy Provider — Residential.

Pool: Residential IPs (rotating by default)
Docs: https://docs.dataimpulse.com/

URL format:
  http://USER:PASS@gw.dataimpulse.com:823

Geo-targeting (appended to username):
  __cr.us    (country = US)
  __cr.gb    (country = GB)

Sticky sessions (appended to username):
  __sid.SESSION_ID   (sticky session)
"""

import uuid
from ..proxy import ProxyProvider


class DataImpulseProvider(ProxyProvider):
    NAME = "dataimpulse"
    PRICING_MODEL = "bandwidth"
    WEIGHT = 3  # Higher weight — prefer this for difficult sites

    def __init__(
        self,
        enabled: bool = True,
        user: str = "",
        password: str = "",
        host: str = "gw.dataimpulse.com",
        port: int = 823,
        **kwargs,
    ):
        super().__init__(enabled=enabled, user=user, password=password, **kwargs)
        self._user = user
        self._password = password
        self._host = host
        self._port = port

    def get_proxy_url(
        self,
        country: str | None = None,
        sticky: bool = False,
        session_id: str | None = None,
    ) -> str:
        """
        Build DataImpulse proxy URL with optional geo and session params.

        Args:
            country: ISO 2-letter code (e.g. "US", "GB")
            sticky: If True, add a sticky session
            session_id: Custom session ID (implies sticky=True)

        Returns:
            URL like http://user__cr.gb:pass@gw.dataimpulse.com:823
        """
        username = self._user

        # ── Geo-targeting ──
        if country:
            username += f"__cr.{country.lower()}"

        # ── Sticky sessions ──
        if session_id or sticky:
            sid = session_id or uuid.uuid4().hex[:12]
            username += f"__sid.{sid}"

        return f"http://{username}:{self._password}@{self._host}:{self._port}"
