"""
Authentication middleware — RapidAPI + API key support.
"""

import os
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("api.auth")

# Load config from env
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
API_KEYS = set(filter(None, os.getenv("API_KEYS", "").split(",")))
RAPIDAPI_SECRET = os.getenv("RAPIDAPI_PROXY_SECRET", "")


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip auth for health and docs
        path = request.url.path
        if path in ("/health", "/docs", "/openapi.json", "/redoc", "/"):
            return await call_next(request)

        if not REQUIRE_AUTH:
            return await call_next(request)

        # Check RapidAPI header
        rapid_secret = request.headers.get("x-rapidapi-proxy-secret", "")
        if RAPIDAPI_SECRET and rapid_secret == RAPIDAPI_SECRET:
            return await call_next(request)

        # Check API key header
        api_key = request.headers.get("x-api-key", "") or request.query_params.get("api_key", "")
        if API_KEYS and api_key in API_KEYS:
            return await call_next(request)

        logger.warning("Unauthorized request from %s to %s", request.client.host, path)
        return JSONResponse(
            status_code=401,
            content={"status": "error", "error": {"code": "UNAUTHORIZED", "message": "Valid API key required"}},
        )
