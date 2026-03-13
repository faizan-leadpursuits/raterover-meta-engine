"""
Hotel Metasearch API — Clean FastAPI Application

All scraping runs remotely via ScraperFlow Lambda.
This API orchestrates parallel Lambda invocations, normalizes results,
and exposes clean REST endpoints for RapidAPI.

Run:
    uvicorn main:app --port 8000 --reload
"""

import os
import sys
import time
import logging
from pathlib import Path

# Ensure api/ directory is on sys.path so submodules are importable
_api_dir = Path(__file__).resolve().parent
if str(_api_dir) not in sys.path:
    sys.path.insert(0, str(_api_dir))

# Load .env file if present (check both api/ and project root)
try:
    from pathlib import Path
    from dotenv import load_dotenv
    # Load project root .env first, then local .env (local overrides)
    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    load_dotenv()  # also load api/.env if it exists
except ImportError:
    pass

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

# ── Logging ──
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api")

# ── App ──
_start_time = time.time()

app = FastAPI(
    title="Hotel Metasearch API",
    description=(
        "Compare hotel prices across 21 providers in real-time. "
        "Searches Booking.com, Agoda, Hotels.com, Expedia, Trip.com, "
        "Priceline, Kayak, Trivago, and 13 more providers simultaneously."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ──
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth Middleware ──
from middleware.auth import AuthMiddleware
app.add_middleware(AuthMiddleware)

# ── Routes ──
from routes.hotels import router as hotels_router
app.include_router(hotels_router)


# ── Health ──
@app.get("/health", tags=["Infrastructure"])
async def health():
    from core.engine import get_provider_names, get_enabled_providers
    return {
        "status": "healthy",
        "version": "2.0.0",
        "uptime_seconds": round(time.time() - _start_time, 1),
        "providers_available": len(get_provider_names()),
        "providers_enabled": len(get_enabled_providers()),
        "enabled": get_enabled_providers(),
        "mode": "direct-scraper",
    }


# ── Root ──
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")


# ── Global Error Handler ──
@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    logger.error("Unhandled error on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An internal error occurred. Please try again.",
            },
        },
    )


# ── Startup ──
@app.on_event("startup")
async def startup():
    from core.engine import get_provider_names
    providers = get_provider_names()
    logger.info("Hotel Metasearch API v2.0.0 started (direct-scraper mode)")
    logger.info("Discovered %d hotel providers: %s", len(providers), ", ".join(providers))
    auth_status = "ENABLED" if os.getenv("REQUIRE_AUTH", "false").lower() == "true" else "DISABLED"
    logger.info("Auth: %s | Proxy: %s", auth_status, os.getenv("PROXY_URL", "none")[:40])


if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    dev = os.getenv("DEV_MODE", "false").lower() == "true"
    uvicorn.run("main:app", host=host, port=port, reload=dev)
