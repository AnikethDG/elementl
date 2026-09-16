import os
import logging
from typing import Dict
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware

# Configure structured logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("service-api")

app = FastAPI(
    title="Cloud Run Monorepo Service API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Apply CORS middleware if configured (defaulting to safe origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "").split(",") if os.getenv("ALLOWED_ORIGINS") else ["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request, call_next):
    """Adds essential security response headers."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


@app.get("/healthz", status_code=status.HTTP_200_OK)
def health_check() -> Dict[str, str]:
    """Cloud Run liveness / readiness probe endpoint."""
    return {"status": "healthy", "service": "service-api"}


@app.get("/", status_code=status.HTTP_200_OK)
def root() -> Dict[str, str]:
    """Root hello endpoint."""
    service_name = os.getenv("K_SERVICE", "service-api")
    revision = os.getenv("K_REVISION", "local")
    return {
        "message": f"Hello! from {service_name}!",
        "revision": revision,
        "environment": os.getenv("ENVIRONMENT", "development"),
        "environment_short": os.getenv("ENVIRONMENT_SHORT", "dev")
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    # Local dev server runs on 127.0.0.1 for testing, or container starts via entrypoint
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
