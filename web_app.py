import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from supabase import create_client

try:
    import xgboost as xgb  # type: ignore
    _XGB_AVAILABLE = True
except ImportError:
    xgb = None  # type: ignore
    _XGB_AVAILABLE = False


ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from config import SUPABASE_KEY, SUPABASE_URL  # type: ignore  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------------------------------------------------------------------
# FASTAPI APP
# Koyeb is no longer used for data collection (moved to GitHub Actions).
# This app serves only as the Inference API for the Mirpur Traffic dashboard.
# ---------------------------------------------------------------------------
app = FastAPI(title="Mirpur Traffic AI Inference API")
model_lock = Lock()

# ---------------------------------------------------------------------------
# MODEL HOT-LOADER
# Mirrors the bucket / path constants from backend/upload_model.py so the
# inference side always pulls the same artifact that training pushes.
# ---------------------------------------------------------------------------
_MODEL_BUCKET        = os.getenv("SUPABASE_MODEL_BUCKET", "ml-models")
_MODEL_REMOTE_PATH   = os.getenv("MODEL_REMOTE_LATEST_PATH", "latest/model_ml_weight.json")
_MODEL_BUNDLE_PATH   = Path(__file__).resolve().parent / "backend" / "model_ml_weight.json"

# Asia/Dhaka (UTC+6) — used for human-readable load_at timestamps in logs.
BDT = timezone(timedelta(hours=6))

_active_model: Any = None  # Holds the xgb.Booster instance when loaded
_model_source = "none"     # "supabase" | "bundled" | "none"
_model_sha256  = ""        # hex digest of the last loaded artifact

INSTANCE_ID = (
    os.getenv("KOYEB_INSTANCE_ID")
    or os.getenv("KOYEB_SERVICE_ID")
    or os.getenv("HOSTNAME")
    or "local-instance"
)


def log_event(event: str, **fields):
    payload = {
        "event": event,
        "service": "mirpur-traffic-ai",
        "instance_id": INSTANCE_ID,
        **fields,
    }
    logging.info(json.dumps(payload, sort_keys=True, default=str))


@lru_cache(maxsize=1)
def _get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ---------------------------------------------------------------------------
# MODEL LOADER HELPERS
# ---------------------------------------------------------------------------
def _sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_booster_from_bytes(data: bytes) -> Any:
    """Deserialise a Booster from JSON bytes without touching the filesystem."""
    if not _XGB_AVAILABLE:
        return None
    booster = xgb.Booster()
    booster.load_model(bytearray(data))
    return booster


def _try_load_from_supabase() -> tuple[Any, bytes]:
    """
    Download the latest model artifact from Supabase Storage.
    Returns (booster, raw_bytes).  Both are None/empty on any failure.

    Bucket and path are controlled by SUPABASE_MODEL_BUCKET and
    MODEL_REMOTE_LATEST_PATH env vars (defaults mirror upload_model.py).
    """
    try:
        supabase = _get_supabase_client()
        raw = supabase.storage.from_(_MODEL_BUCKET).download(_MODEL_REMOTE_PATH)
        if not raw:
            return None, b""
        booster = _load_booster_from_bytes(raw)
        return booster, raw
    except Exception as exc:  # network, auth, bucket not found, etc.
        log_event("model_supabase_download_failed", error=str(exc))
        return None, b""


def _try_load_from_bundle() -> tuple[Any, bytes]:
    """Fall back to a model artifact bundled alongside the source tree."""
    if not _MODEL_BUNDLE_PATH.exists():
        return None, b""
    try:
        data = _MODEL_BUNDLE_PATH.read_bytes()
        booster = _load_booster_from_bytes(data)
        return booster, data
    except Exception as exc:
        log_event("model_bundle_load_failed", error=str(exc), path=str(_MODEL_BUNDLE_PATH))
        return None, b""


def _apply_model(booster: Any, raw_bytes: bytes, source: str) -> None:
    """
    Thread-safe swap of the global active model.
    Logs sha256, loaded_at (BDT), and source so every swap is auditable.
    """
    global _active_model, _model_source, _model_sha256
    digest = _sha256_of(raw_bytes) if raw_bytes else ""
    loaded_at = datetime.now(BDT).isoformat()
    with model_lock:
        _active_model  = booster
        _model_source  = source
        _model_sha256  = digest
    log_event(
        "model_loaded",
        source=source,
        model_sha256=digest[:16] + "…",  # abbreviated for log readability
        loaded_at=loaded_at,
    )


def _download_and_hot_load() -> dict:
    """
    Attempt to pull latest artifact from Supabase; fall back to bundled file.
    Returns a status dict suitable for an API response or log payload.
    """
    booster, raw = _try_load_from_supabase()
    if booster is not None:
        _apply_model(booster, raw, "supabase")
        return {"source": "supabase", "model_sha256": _sha256_of(raw)}

    booster, raw = _try_load_from_bundle()
    if booster is not None:
        _apply_model(booster, raw, "bundled")
        log_event("model_fallback_to_bundle", reason="supabase download failed")
        return {"source": "bundled", "model_sha256": _sha256_of(raw)}

    log_event("model_load_skipped", reason="xgboost unavailable or no artifact found")
    return {"source": "none", "model_sha256": ""}


# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------
@app.on_event("startup")
def startup_load_model() -> None:
    """
    Run once when the API starts.
    Pulls the daily-trained artifact from Supabase Storage so the inference
    service is always serving the latest model.
    """
    log_event("startup_model_load_begin", remote_path=_MODEL_REMOTE_PATH)
    result = _download_and_hot_load()
    log_event("startup_model_load_done", **result)


@app.get("/healthz")
def healthcheck():
    """Healthcheck endpoint for Koyeb/Vercel."""
    with model_lock:
        src    = _model_source
        digest = _model_sha256[:16] + "…" if _model_sha256 else ""
    return {"status": "ok", "model_source": src, "model_sha256_prefix": digest}


@app.get("/")
def root():
    return {
        "service": "mirpur-traffic-ai inference",
        "healthcheck": "/healthz",
        "model_admin": {
            "path": "/admin/reload-model",
            "method": "POST",
            "auth": "X-Reload-Secret: <RELOAD_SECRET>",
        },
    }


@app.post("/admin/reload-model")
def admin_reload_model(x_reload_secret: str | None = Header(default=None)):
    """
    Hot-swap the global XGBoost model from Supabase Storage without restarting.
    Gated by the RELOAD_SECRET env-var.
    """
    expected_secret = os.getenv("RELOAD_SECRET")
    if not expected_secret:
        log_event("model_reload_misconfigured")
        raise HTTPException(status_code=503, detail="RELOAD_SECRET is not configured")
    if x_reload_secret != expected_secret:
        log_event("model_reload_unauthorized")
        raise HTTPException(status_code=401, detail="Unauthorized")

    log_event("model_reload_requested")
    result = _download_and_hot_load()
    log_event("model_reload_done", **result)
    return {"status": "ok", **result}
