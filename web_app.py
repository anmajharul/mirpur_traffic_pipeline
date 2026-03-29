import logging
import os
import sys
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, Header, HTTPException


ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from pipeline import maybe_retrain, run_collection_cycle  # type: ignore  # noqa: E402


app = FastAPI(title="Mirpur Traffic AI Trigger API")
run_lock = Lock()


@app.get("/healthz")
def healthcheck():
    return {"status": "ok"}


@app.get("/")
def root():
    logging.info("[API] Root endpoint requested")
    return {
        "service": "mirpur-traffic-ai",
        "healthcheck": "/healthz",
        "trigger": "/run",
    }


@app.get("/run")
@app.post("/run")
def trigger_pipeline(x_cron_token: str | None = Header(default=None)):
    expected_token = os.getenv("CRON_SECRET")
    if not expected_token:
        raise HTTPException(status_code=503, detail="CRON_SECRET is not configured")

    if x_cron_token != expected_token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not run_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Pipeline run already in progress")

    try:
        collection_summary = run_collection_cycle()
        training_summary = maybe_retrain()
        return {
            "status": "ok",
            "collection": collection_summary,
            "training": training_summary,
        }
    finally:
        run_lock.release()
