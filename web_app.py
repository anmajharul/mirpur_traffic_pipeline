import json
import logging
import os
import sys
from functools import lru_cache
from pathlib import Path
from threading import Lock
from time import monotonic
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException
from supabase import create_client


ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "backend"

if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from config import SUPABASE_KEY, SUPABASE_URL  # type: ignore  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(message)s")

app = FastAPI(title="Mirpur Traffic AI Trigger API")
run_lock = Lock()
rate_limit_lock = Lock()

INSTANCE_ID = (
    os.getenv("KOYEB_INSTANCE_ID")
    or os.getenv("KOYEB_SERVICE_ID")
    or os.getenv("HOSTNAME")
    or "local-instance"
)
PIPELINE_LOCK_NAME = os.getenv("PIPELINE_LOCK_NAME", "mirpur_pipeline_trigger")
PIPELINE_LOCK_LEASE_SECONDS = int(os.getenv("PIPELINE_LOCK_LEASE_SECONDS", "900"))
PIPELINE_MIN_TRIGGER_INTERVAL_SECONDS = int(
    os.getenv("PIPELINE_MIN_TRIGGER_INTERVAL_SECONDS", "60")
)

last_trigger_started_at = 0.0


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


def _get_pipeline_functions():
    from pipeline import maybe_retrain, run_collection_cycle  # type: ignore

    return maybe_retrain, run_collection_cycle


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None

    return token.strip()


def _coerce_rpc_bool(data) -> bool:
    if isinstance(data, bool):
        return data
    if isinstance(data, list) and data:
        return bool(data[0])
    if isinstance(data, dict):
        return bool(next(iter(data.values()))) if data else False
    return bool(data)


def _enforce_rate_limit():
    global last_trigger_started_at

    now_monotonic = monotonic()
    with rate_limit_lock:
        elapsed = now_monotonic - last_trigger_started_at
        if elapsed < PIPELINE_MIN_TRIGGER_INTERVAL_SECONDS:
            raise HTTPException(
                status_code=429,
                detail="Trigger rate limit exceeded",
            )
        last_trigger_started_at = now_monotonic


def _acquire_distributed_lock(owner: str) -> None:
    supabase = _get_supabase_client()
    try:
        response = supabase.rpc(
            "try_acquire_pipeline_run_lock",
            {
                "p_lock_name": PIPELINE_LOCK_NAME,
                "p_owner": owner,
                "p_lease_seconds": PIPELINE_LOCK_LEASE_SECONDS,
            },
        ).execute()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Pipeline lock backend unavailable",
        ) from exc

    if not _coerce_rpc_bool(response.data):
        raise HTTPException(
            status_code=409,
            detail="Pipeline run already in progress",
        )


def _release_distributed_lock(owner: str) -> None:
    supabase = _get_supabase_client()
    try:
        supabase.rpc(
            "release_pipeline_run_lock",
            {
                "p_lock_name": PIPELINE_LOCK_NAME,
                "p_owner": owner,
            },
        ).execute()
    except Exception as exc:
        log_event(
            "pipeline_lock_release_error",
            owner=owner,
            error=str(exc),
        )


@app.get("/healthz")
def healthcheck():
    return {"status": "ok"}


@app.get("/")
def root():
    return {
        "service": "mirpur-traffic-ai",
        "healthcheck": "/healthz",
        "trigger": {
            "path": "/run",
            "method": "POST",
            "auth": "Authorization: Bearer <CRON_SECRET>",
        },
    }


@app.post("/run")
def trigger_pipeline(authorization: str | None = Header(default=None)):
    run_id = uuid4().hex
    expected_token = os.getenv("CRON_SECRET")
    owner = f"{INSTANCE_ID}:{run_id}"

    if not expected_token:
        log_event("pipeline_trigger_misconfigured", run_id=run_id)
        raise HTTPException(status_code=503, detail="CRON_SECRET is not configured")

    provided_token = _extract_bearer_token(authorization)
    if provided_token != expected_token:
        log_event("pipeline_trigger_unauthorized", run_id=run_id)
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        _enforce_rate_limit()
    except HTTPException as exc:
        log_event(
            "pipeline_trigger_rate_limited",
            run_id=run_id,
            min_interval_seconds=PIPELINE_MIN_TRIGGER_INTERVAL_SECONDS,
        )
        raise exc

    if not run_lock.acquire(blocking=False):
        log_event("pipeline_trigger_local_lock_conflict", run_id=run_id)
        raise HTTPException(status_code=409, detail="Pipeline run already in progress")

    distributed_lock_acquired = False
    started_at = monotonic()

    try:
        maybe_retrain, run_collection_cycle = _get_pipeline_functions()
        _acquire_distributed_lock(owner)
        distributed_lock_acquired = True

        log_event("pipeline_run_started", run_id=run_id, lock_owner=owner)
        collection_summary = run_collection_cycle()
        training_summary = maybe_retrain()
        duration_sec = round(monotonic() - started_at, 3)

        log_event(
            "pipeline_run_completed",
            run_id=run_id,
            status="success",
            duration_sec=duration_sec,
            collection=collection_summary,
            training=training_summary,
        )
        return {
            "status": "ok",
            "run_id": run_id,
            "duration_sec": duration_sec,
            "collection": collection_summary,
            "training": training_summary,
        }
    except HTTPException as exc:
        log_event(
            "pipeline_run_rejected",
            run_id=run_id,
            status_code=exc.status_code,
            detail=exc.detail,
        )
        raise exc
    except Exception as exc:
        duration_sec = round(monotonic() - started_at, 3)
        log_event(
            "pipeline_run_failed",
            run_id=run_id,
            status="error",
            duration_sec=duration_sec,
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail="Pipeline execution failed") from exc
    finally:
        if distributed_lock_acquired:
            _release_distributed_lock(owner)
        run_lock.release()
