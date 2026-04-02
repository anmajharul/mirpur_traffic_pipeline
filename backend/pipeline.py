"""
pipeline.py — Q1 DEFENSIBLE ORCHESTRATION MODULE
==================================================
Purpose:
- Data collection orchestrator for all Mirpur-10 corridors
- 5-minute single-shot collection cycle (GitHub Actions) or continuous
  scheduler (Cloud Run Jobs, start_pipeline())
- Safe database insertion with response validation
- Graceful error handling (pipeline never crashes on single corridor failure)

ARCHITECTURE NOTE (Cloud Run):
  Data collection runs as Cloud Run Jobs (us-central1, free tier) triggered
  by GitHub Actions (collect.yml, */5 min). Each job executes run_collection.py
  which calls run_collection_cycle() once and exits (exit code 0/1).
  The start_pipeline() scheduler below is provided for local development and
  long-running compute environments; it is NOT used in the Cloud Run job.

  Model training is a separate Cloud Run Job (mirpur-trainer, 1Gi RAM, 2 CPU,
  1800s timeout) triggered by train.yml every 12 hours. Training never runs
  inside the collector job — this prevents RAM exhaustion and collection
  blocking.
  Reference: Sculley et al. (2015) — separation of training and serving.

  Waze data is NOT fetched directly in this module. Cloud Run IPs (GCP
  us-central1) are blocked by Waze. A separate GitHub Actions workflow
  (waze_cache.yml, Azure IP) fetches Waze data and writes to
  Supabase waze_speed_cache. data_collector.py reads from this cache.
  Reference: Bachmann et al. (2013) — heterogeneous multi-source fusion.

REFERENCES:
[1] Sculley, D. et al. (2015). Hidden technical debt in machine learning systems.
    NeurIPS 2015.
    https://proceedings.neurips.cc/paper/2015/hash/86df7dcfd896fcaf2674f757a2463eba-Abstract.html

[2] Breck, E. et al. (2019). Data Validation for Machine Learning.
    SysML 2019. https://mlsys.org/Conferences/2019/doc/2019/167.pdf

[3] Vlahogianni, E.I. et al. (2014). Short-term traffic forecasting:
    Where we are and where we're going.
    Transportation Research Part C, 43, 3-19.
    https://doi.org/10.1016/j.trc.2014.01.005

[4] Bachmann, C. et al. (2013). Transportation Research Part C, 26, 12–26.
    https://doi.org/10.1016/j.trc.2012.09.003
"""

import schedule  # type: ignore
import time
import logging
from datetime import datetime, timezone, timedelta

from config import CORRIDORS, MAPBOX_TOKEN, SUPABASE_URL, SUPABASE_KEY  # type: ignore
from supabase import create_client  # type: ignore

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
BDT = timezone(timedelta(hours=6))


# ======================================================
# SAFE DATABASE INSERTION
# FIX: validates response.data is non-empty list — not just non-None
# Empty list means constraint violation was silently suppressed.
# Reference: Sculley et al. (2015) — silent failure anti-pattern
# ======================================================
def safe_insert(table: str, record: dict) -> bool:
    """
    Insert a record into Supabase with validation.

    Returns:
        True if insertion succeeded AND data was actually written.
        False otherwise (with logging).
    """
    try:
        response = supabase.table(table).insert(record).execute()

        # FIX: Check that response.data contains at least one row
        # Empty list = constraint violation or RLS rejection
        if not response.data:
            logging.warning(
                f"[DB] Insert to '{table}' returned empty data — "
                f"possible constraint violation or RLS rejection"
            )
            return False

        return True

    except Exception as e:
        logging.error(f"[DB] Insert to '{table}' failed: {e}")
        return False


# ======================================================
# COLLECTION CYCLE
# Five-minute collection is a standard short-term forecasting cadence for
# urban arterials and matches the horizon_min used downstream.
# Reference: Vlahogianni et al. (2014), TR Part C, 43, 3-19.
# ======================================================
def run_collection_cycle():
    """
    Collect one observation for each corridor.
    Gracefully skips individual corridor failures.
    """
    now = datetime.now(BDT)
    logging.info(f"[PIPELINE] Collection cycle started at {now.isoformat()}")

    success_count = 0
    fail_count = 0
    from data_collector import collect  # type: ignore

    for name, coords in CORRIDORS.items():
        try:
            result = collect(
                origin=coords["origin"],
                dest=coords["dest"],
                mapbox_token=MAPBOX_TOKEN,
                direction_name=name
            )

            if result.get("status") != "OK":
                logging.warning(f"[PIPELINE] {name}: {result.get('status', 'Unknown error')}")
                fail_count += 1
                continue

            # Remove non-DB fields
            result.pop("status", None)

            if safe_insert("smart_eta_logs", result):
                success_count += 1
            else:
                fail_count += 1

        except Exception as e:
            logging.error(f"[PIPELINE] Collection failed for {name}: {e}")
            fail_count += 1

    logging.info(
        f"[PIPELINE] Cycle complete: {success_count}/{len(CORRIDORS)} corridors "
        f"({fail_count} failed)"
    )

    return {
        "started_at": now.isoformat(),
        "total_corridors": len(CORRIDORS),
        "success_count": success_count,
        "fail_count": fail_count,
    }


# ======================================================
# SCHEDULER
# Collection-only: model training is handled by GitHub Actions (weekly).
# Reference: 5-minute cycle standard for urban traffic monitoring
# and short-term arterial forecasting (Vlahogianni et al. 2014, Section 4).
# ======================================================
def start_pipeline():
    """
    Start the collection-only pipeline scheduler.
    Runs every 5 minutes for all Mirpur-10 corridors.

    DEPLOYMENT CONTEXT:
      In production (Cloud Run), this function is NOT called.
      Cloud Run Jobs execute run_collection.py (which calls
      run_collection_cycle() once and exits). GitHub Actions
      (collect.yml) triggers the job every 5 minutes.

      start_pipeline() is used for:
        - Local development (python pipeline.py)
        - Manual long-running compute sessions

    Model training is NOT performed here. Training runs as a
    separate Cloud Run Job (mirpur-trainer) triggered by train.yml
    every 12 hours. The artifact (model_ml_weight.json) is written
    to Supabase Storage and hot-loaded by web_app.py at startup.

    Reference: Vlahogianni et al. (2014) — 5-min collection cadence.
    """
    logging.info("[PIPELINE] Starting Mirpur-10 data collection pipeline")

    # Immediate first run
    run_collection_cycle()

    # Schedule subsequent collection runs every 5 minutes
    schedule.every(5).minutes.do(run_collection_cycle)

    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except KeyboardInterrupt:
            logging.info("[PIPELINE] Stopped by user")
            break
        except Exception as e:
            logging.error(f"[PIPELINE] Scheduler error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    start_pipeline()
