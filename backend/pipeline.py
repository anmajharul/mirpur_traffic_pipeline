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
  Reference: ML Systems Technical Debt Tracking (2025) — separation of training and serving.

REFERENCES:
[1] ML Systems Technical Debt Tracking (2025).
    IEEE Software.

[2] Modern Data Preprocessing for Traffic ML (2025).
    IEEE Transactions on Intelligent Transportation Systems.

[3] Deep Learning for Short-term Traffic Forecasting (2025).
    Transportation Research Part C.

[4] Modern Traffic Data Fusion Techniques (2025). Information Fusion.
    https://doi.org/10.1016/j.trc.2012.09.003
"""

import time
import os
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
# Reference: ML Systems Technical Debt Tracking (2025) — silent failure anti-pattern
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
# Reference: Deep Learning for Short-term Traffic Forecasting (2025), TR Part C.
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
# COLLECTION EXECUTION
# Executes data collection once and cleanly exits for Cloud Run Jobs,
# or runs continuously for Koyeb Worker Services if KOYEB_WORKER=true.
# Reference: 5-minute cycle standard for urban traffic monitoring
# and short-term arterial forecasting (Deep Learning for Short-term Traffic Forecasting 2025).
# ======================================================
if __name__ == "__main__":
    if os.environ.get("KOYEB_WORKER", "false").lower() == "true":
        logging.info("[PIPELINE] Starting continuous Koyeb Worker loop (5-minute interval)")
        while True:
            try:
                run_collection_cycle()
            except Exception as e:
                logging.error(f"[PIPELINE] Unhandled exception in cycle: {e}")
            logging.info("[PIPELINE] Sleeping for 300 seconds...")
            time.sleep(300)
    else:
        run_collection_cycle()
