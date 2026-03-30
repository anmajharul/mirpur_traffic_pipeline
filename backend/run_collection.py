"""
run_collection.py — Single-shot data collection entrypoint for GitHub Actions
==============================================================================
Purpose:
- Run exactly ONE collection cycle for all Mirpur-10 corridors and exit.
- Designed for GitHub Actions (*/5 min schedule) where a persistent scheduler
  (pipeline.py:start_pipeline) is not needed.

WHAT IT DOES per run:
  - Calls Mapbox driving-traffic API for each corridor (real-time ETA)
  - Calls Waze crowd-sourced routing (Azure IP → not blocked)
  - Calls WeatherAPI for Dhaka AQI + meteorological data
  - Queries MRT schedule status
  - Fuses speeds via fusion.py (anomaly detection + confidence scoring)
  - Inserts one row per corridor into Supabase smart_eta_logs
  - Exits with code 0 on partial success, 1 if ALL corridors fail

REFERENCES:
[1] Vlahogianni et al. (2014). Transportation Research Part C, 43, 3-19.
    5-min collection cadence standard for urban arterial forecasting.
    https://doi.org/10.1016/j.trc.2014.01.005
[2] Sculley et al. (2015). Hidden technical debt in ML systems. NeurIPS.
    https://proceedings.neurips.cc/paper/2015/hash/86df7dcfd896fcaf2674f757a2463eba-Abstract.html
"""

from __future__ import annotations

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


def main() -> None:
    from pipeline import run_collection_cycle  # type: ignore

    logging.info("[RUN] Starting single-shot Mirpur-10 data collection cycle")
    summary = run_collection_cycle()

    total   = summary.get("total_corridors", 0)
    success = summary.get("success_count", 0)
    failed  = summary.get("fail_count", 0)

    logging.info(
        f"[RUN] Done — {success}/{total} corridors OK, "
        f"{failed} failed | started_at={summary.get('started_at')}"
    )

    # Exit 1 only if ALL corridors failed (hard failure for GitHub Actions log)
    if success == 0 and total > 0:
        logging.error("[RUN] All corridors failed — check API keys and secrets")
        sys.exit(1)


if __name__ == "__main__":
    main()
