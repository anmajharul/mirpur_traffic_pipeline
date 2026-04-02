"""
waze_cache_collector.py — GitHub Actions Waze Speed Cache Collector
===================================================================
Purpose:
- Runs ONLY in GitHub Actions (Microsoft Azure IPs — not blocked by Waze).
- Collects Waze crowd-sourced speeds for all Mirpur-10 corridors.
- Upserts results to Supabase `waze_speed_cache` table.
- Koyeb's data_collector.py reads from this cache instead of calling
  Waze directly (Koyeb datacenter IPs are blocked by Waze).

ARCHITECTURE:
  Waze's unofficial API rejects requests originating from cloud
  datacenter IP ranges (AWS, GCP, Koyeb). GitHub Actions uses Microsoft
  Azure runner IPs, which Waze does not currently block.
  This script acts as a residential-equivalent relay for Waze data.

  Flow:
    GitHub Actions (*/5 min, Azure IP)
        └── WazeRouteCalculator → Supabase waze_speed_cache (upsert)
    Koyeb (/run, every 5 min)
        └── data_collector.py → reads waze_speed_cache → fuse_speeds()

MIXED TRAFFIC NOTE:
  Waze crowd-sourced GPS data inherently reflects mixed-flow conditions
  (motorcycles, CNG, buses all contribute probes).
  vehicle_type is not specified to keep routing in aggregate mode.
  References: JICA (2015) BD-P18; Bachmann et al. (2013) TR Part C.

REFERENCES:
[1] WazeRouteCalculator (unofficial Waze API Python wrapper)
    https://github.com/kovacsbalu/WazeRouteCalculator
[2] JICA (2015). Preparatory Survey on Dhaka Urban Transport Network
    Development Study (BD-P18). Table 4.3 — fleet composition.
    https://openjicareport.jica.go.jp/pdf/11996774_01.pdf
[3] Bachmann, C. et al. (2013). Transportation Research Part C, 26, 12–26.
    https://doi.org/10.1016/j.trc.2012.09.003
[4] Vlahogianni, E.I. et al. (2014). Transportation Research Part C, 43, 3–19.
    https://doi.org/10.1016/j.trc.2014.01.005
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import WazeRouteCalculator  # type: ignore
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

# ---------------------------------------------------------------------------
# CONFIG — must mirror config.py CORRIDORS exactly
# ---------------------------------------------------------------------------
CORRIDORS: dict[str, dict[str, str]] = {
    "North (Mirpur-11 to 10)": {
        "origin": "23.818833,90.365443",
        "dest":   "23.807247,90.368658",
    },
    "South (Kazipara to 10)": {
        "origin": "23.795476,90.373516",
        "dest":   "23.806925,90.368497",
    },
    "East (Mirpur-14 to 10)": {
        "origin": "23.801368,90.380476",
        "dest":   "23.807028,90.368790",
    },
    "West (Mirpur-1 to 10)": {
        "origin": "23.801584,90.357905",
        "dest":   "23.807144,90.368412",
    },
}

CACHE_TABLE = "waze_speed_cache"

# Physical plausibility bounds (RSTP 2015: Dhaka urban 5–80 km/h)
SPEED_MIN_KMH = 5.0
SPEED_MAX_KMH = 80.0


def _fetch_waze_speed(origin: str, dest: str) -> float | None:
    """
    Fetch travel speed from Waze crowd-sourced routing.
    region='EU' verified empirically for Bangladesh routing
    (region='IL' returns HTTP 500 for Dhaka coordinates).
    """
    try:
        route = WazeRouteCalculator.WazeRouteCalculator(
            origin, dest, region="EU"
        )
        dur_min, dist_km = route.calc_route_info()

        if dur_min <= 0 or dist_km <= 0:
            return None

        speed = dist_km / (dur_min / 60.0)  # km/h

        if not (SPEED_MIN_KMH <= speed <= SPEED_MAX_KMH):
            logging.warning(
                f"[WAZE] Implausible speed {speed:.1f} km/h — rejected"
            )
            return None

        return round(speed, 2)

    except Exception as exc:
        logging.warning(f"[WAZE] Route calculation failed: {exc}")
        return None


def run() -> None:
    """
    Main entry point: collect Waze speeds for all corridors and
    upsert to Supabase waze_speed_cache.

    Called by GitHub Actions every 5 minutes.
    Reference: Vlahogianni et al. (2014) — 5-min standard collection cadence.
    """
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY are required")

    supabase = create_client(supabase_url, supabase_key)
    now_utc = datetime.now(timezone.utc).isoformat()

    success_count = 0
    failed_count  = 0

    for corridor_id, coords in CORRIDORS.items():
        speed = _fetch_waze_speed(coords["origin"], coords["dest"])

        record = {
            "corridor_id":  corridor_id,
            "waze_speed":   speed,       # None = Waze call failed this cycle
            "collected_at": now_utc,
        }

        try:
            supabase.table(CACHE_TABLE).upsert(
                record, on_conflict="corridor_id"
            ).execute()

            speed_str = f"{speed:.2f} km/h" if speed is not None else "None"
            logging.info(f"[CACHE] {corridor_id} → {speed_str}")
            success_count += 1

        except Exception as exc:
            logging.error(
                f"[CACHE] DB upsert failed for '{corridor_id}': {exc}"
            )
            failed_count += 1

    logging.info(
        f"[CACHE] Collection complete: {success_count} ok, "
        f"{failed_count} failed | timestamp={now_utc}"
    )


if __name__ == "__main__":
    run()
