"""
data_loader.py — Q1 DEFENSIBLE DATA LOADING MODULE
====================================================
Purpose:
- Robust ETL pipeline for traffic ML
- Strict temporal ordering (no random shuffle)
- Comprehensive leakage column removal
- Physical consistency validation
- No target-derived features passed to training

LEAKAGE COLUMNS REMOVED (exhaustive):
    mapbox_speed    → one component of fused speed_kmh
    waze_speed      → one component of fused speed_kmh
    data_confidence → post-hoc fusion quality (not available at inference)
    anomaly_ratio   → computed from mapbox+waze (inference unavailable)
    speed_kmh       → derived from actual_eta_min (TARGET PROXY)
    congestion_percent → = f(speed_kmh, free_flow) → target-derived
    tti             → = actual_eta_min / free_time → DIRECT target transform
    speed_ratio     → = speed_kmh / free_flow → target-derived
    travel_time_sec → = actual_eta_min × 60 → EXACT duplicate target
    free_flow_kmh   → used only for congestion calc (not predictive alone)

Reference: Kaufman et al. (2012) — leakage taxonomy and avoidance.

REFERENCES:
[1] JICA (2015). Dhaka RSTP.
    https://openjicareport.jica.go.jp/pdf/12247623_01.pdf

[2] Vlahogianni, E.I. et al. (2014). Transportation Research Part C, 43, 3–19.
    https://doi.org/10.1016/j.trc.2014.01.005

[3] Kaufman, S. et al. (2012). ACM TKDD, 6(4), Article 15.
    https://doi.org/10.1145/2382577.2382579

[4] TRB (2022). Highway Capacity Manual, 7th Edition.
    Transportation Research Board. ISBN 978-0-309-08766-8.
"""

import pandas as pd
import logging
from supabase import create_client
from datetime import datetime, timezone, timedelta

from config import SUPABASE_URL, SUPABASE_KEY

logging.basicConfig(level=logging.INFO)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
BDT = timezone(timedelta(hours=6))


# -------------------------------------------------
# LEAKAGE COLUMN REGISTRY (EXHAUSTIVE)
# Reference: Kaufman et al. (2012)
# -------------------------------------------------
LEAKAGE_COLS = [
    # Source speed components (not available at inference)
    "mapbox_speed",
    "waze_speed",
    "data_confidence",
    "anomaly_ratio",

    # TARGET-DERIVED FEATURES (critical leakage)
    "speed_kmh",           # = distance_km / (actual_eta_min / 60) → target proxy
    "congestion_percent",  # = (1 - speed_kmh/ff) * 100 → target-derived
    "tti",                 # = actual_eta_min / free_time → DIRECT linear transform
    "speed_ratio",         # = speed_kmh / ff → target-derived
    "travel_time_sec",     # = actual_eta_min * 60 → exact duplicate of target
    "free_flow_kmh",       # used for congestion calc only
]


# -------------------------------------------------
# CLEANING (PHYSICAL CONSISTENCY)
# Speed bounds: 5–80 km/h (RSTP/HCM urban arterial)
# -------------------------------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Core null filter
    required = ["speed_kmh", "actual_eta_min"] if "actual_eta_min" in df.columns else ["speed_kmh"]
    df = df.dropna(subset=[c for c in required if c in df.columns])

    # Physical speed bounds: 5–80 km/h (RSTP; JICA 2015 + HCM 2022)
    if "speed_kmh" in df.columns:
        df = df[(df["speed_kmh"] >= 5) & (df["speed_kmh"] <= 80)]

    # ETA–distance consistency check
    if "distance_km" in df.columns and "actual_eta_min" in df.columns:
        valid_mask = df["actual_eta_min"] > 0
        df = df[valid_mask].copy()
        derived_speed = df["distance_km"] / (df["actual_eta_min"] / 60.0)
        df = df[(derived_speed >= 5) & (derived_speed <= 80)]

    return df


# -------------------------------------------------
# SINGLE CORRIDOR FETCH
# -------------------------------------------------
def fetch_direction_data(direction: str, days_lookback: int = 14) -> pd.DataFrame:
    """
    Fetch recent data for one corridor (used for free-flow estimation).
    """
    cutoff_date = (datetime.utcnow() - timedelta(days=days_lookback)).isoformat()

    try:
        response = (
            supabase
            .table("smart_eta_logs")
            .select("*")
            .eq("direction", direction)
            .gte("created_at", cutoff_date)
            .order("created_at", desc=False)
            .execute()
        )

        data = response.data
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
        df = df.dropna(subset=["created_at"])
        df["created_at"] = df["created_at"].dt.tz_convert(BDT)
        df = clean_dataframe(df)
        return df

    except Exception as e:
        logging.error(f"[DATA LOADER] fetch_direction_data error: {e}")
        return pd.DataFrame()


# -------------------------------------------------
# FULL DATASET (ML READY)
# -------------------------------------------------
def load_and_preprocess_data(days_lookback: int = 30) -> pd.DataFrame:
    """
    Load full dataset for ML training with all leakage columns removed.

    Processing order (CRITICAL — do not reorder):
        1. Fetch from DB
        2. Filter future timestamps
        3. Sort by [direction, created_at]
        4. Clean (physical bounds)
        5. Remove leakage columns

    Returns:
        DataFrame safe for ML feature engineering and training.
        Empty DataFrame on any failure.
    """
    cutoff_date = (datetime.utcnow() - timedelta(days=days_lookback)).isoformat()

    try:
        response = (
            supabase
            .table("smart_eta_logs")
            .select("*")
            .gte("created_at", cutoff_date)
            .execute()
        )

        data = response.data
        if not data:
            logging.warning("[DATA LOADER] No data returned from DB")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Time parse
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
        df = df.dropna(subset=["created_at"])

        # Strict future timestamp filter
        now_utc = datetime.now(timezone.utc)
        df = df[df["created_at"] < now_utc].copy()

        # Timezone conversion
        df["created_at"] = df["created_at"].dt.tz_convert(BDT)

        # Sort (CRITICAL for lag feature correctness)
        df = df.sort_values(["direction", "created_at"]).reset_index(drop=True)

        # Physical cleaning
        df = clean_dataframe(df)

        # Remove all leakage columns (exhaustive list)
        df = df.drop(columns=LEAKAGE_COLS, errors="ignore")

        logging.info(f"[DATA LOADER] Loaded {len(df)} rows — leakage columns removed")
        return df

    except Exception as e:
        logging.error(f"[DATA LOADER] load_and_preprocess_data error: {e}")
        return pd.DataFrame()