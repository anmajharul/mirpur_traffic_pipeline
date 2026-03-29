"""
freeflow.py — Q1 DEFENSIBLE FREE-FLOW SPEED ESTIMATION
=======================================================
Purpose:
- Estimate corridor free-flow speed from empirical probe data
- Method: 85th percentile of speeds during low-demand period (HCM 6e)
- Outlier removal: Tukey IQR (non-parametric, reproducible)
- Fallback: RSTP design speed for Dhaka arterials (40 km/h)
- Cache: 7-day TTL (process-local; see limitation note below)

LIMITATION:
    Cache is process-local (dict). In multi-process or serverless
    deployment, free-flow is recomputed on each process restart.
    Production deployment should use Redis or DB-backed TTL cache.

REFERENCES:
[1] TRB (2022). Highway Capacity Manual, 7th Edition.
    Transportation Research Board. ISBN 978-0-309-08766-8.
    https://www.trb.org/Main/Blurbs/181828.aspx

[2] JICA (2015). Dhaka Revised Strategic Transport Plan (RSTP).
    Japan International Cooperation Agency.
    https://openjicareport.jica.go.jp/pdf/12235575.pdf

[3] Tukey, J.W. (1977). Exploratory Data Analysis.
    Addison-Wesley. ISBN 0-201-07616-0.

[4] Vlahogianni, E.I. et al. (2014). Short-term traffic forecasting:
    Where we are and where we're going.
    Transportation Research Part C, 43, 3–19.
    https://doi.org/10.1016/j.trc.2014.01.005
"""

import pandas as pd
import logging
from typing import Optional
from datetime import datetime, timezone, timedelta

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
BDT = timezone(timedelta(hours=6))
DEFAULT_FREE_FLOW = 40.0   # RSTP design speed for Dhaka arterials (JICA 2015)
MIN_SAMPLES_TOTAL = 50     # minimum rows to attempt estimation
MIN_SAMPLES_NIGHT = 20     # minimum rows in low-demand window
MIN_SAMPLES_POST_IQR = 10  # minimum rows after outlier removal
CACHE_TTL_DAYS = 7

_free_flow_cache: dict = {}


# -------------------------------------------------
# TUKEY IQR OUTLIER REMOVAL
# Reference: Tukey (1977), Chapter 2
# Fence: [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
# -------------------------------------------------
def remove_outliers_iqr(series: pd.Series) -> pd.Series:
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return series
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return series[(series >= lower) & (series <= upper)]


# -------------------------------------------------
# FREE-FLOW ESTIMATION
# -------------------------------------------------
def estimate_free_flow_from_data(df: pd.DataFrame) -> Optional[float]:
    """
    Estimate free-flow speed using 85th percentile of speeds during
    low-demand period (02:00–05:00 BDT), following HCM 6e methodology.

    Method:
        1. Filter to low-demand window (02:00–05:00 local time)
        2. Remove physical outliers (5–80 km/h; RSTP bounds)
        3. Apply Tukey IQR filter (Tukey 1977)
        4. Return 85th percentile (HCM 6e, Chapter 15)

    Args:
        df: DataFrame with 'created_at' (UTC-aware) and 'speed_kmh' columns

    Returns:
        estimated free-flow speed (float) or None if insufficient data
    """
    if df is None or len(df) < MIN_SAMPLES_TOTAL:
        return None

    df = df.copy()

    # Time parse with strict UTC enforcement
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df = df.dropna(subset=["created_at"])
    df["created_at"] = df["created_at"].dt.tz_convert(BDT)
    df["hour"] = df["created_at"].dt.hour

    # Low-demand window: 02:00–05:00 (HCM off-peak proxy)
    night_df = df[(df["hour"] >= 2) & (df["hour"] <= 5)]
    if len(night_df) < MIN_SAMPLES_NIGHT:
        return None

    # Physical speed filter (RSTP/HCM: 5–80 km/h for urban arterials)
    speeds = night_df["speed_kmh"].dropna()
    speeds = speeds[(speeds >= 5) & (speeds <= 80)]
    if len(speeds) < MIN_SAMPLES_NIGHT:
        return None

    # Variance check — degenerate distribution guard
    if speeds.std() < 1:
        return None

    # Tukey IQR outlier removal (Tukey 1977)
    speeds = remove_outliers_iqr(speeds)
    if len(speeds) < MIN_SAMPLES_POST_IQR:
        return None

    # HCM 85th percentile free-flow speed
    free_flow = float(speeds.quantile(0.85))
    return float(f"{free_flow:.2f}")


# -------------------------------------------------
# MAIN FUNCTION (with cache)
# -------------------------------------------------
def get_free_flow(direction: str, df: Optional[pd.DataFrame] = None) -> float:
    """
    Get free-flow speed for a corridor.
    Priority: (1) valid cache, (2) data-driven estimate, (3) RSTP fallback.

    Returns:
        free_flow_kmh (float): always returns a positive value
    """
    now = datetime.now(timezone.utc)

    # Cache check
    if direction in _free_flow_cache:
        cached = _free_flow_cache[direction]
        age_days = (now - cached["timestamp"]).total_seconds() / 86400
        if age_days < CACHE_TTL_DAYS:
            return cached["speed"]
        logging.info(f"[FREEFLOW] Cache expired for '{direction}' (age={age_days:.1f} days)")

    # Data-driven estimation
    if df is not None:
        try:
            est = estimate_free_flow_from_data(df)
            if est is not None:
                _free_flow_cache[direction] = {"speed": est, "timestamp": now}
                logging.info(f"[FREEFLOW] Estimated {est} km/h for '{direction}' from data")
                return est
        except Exception as e:
            logging.warning(f"[FREEFLOW] Estimation failed for '{direction}': {e}")

    # RSTP fallback (JICA 2015 — 40 km/h design speed for Dhaka arterials)
    logging.info(f"[FREEFLOW] Using RSTP fallback {DEFAULT_FREE_FLOW} km/h for '{direction}'")
    _free_flow_cache[direction] = {"speed": DEFAULT_FREE_FLOW, "timestamp": now}
    return DEFAULT_FREE_FLOW