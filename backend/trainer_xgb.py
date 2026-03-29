"""
trainer_xgb.py — Q1 DEFENSIBLE XGBOOST TRAINER MODULE
=======================================================
Purpose:
- XGBoost model training for corridor ETA prediction
- Walk-forward cross-validation (5 folds, with std + bootstrap CI)
- Gap-aware lag features (NaN if gap > 15 min)
- Hard leakage assertion guard (_assert_no_leakage)
- Per-hour median forecasting for 24h horizon
- SettingWithCopyWarning prevented via explicit .copy()

FIXES FROM REVIEW:
- [FATAL] 24h forecast: per-hour median lag (not static last_lag)
- [FATAL] Leakage cols: hard assertion guard before fit()
- [MAJOR] Walk-forward: 5 folds + std + bootstrap CI (was 3 folds)
- [MAJOR] Gap-aware lag: NaN if gap > 15 min (non-consecutive)
- [MINOR] SettingWithCopyWarning: explicit .copy() before imputation

REFERENCES:
[1] Chen, T. & Guestrin, C. (2016). XGBoost: A scalable tree boosting system.
    KDD 2016. https://doi.org/10.1145/2939672.2939785

[2] Bergmeir, C. & Benítez, J.M. (2012). On the use of cross-validation for
    time series predictor evaluation. Information Sciences, 191, 192–213.
    https://doi.org/10.1016/j.ins.2011.12.028

[3] Hyndman, R.J. & Athanasopoulos, G. (2021). Forecasting: Principles and
    Practice, 3rd edition. https://otexts.com/fpp3/

[4] Efron, B. & Tibshirani, R.J. (1993). An Introduction to the Bootstrap.
    Chapman & Hall/CRC. ISBN 0-412-04231-2.

[5] Kaufman, S. et al. (2012). ACM TKDD, 6(4), Article 15.
    https://doi.org/10.1145/2382577.2382579

[6] Vlahogianni, E.I. et al. (2014). Short-term traffic forecasting:
    Where we are and where we're going.
    Transportation Research Part C, 43, 3–19.
    https://doi.org/10.1016/j.trc.2014.01.005
"""

import xgboost as xgb
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timezone, timedelta
from supabase import create_client
from typing import Optional, Dict

from config import SUPABASE_URL, SUPABASE_KEY
from data_loader import load_and_preprocess_data

logging.basicConfig(level=logging.INFO)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
BDT = timezone(timedelta(hours=6))


# ======================================================
# FORBIDDEN FEATURES — LEAKAGE REGISTRY (EXHAUSTIVE)
# Reference: Kaufman et al. (2012)
# These MUST NOT appear in feature_cols at any point.
# ======================================================
FORBIDDEN_FEATURES = {
    "speed_kmh",
    "tti",
    "travel_time_sec",
    "speed_ratio",
    "congestion_percent",
    "mapbox_speed",
    "waze_speed",
    "data_confidence",
    "anomaly_ratio",
    "free_flow_kmh",
}


def _assert_no_leakage(feature_cols: list):
    """
    Hard assertion guard: raise immediately if any forbidden feature
    is present in the feature columns.
    Reference: Kaufman et al. (2012), Section 3.
    """
    leaked = set(feature_cols) & FORBIDDEN_FEATURES
    if leaked:
        raise ValueError(
            f"[LEAKAGE GUARD] Forbidden features detected in feature_cols: {leaked}. "
            f"These are target-derived and MUST NOT be used as model inputs. "
            f"Reference: Kaufman et al. (2012)"
        )


# ======================================================
# GAP-AWARE LAG FEATURES
# FIX: NaN if time gap between consecutive records > 15 min
# This preserves the Markov assumption.
# Reference: Vlahogianni et al. (2014), Section 2.3
# ======================================================
MAX_GAP_MINUTES = 15


def create_lag_features(df: pd.DataFrame, target_col: str, lags: list = [1, 2, 3]) -> pd.DataFrame:
    """
    Create gap-aware lag features for each corridor.

    If the time gap between observation t and observation t-k exceeds
    MAX_GAP_MINUTES (15 min), the lag value is set to NaN.
    This prevents the model from using stale values as if they were recent.

    Args:
        df: DataFrame sorted by [direction, created_at] with 'created_at' column
        target_col: column to create lags for (e.g., 'actual_eta_min')
        lags: list of lag orders

    Returns:
        DataFrame with lag columns added (gap-violating values = NaN)
    """
    df = df.copy()  # FIX: prevent SettingWithCopyWarning

    for lag in lags:
        col_name = f"{target_col}_lag{lag}"
        df[col_name] = df.groupby("direction")[target_col].shift(lag)

        # Gap-aware invalidation
        if "created_at" in df.columns:
            time_diff = df.groupby("direction")["created_at"].diff(lag)
            # NaN if gap > MAX_GAP_MINUTES
            gap_mask = time_diff > pd.Timedelta(minutes=MAX_GAP_MINUTES * lag)
            df.loc[gap_mask, col_name] = np.nan

    return df


# ======================================================
# FEATURE ENGINEERING
# ======================================================
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create time-based and lag features for XGBoost.

    Returns DataFrame with all engineered features.
    Lag features are gap-aware (NaN if gap > 15 min).
    """
    df = df.copy()  # FIX: prevent SettingWithCopyWarning

    # Time features
    if "created_at" in df.columns:
        df["hour"] = df["created_at"].dt.hour
        df["minute"] = df["created_at"].dt.minute
        df["day_of_week_num"] = df["created_at"].dt.dayofweek
        df["is_weekend"] = (df["created_at"].dt.dayofweek >= 5).astype(int)

        # Cyclical time encoding (Hyndman & Athanasopoulos 2021, Ch 7.4)
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # Gap-aware lag features
    if "actual_eta_min" in df.columns:
        df = create_lag_features(df, "actual_eta_min", lags=[1, 2, 3])

    return df


# ======================================================
# FEATURE COLUMNS (SCIENTIFICALLY VALID ONLY)
# ======================================================
FEATURE_COLS = [
    # Time features
    "hour", "minute", "day_of_week_num", "is_weekend",
    "hour_sin", "hour_cos",

    # Gap-aware lag features
    "actual_eta_min_lag1",
    "actual_eta_min_lag2",
    "actual_eta_min_lag3",

    # Exogenous features (genuinely external — not target-derived)
    "temperature", "rain_mm", "humidity", "wind_speed", "visibility_km",
    "pm2_5", "pm10", "aqi",
    "mrt_status", "mrt_headway",
    "distance_km",
    "is_anomaly",
]


# ======================================================
# WALK-FORWARD CROSS-VALIDATION (5 FOLDS)
# FIX: was 3 folds — insufficient for statistical validity
# Reference: Bergmeir & Benítez (2012)
# ======================================================
N_FOLDS = 5


def walk_forward_cv(
    df: pd.DataFrame,
    feature_cols: list,
    target_col: str = "actual_eta_min",
    n_folds: int = N_FOLDS
) -> Dict:
    """
    Time-ordered walk-forward cross-validation.

    For each fold k (1..n_folds):
        train = first k/(n_folds+1) of data
        test  = next 1/(n_folds+1) of data

    No data from the future is ever used for training.

    Returns:
        dict with per-fold MAE, mean MAE, std MAE, bootstrap 95% CI
    """
    _assert_no_leakage(feature_cols)

    df = df.sort_values("created_at").reset_index(drop=True)
    n = len(df)
    fold_size = n // (n_folds + 1)

    if fold_size < 20:
        logging.warning("[TRAINER] Insufficient data for walk-forward CV")
        return {"error": "Insufficient data", "fold_maes": []}

    fold_maes = []

    for k in range(1, n_folds + 1):
        train_end = fold_size * k
        test_end = fold_size * (k + 1)

        train_df = df.iloc[:train_end].copy()
        test_df = df.iloc[train_end:test_end].copy()

        # Impute NaN lags with median from TRAIN set only
        X_train = train_df[feature_cols].copy()  # FIX: .copy() to avoid warning
        y_train = train_df[target_col].values

        X_test = test_df[feature_cols].copy()
        y_test = test_df[target_col].values

        # Median imputation (train-derived — no leakage)
        train_medians = X_train.median()
        X_train = X_train.fillna(train_medians)
        X_test = X_test.fillna(train_medians)

        model = xgb.XGBRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )

        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        y_pred = model.predict(X_test)

        fold_mae = float(np.mean(np.abs(y_test - y_pred)))
        fold_maes.append(fold_mae)
        logging.info(f"[CV] Fold {k}/{n_folds}: MAE = {fold_mae:.4f}")

    mean_mae = float(np.mean(fold_maes))
    std_mae = float(np.std(fold_maes))

    # Bootstrap 95% CI on fold MAEs (Efron & Tibshirani 1993)
    rng = np.random.default_rng(seed=42)
    bootstrap_means = [
        float(np.mean(rng.choice(fold_maes, size=len(fold_maes), replace=True)))
        for _ in range(1000)
    ]
    ci_lo = float(np.quantile(bootstrap_means, 0.025))
    ci_hi = float(np.quantile(bootstrap_means, 0.975))

    result = {
        "fold_maes": fold_maes,
        "mean_mae": mean_mae,
        "std_mae": std_mae,
        "ci_95_lower": ci_lo,
        "ci_95_upper": ci_hi,
        "n_folds": n_folds
    }

    logging.info(
        f"[CV] Walk-forward {n_folds}-fold: MAE = {mean_mae:.4f} ± {std_mae:.4f} "
        f"(95% CI: [{ci_lo:.4f}, {ci_hi:.4f}])"
    )

    return result


# ======================================================
# FULL TRAINING
# ======================================================
def train_model() -> Optional[xgb.XGBRegressor]:
    """
    Full training pipeline:
    1. Load data (with leakage columns already removed by data_loader)
    2. Engineer features
    3. Assert no leakage (hard guard)
    4. Walk-forward CV
    5. Final model fit on all data
    6. Store metrics in DB
    """
    df = load_and_preprocess_data(days_lookback=30)
    if df.empty or len(df) < 100:
        logging.warning("[TRAINER] Insufficient data — skipping training")
        return None

    df = engineer_features(df)

    # Validate feature availability
    available_features = [c for c in FEATURE_COLS if c in df.columns]
    if len(available_features) < 5:
        logging.warning(f"[TRAINER] Only {len(available_features)} features available — skipping")
        return None

    target_col = "actual_eta_min"
    if target_col not in df.columns:
        logging.error("[TRAINER] Target column 'actual_eta_min' not found")
        return None

    # Drop rows where target is NaN
    df = df.dropna(subset=[target_col])

    # Hard leakage guard (CRITICAL)
    _assert_no_leakage(available_features)

    # Walk-forward CV
    cv_result = walk_forward_cv(df, available_features, target_col)

    # Final model (on ALL data)
    X = df[available_features].copy()
    y = df[target_col].values

    # Median imputation
    medians = X.median()
    X = X.fillna(medians)

    model = xgb.XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X, y, verbose=False)

    # Store metrics
    try:
        supabase.table("model_metrics").insert({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model_type": "xgboost",
            "n_samples": len(df),
            "n_features": len(available_features),
            "cv_mean_mae": cv_result.get("mean_mae"),
            "cv_std_mae": cv_result.get("std_mae"),
            "cv_ci95_lower": cv_result.get("ci_95_lower"),
            "cv_ci95_upper": cv_result.get("ci_95_upper"),
            "cv_n_folds": cv_result.get("n_folds"),
            "features_used": ",".join(available_features)
        }).execute()
    except Exception as e:
        logging.warning(f"[TRAINER] Could not store metrics: {e}")

    logging.info(f"[TRAINER] Model trained on {len(df)} samples with {len(available_features)} features")
    return model


# ======================================================
# 24-HOUR FORECAST — PER-HOUR MEDIAN LAG
# FIX: was using static last_lag → physically invalid for multi-hour horizon
# Correct: use per-hour historical median from training data
# Reference: Hyndman & Athanasopoulos (2021), Chapter 5
# ======================================================
def forecast_24h(model: xgb.XGBRegressor, df: pd.DataFrame) -> list:
    """
    Generate 24-hour forecast using per-hour historical medians.

    For each hour h in [0..23]:
        lag features = historical median of actual_eta_min at hour h
        (computed from training data only)

    FIX: Previous version used static last observed lag for ALL hours.
    This violates diurnal variation patterns and produces unrealistic
    flat forecasts.
    """
    if model is None or df.empty:
        return []

    available_features = [c for c in FEATURE_COLS if c in df.columns]
    _assert_no_leakage(available_features)

    now = datetime.now(BDT)
    forecasts = []

    # Per-hour historical medians (from training data)
    df_with_hour = df.copy()
    if "created_at" in df_with_hour.columns:
        df_with_hour["_hour"] = df_with_hour["created_at"].dt.hour
    elif "hour" in df_with_hour.columns:
        df_with_hour["_hour"] = df_with_hour["hour"]
    else:
        df_with_hour["_hour"] = 12  # fallback

    hourly_medians: Dict[int, float] = {}
    if "actual_eta_min" in df_with_hour.columns:
        # Cast explicitly to dict to satisfy Pyright
        raw_medians = df_with_hour.groupby("_hour")["actual_eta_min"].median().to_dict()
        hourly_medians = {int(k): float(v) for k, v in raw_medians.items()}

    global_median = float(df["actual_eta_min"].median()) if "actual_eta_min" in df.columns else 5.0

    for offset_h in range(24):
        forecast_time = now + timedelta(hours=offset_h)
        target_hour = forecast_time.hour

        row: Dict[str, float] = {}
        row["hour"] = float(target_hour)
        row["minute"] = float(forecast_time.minute)
        row["day_of_week_num"] = float(forecast_time.weekday())
        row["is_weekend"] = float(int(forecast_time.weekday() >= 5))
        row["hour_sin"] = float(np.sin(2 * np.pi * target_hour / 24))
        row["hour_cos"] = float(np.cos(2 * np.pi * target_hour / 24))

        # FIX: Per-hour median lag (NOT static last_lag)
        hour_median = float(hourly_medians.get(target_hour, global_median))
        row["actual_eta_min_lag1"] = hour_median
        row["actual_eta_min_lag2"] = hour_median
        row["actual_eta_min_lag3"] = hour_median

        # Exogenous — use last known values
        for col in available_features:
            if col not in row and col in df.columns:
                last_val = df[col].dropna().iloc[-1] if len(df[col].dropna()) > 0 else 0
                row[col] = float(last_val)

        # Suppress generic Pyright type instanciation on pandas
        input_row = pd.DataFrame([row]) # type: ignore
        input_row = input_row[available_features] # type: ignore
        input_row = input_row.fillna(0)

        pred = float(model.predict(input_row)[0])
        pred = max(0.5, min(pred, 60.0))  # Physical clamp

        forecasts.append({
            "hour": target_hour,
            "offset_h": offset_h,
            "predicted_eta_min": float(f"{pred:.2f}"),
            "timestamp": forecast_time.isoformat()
        })

    return forecasts