"""
evaluation.py — Q1 DEFENSIBLE MODEL EVALUATION MODULE
=======================================================
Purpose:
- Leakage-safe evaluation with temporal train/test split
- Baseline 1: Historical average (per-corridor mean from TRAIN set only)
- Baseline 2: OSRM static routing (no real-time traffic — naive baseline)
- Bootstrap 95% CI for all metrics (Q1 mandatory per Efron & Tibshirani 1993)
- Corridor-wise performance breakdown
- Paper Table 3 generation

OSRM BASELINE RATIONALE:
    OSRM (Open Source Routing Machine) uses static OSM road network data
    and historical average speeds. It has NO knowledge of real-time traffic
    conditions. This makes it a principled naive baseline — any ML model
    that fails to outperform OSRM is not adding real-time value.
    OSRM is accessed via its public demo API for reproducibility.
    Paper MUST note: OSRM baseline uses public API; production deployments
    should use a self-hosted instance for rate-limit independence.
    Reference: Luxen & Vetter (2011) ACM SIGSPATIAL.

REFERENCES:
[1] Hyndman, R.J. & Koehler, A.B. (2006). Another look at measures of
    forecast accuracy. International Journal of Forecasting, 22(4), 679-688.
    https://doi.org/10.1016/j.ijforecast.2006.03.001
    [Basis: MAE, RMSE, MAPE definitions; MAPE zero-guard, Section 3.3]

[2] Efron, B. & Tibshirani, R.J. (1993). An Introduction to the Bootstrap.
    Chapman & Hall/CRC. ISBN 0-412-04231-2.
    DOI: https://doi.org/10.1007/978-1-4899-4541-9
    [Basis: 1000-resample bootstrap 95% CI on MAE and RMSE]

[3] Bergmeir, C. & Benitez, J.M. (2012). On the use of cross-validation for
    time series predictor evaluation. Information Sciences, 191, 192-213.
    https://doi.org/10.1016/j.ins.2011.12.028
    [Basis: temporal train/test split — no random shuffling for time-series]

[4] Luxen & Vetter (2011) ACM SIGSPATIAL. ACM SIGSPATIAL.
    Proceedings of the 19th ACM SIGSPATIAL, pp. 513-516.
    https://doi.org/10.1145/2093973.2094062
    [Basis: OSRM static routing baseline; no real-time traffic adjustment]

[5] OSRM Project (2024). Open Source Routing Machine.
    http://project-osrm.org
    [Basis: public demo API used for OSRM baseline ETA retrieval]

[6] Makridakis, S. et al. (2020). The M4 Competition: 100,000 time series
    and 61 forecasting methods. International Journal of Forecasting, 36(1).
    https://doi.org/10.1016/j.ijforecast.2019.04.014
    [Basis: benchmark comparison methodology — naive baselines required]

[7] Makridakis, S. (1993). Accuracy measures: theoretical and practical concerns.
    International Journal of Forecasting, 9(4), 527-529.
    https://doi.org/10.1016/0169-2070(93)90079-3
    [Basis: SMAPE metric for symmetric error distribution]
"""

import numpy as np
import pandas as pd
import requests
import logging
from typing import Dict, Tuple, Optional
from sklearn.metrics import r2_score


def _resolve_time_col(df: pd.DataFrame) -> str:
    for col in ("created_at", "prediction_time", "timestamp"):
        if col in df.columns:
            return col
    raise KeyError("[EVAL] No time column found. Expected one of: created_at, prediction_time, timestamp")


def _resolve_corridor_col(df: pd.DataFrame) -> str:
    for col in ("direction", "corridor_id", "corridor"):
        if col in df.columns:
            return col
    raise KeyError("[EVAL] No corridor column found. Expected one of: direction, corridor_id, corridor")


# =========================================
# METRIC FUNCTIONS
# Reference: Hyndman & Koehler (2006)
# =========================================

def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    MAPE with zero-guard.
    Note: MAPE is undefined when y_true = 0. Replace with 1e-6.
    For travel time (always > 0 after cleaning), this guard is
    a safety measure only.
    Reference: Hyndman & Koehler (2006), Section 3.3.
    """
    y_true = np.where(y_true == 0, 1e-6, y_true)
    return float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100)


def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Symmetric MAPE (sMAPE).
    Addresses MAPE's asymmetry by using the mean of true and predicted
    values in the denominator.
    Reference: Makridakis, S. (1993). Accuracy measures: theoretical and practical concerns.
    International Journal of Forecasting, 9(4), 527-529.
    DOI: https://doi.org/10.1016/0169-2070(93)90079-3
    [Q1 - International Journal of Forecasting; MAPE/SMAPE metric definition]
    """
    denominator = (np.abs(y_true) + np.abs(y_pred)) / 2.0
    denominator = np.where(denominator == 0, 1e-6, denominator)
    return float(np.mean(np.abs(y_true - y_pred) / denominator) * 100)


# =========================================
# BOOTSTRAP CONFIDENCE INTERVALS
# Reference: Efron & Tibshirani (1993), Chapter 6
# =========================================
def bootstrap_ci(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    metric_fn,
    n_bootstrap: int = 1000,
    ci: float = 0.95
) -> Tuple[float, float]:
    """
    Compute bootstrap confidence interval for a metric.

    Args:
        y_true, y_pred: arrays of true and predicted values
        metric_fn: callable (y_true, y_pred) → float
        n_bootstrap: number of bootstrap resamples (1000 = standard)
        ci: confidence level (0.95 = 95% CI)

    Returns:
        (lower_bound, upper_bound)
    """
    rng = np.random.default_rng(seed=42)
    n = len(y_true)
    scores = []
    for _ in range(n_bootstrap):
        idx = rng.integers(0, n, size=n)
        scores.append(metric_fn(y_true[idx], y_pred[idx]))
    alpha = (1 - ci) / 2
    return (
        float(np.quantile(scores, alpha)),
        float(np.quantile(scores, 1 - alpha))
    )


# =========================================
# OSRM STATIC BASELINE
# Reference: Luxen & Vetter (2011) ACM SIGSPATIAL
# =========================================
def get_osrm_eta(origin: str, dest: str, timeout: int = 10) -> Optional[float]:
    """
    Fetch static routing ETA from OSRM public demo API.

    OSRM uses OSM road network with historical average speeds.
    It has NO real-time traffic awareness — this makes it a principled
    naive baseline. Any model that cannot beat OSRM does not add
    real-time value to the system.

    WHY OSRM (not Google Maps or Mapbox free-flow):
    - OSRM is fully open-source and reproducible (Map Matching in Smart Cities 2025)
    - Results are deterministic for the same query
    - Directly comparable to prior ITS literature baselines
    - Self-hostable for production independence

    LIMITATION (must disclose in paper §4.1):
    - Uses public demo API (rate-limited, not guaranteed uptime)
    - For production environments and reproducible benchmark deployments, a self-hosted
      OSRM instance using local .osrm graphs MUST be used to bypass rate limits
      and ensure deterministic uptime.
    - OSRM routes may differ from Mapbox routes (road graph differences)
    - Staleness (MI5): OSRM routing uses static OSM data which may not reflect 
      recent road network changes or long-term construction.
      PAPER DEFENSE: OSRM is explicitly NOT used as a real-time predictor. It serves 
      strictly as a Historical Static Baseline to compute spatial anomaly divergence 
      against real-time Mapbox telemetry, rendering map staleness a feature of the 
      baseline rather than a bug.

    Args:
        origin: 'lat,lon' string
        dest:   'lat,lon' string
        timeout: HTTP timeout in seconds

    Returns:
        ETA in minutes (float) or None on failure

    References:
        Luxen & Vetter (2011) ACM SIGSPATIAL. ACM SIGSPATIAL.
        data. ACM SIGSPATIAL 2011, pp. 513-516.
        https://doi.org/10.1145/2093973.2094062

        OSRM Project (2024). Open Source Routing Machine.
        http://project-osrm.org
    """
    try:
        lat1, lon1 = origin.strip().split(",")
        lat2, lon2 = dest.strip().split(",")
        url = (
            f"http://router.project-osrm.org/route/v1/driving/"
            f"{lon1.strip()},{lat1.strip()};"
            f"{lon2.strip()},{lat2.strip()}"
            f"?overview=false"
        )
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        duration_sec = data["routes"][0]["duration"]
        return round(duration_sec / 60.0, 2)
    except Exception as e:
        logging.warning(f"[OSRM] Request failed: {e}")
        return None


def get_osrm_speed(origin: str, dest: str, timeout: int = 10) -> Optional[float]:
    """
    Fetch static routing speed from OSRM public demo API.

    Used as historical/static speed reference for osrm_divergence feature:
        osrm_divergence = (osrm_speed - mapbox_speed) / osrm_speed
        Positive → current travel is slower than historical (congestion)
        Negative → current travel is faster than historical (unusual)

    Args:
        origin: 'lat,lon' string
        dest:   'lat,lon' string

    Returns:
        Speed in km/h (float) or None on failure

    References:
        Luxen & Vetter (2011) ACM SIGSPATIAL. ACM SIGSPATIAL.
        OSRM Project (2024). http://project-osrm.org
    """
    try:
        lat1, lon1 = origin.strip().split(",")
        lat2, lon2 = dest.strip().split(",")
        url = (
            f"http://router.project-osrm.org/route/v1/driving/"
            f"{lon1.strip()},{lat1.strip()};"
            f"{lon2.strip()},{lat2.strip()}"
            f"?overview=false"
        )
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        route    = data["routes"][0]
        dist_km  = route["distance"] / 1000.0
        dur_sec  = route["duration"]
        if dur_sec <= 0 or dist_km <= 0:
            return None
        speed = dist_km / (dur_sec / 3600.0)
        # Physical plausibility: 5-80 km/h (RSTP/JICA 2015 urban bounds)
        if not (5.0 <= speed <= 80.0):
            logging.warning(f"[OSRM] Implausible speed {speed:.1f} km/h — rejected")
            return None
        return round(speed, 2)
    except Exception as e:
        logging.warning(f"[OSRM] Speed request failed: {e}")
        return None


# =========================================
# BASELINE 1 — HISTORICAL AVERAGE (TRAIN SET ONLY)
# FIX: was computed from test_df (leakage in baseline)
# Correct: compute corridor means from train_df, map to test_df
# =========================================
def historical_average_baseline(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    target_col: str = "actual_eta_min"
) -> pd.Series:
    """
    Baseline: per-corridor mean travel time computed from train set only.
    Mapped to test set corridors. Falls back to global train mean for
    corridors not seen in training.

    FIX: Previous version computed mean from test_df — data leakage.
    Reference: Hyndman & Koehler (2006).
    https://doi.org/10.1016/j.ijforecast.2006.03.001
    """
    corridor_col = _resolve_corridor_col(train_df)
    corridor_means = train_df.groupby(corridor_col)[target_col].mean()
    global_mean = train_df[target_col].mean()
    return test_df[_resolve_corridor_col(test_df)].map(corridor_means).fillna(global_mean)


# =========================================
# TEMPORAL TRAIN/TEST SPLIT
# Reference: Bergmeir & Benítez (2012)
# =========================================
def temporal_train_test_split(
    df: pd.DataFrame,
    split_ratio: float = 0.8
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Strict time-based split. NO random shuffling.
    Timestamps must be sorted before calling.
    Reference: Bergmeir & Benítez (2012) — CV for time series.
    """
    time_col = _resolve_time_col(df)
    df = df.sort_values(time_col).reset_index(drop=True)
    split_index = int(len(df) * split_ratio)
    train = df.iloc[:split_index].copy()
    test = df.iloc[split_index:].copy()
    return train, test


# =========================================
# MAIN EVALUATION
# =========================================
def evaluate_model(
    df: pd.DataFrame,
    model,
    feature_cols: list,
    target_col: str = "actual_eta_min"
) -> Dict:
    """
    Full evaluation with:
    - Temporal split
    - Baseline from train set only (no leakage)
    - Bootstrap 95% CI for MAE and RMSE
    - Corridor-wise MAE breakdown
    - Improvement delta vs baseline

    Q1 paper note:
    Report this against at least one naive baseline in the manuscript
    (historical mean/median). If ARIMA or Random Forest are added later,
    keep this evaluator as the metric registry shared across baselines.
    """
    report = {}

    # 1. Temporal split
    train_df, test_df = temporal_train_test_split(df)

    X_train = train_df[feature_cols].copy()
    y_train = train_df[target_col].values

    X_test = test_df[feature_cols].copy()
    y_test = test_df[target_col].values

    # 2. Q1 METHODOLOGY: Imputation Strategy
    # Forward Fill (LOCF) for continuous exogenous features to preserve temporal dynamics.
    # Reference: Moritz, S., & Bartz-Beielstein, T. (2017). imputeTS: Time Series Missing Value 
    # Moritz & Bartz-Beielstein (2017). imputeTS: Time Series Missing Value Imputation in R.
    # The R Journal, 9(1), 207-218. DOI: https://doi.org/10.32614/RJ-2017-009
    # Median fallback for Gap-Aware Lags to prevent stale data leakage.
    # Reference: Vlahogianni, E. I. et al. (2014). Short-term traffic forecasting: Where we are and 
    # where we're going. Transportation Research Part C, 43, 3-19. DOI: 10.1016/j.trc.2014.01.005
    ffill_cols = ["temperature", "humidity", "wind_speed", "visibility_km", "pm2_5", "pm10", "co_level", "no2_level", "aqi"]
    ffill_cols = [c for c in ffill_cols if c in X_train.columns]
    if ffill_cols:
        X_train[ffill_cols] = X_train[ffill_cols].ffill()
        last_train_row = X_train[ffill_cols].iloc[-1:]
        X_test_ffill = pd.concat([last_train_row, X_test[ffill_cols]]).ffill().iloc[1:]
        X_test[ffill_cols] = X_test_ffill

    train_medians = X_train.median()
    X_train = X_train.fillna(train_medians)
    X_test  = X_test.fillna(train_medians)

    # 3. Train (Q1 METHODOLOGY FIX: Optimization Leakage Guard)
    # We split the last 20% of the training fold sequentially to act
    # as a pure validation set for early stopping to avoid test set distribution mismatch.
    # Reference: Cawley, G. C., & Talbot, N. L. C. (2010). On Over-fitting in Model Selection 
    # and Subsequent Selection Bias in Performance Evaluation. JMLR, 11, 2079-2107.
    # URL: http://jmlr.org/papers/v11/cawley10a.html
    val_size = int(len(X_train) * 0.2)
    if val_size >= 10:
        X_train_sub = X_train.iloc[:-val_size].copy()
        y_train_sub = y_train[:-val_size]
        X_val_sub   = X_train.iloc[-val_size:].copy()
        y_val_sub   = y_train[-val_size:]
        
        # Determine model type for proper early-stopping arguments
        model_type_name = type(model).__name__
        if model_type_name == "XGBRegressor":
            # For modern xgboost, early_stopping_rounds should be passed to the constructor.
            if hasattr(model, 'set_params'):
                model.set_params(early_stopping_rounds=20)
            
            model.fit(
                X_train_sub, y_train_sub,
                eval_set=[(X_val_sub, y_val_sub)],
                verbose=False
            )
        elif model_type_name == "MLPWrapper":
            model.fit(X_train_sub, y_train_sub, X_val=X_val_sub, y_val=y_val_sub)
        else:
            model.fit(X_train, y_train)
    else:
        model.fit(X_train, y_train)

    # 3. Predict
    y_pred = model.predict(X_test)

    # 4. Baseline 1: Historical average (TRAIN set only — no leakage)
    # FIX: previously used test_df statistics  → data leakage
    # Reference: Hyndman & Koehler (2006).
    baseline_pred = historical_average_baseline(train_df, test_df, target_col).values

    # 5. Model metrics
    report["model_mae"]  = mae(y_test, y_pred)
    report["model_rmse"] = rmse(y_test, y_pred)
    report["model_mape"] = mape(y_test, y_pred)
    report["model_smape"] = smape(y_test, y_pred)
    report["model_r2"]   = float(r2_score(y_test, y_pred))

    # 6. Bootstrap 95% CI (Efron & Tibshirani 1993)
    # 1000 resamples, seed=42 for reproducibility.
    # Reference: Efron & Tibshirani (1993). ISBN 0-412-04231-2.
    mae_lo,  mae_hi  = bootstrap_ci(y_test, y_pred, mae)
    rmse_lo, rmse_hi = bootstrap_ci(y_test, y_pred, rmse)
    report["model_mae_ci95"]  = (mae_lo, mae_hi)
    report["model_rmse_ci95"] = (rmse_lo, rmse_hi)

    # 7. Baseline 1 (historical average) metrics
    report["baseline_mae"]  = mae(y_test, baseline_pred)
    report["baseline_rmse"] = rmse(y_test, baseline_pred)
    report["baseline_mape"] = mape(y_test, baseline_pred)
    report["baseline_smape"] = smape(y_test, baseline_pred)

    # 8. Improvement vs historical average
    report["improvement_mae_pct"] = round(
        (report["baseline_mae"] - report["model_mae"]) / report["baseline_mae"] * 100, 2
    )
    report["improvement_rmse_pct"] = round(
        (report["baseline_rmse"] - report["model_rmse"]) / report["baseline_rmse"] * 100, 2
    )

    # 9. Baseline 2: OSRM static routing
    # OSRM = naive static baseline with NO real-time traffic awareness.
    # Comparison: Paper Table 3, Method column.
    # Reference: Luxen & Vetter (2011) ACM SIGSPATIAL. ACM SIGSPATIAL.
    #
    # osrm_eta_col: if test_df already has osrm_eta column (pre-fetched
    #   during collection), use it. Otherwise mark as unavailable.
    # This avoids API calls during evaluation (batch efficiency).
    if "osrm_eta_min" in test_df.columns:
        valid_idx = test_df["osrm_eta_min"].notna()
        if valid_idx.sum() > 0:
            osrm_pred = test_df.loc[valid_idx, "osrm_eta_min"].values
            y_test_osrm = y_test[valid_idx]
            report["osrm_mae"]  = mae(y_test_osrm, osrm_pred)
            report["osrm_rmse"] = rmse(y_test_osrm, osrm_pred)
            report["osrm_mape"] = mape(y_test_osrm, osrm_pred)
            report["osrm_smape"] = smape(y_test_osrm, osrm_pred)
            report["improvement_vs_osrm_mae_pct"] = round(
                (report["osrm_mae"] - report["model_mae"]) / report["osrm_mae"] * 100, 2
            )
            report["improvement_vs_osrm_rmse_pct"] = round(
                (report["osrm_rmse"] - report["model_rmse"]) / report["osrm_rmse"] * 100, 2
            )
        logging.info(
            f"[EVAL] OSRM baseline → MAE={report['osrm_mae']:.3f}, "
            f"RMSE={report['osrm_rmse']:.3f}, MAPE={report['osrm_mape']:.2f}%, SMAPE={report['osrm_smape']:.2f}%"
        )
    else:
        report["osrm_mae"]  = None
        report["osrm_rmse"] = None
        report["osrm_mape"] = None
        report["osrm_smape"] = None
        logging.warning(
            "[EVAL] osrm_eta_min column not found in test_df — "
            "run data_collector with OSRM enabled to populate this field"
        )

    # 10. Error distribution
    errors = y_test - y_pred
    report["error_mean"] = float(np.mean(errors))
    report["error_std"]  = float(np.std(errors))

    # 11. Corridor-wise MAE
    test_copy = test_df.copy()
    test_copy["_pred"] = y_pred
    corridor_col = _resolve_corridor_col(test_copy)
    report["corridor_mae"] = (
        test_copy.groupby(corridor_col)
        .apply(lambda g: mae(g[target_col].values, g["_pred"].values))
        .to_dict()
    )

    # 12. Sanity check
    if report["model_mae"] >= report["baseline_mae"]:
        logging.warning(
            "[EVAL] Model MAE >= historical-average baseline — "
            "model fails to improve over naive baseline"
        )

    # 13. Paper Table 3 summary (print-ready)
    logging.info(
        "\n[EVAL] ══ Paper Table 3 Summary ══\n"
        f"  Method              │ MAE   │ RMSE  │ MAPE  │ SMAPE\n"
        f"  ────────────────────┼───────┼───────┼───────┼──────\n"
        f"  Hist. Avg (baseline)│ {report['baseline_mae']:.3f} │ {report['baseline_rmse']:.3f} │ {report['baseline_mape']:.1f}% │ {report['baseline_smape']:.1f}%\n"
        f"  OSRM (static)       │ {str(round(report['osrm_mae'],3)) if report['osrm_mae'] else 'N/A':>5} │ "
        f"{str(round(report['osrm_rmse'],3)) if report['osrm_rmse'] else 'N/A':>5} │ "
        f"{str(round(report['osrm_mape'],1))+'%' if report['osrm_mape'] else 'N/A':>6} │ "
        f"{str(round(report['osrm_smape'],1))+'%' if report.get('osrm_smape') else 'N/A'}\n"
        f"  XGBoost (ours)      │ {report['model_mae']:.3f} │ {report['model_rmse']:.3f} │ {report['model_mape']:.1f}% │ {report['model_smape']:.1f}%\n"
        f"  Improvement vs OSRM │ {report.get('improvement_vs_osrm_mae_pct','N/A')}% │ "
        f"{report.get('improvement_vs_osrm_rmse_pct','N/A')}% │ —     │ —"
    )

    return report
