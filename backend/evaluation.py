"""
evaluation.py — Q1 DEFENSIBLE MODEL EVALUATION MODULE
=======================================================
Purpose:
- Leakage-safe evaluation with temporal train/test split
- Baseline: historical average computed from TRAIN set only
  FIX: was computed from test set (data leakage in baseline)
- Bootstrap confidence intervals for all metrics (Q1 mandatory)
- Corridor-wise performance breakdown

REFERENCES:
[1] Hyndman, R.J. & Koehler, A.B. (2006). Another look at measures of
    forecast accuracy. International Journal of Forecasting, 22(4), 679–688.
    https://doi.org/10.1016/j.ijforecast.2006.03.001

[2] Zheng, Y. et al. (2016). Urban computing: Big data, machine learning,
    and intelligent urban design. KDD 2016 Tutorial.
    https://doi.org/10.1145/2939672.2939692

[3] Makridakis, S. et al. (2020). The M4 Competition: 100,000 time series
    and 61 forecasting methods. International Journal of Forecasting, 36(1).
    https://doi.org/10.1016/j.ijforecast.2019.04.014

[4] Efron, B. & Tibshirani, R.J. (1993). An Introduction to the Bootstrap.
    Chapman & Hall/CRC. ISBN 0-412-04231-2.
"""

import numpy as np
import pandas as pd
import logging
from typing import Dict, Tuple

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
# BASELINE — TRAIN SET ONLY
# FIX: was computed from test_df (leakage in baseline)
# Correct: compute corridor means from train_df, map to test_df
# =========================================
def historical_average_baseline(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    target_col: str = "travel_time_min"
) -> pd.Series:
    """
    Baseline: per-corridor mean travel time computed from train set only.
    Mapped to test set corridors. Falls back to global train mean for
    corridors not seen in training.

    FIX: Previous version computed mean from test_df — data leakage.
    Reference: Hyndman & Koehler (2006).
    """
    corridor_means = train_df.groupby("corridor")[target_col].mean()
    global_mean = train_df[target_col].mean()
    return test_df["corridor"].map(corridor_means).fillna(global_mean)


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
    df = df.sort_values("timestamp").reset_index(drop=True)
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
    target_col: str = "travel_time_min"
) -> Dict:
    """
    Full evaluation with:
    - Temporal split
    - Baseline from train set only (no leakage)
    - Bootstrap 95% CI for MAE and RMSE
    - Corridor-wise MAE breakdown
    - Improvement delta vs baseline
    """
    report = {}

    # 1. Temporal split
    train_df, test_df = temporal_train_test_split(df)

    X_train = train_df[feature_cols]
    y_train = train_df[target_col].values

    X_test = test_df[feature_cols]
    y_test = test_df[target_col].values

    # 2. Train
    model.fit(X_train, y_train)

    # 3. Predict
    y_pred = model.predict(X_test)

    # 4. Baseline (from TRAIN set only — no leakage)
    # FIX: previously used test_df statistics
    baseline_pred = historical_average_baseline(train_df, test_df, target_col).values

    # 5. Model metrics
    report["model_mae"] = mae(y_test, y_pred)
    report["model_rmse"] = rmse(y_test, y_pred)
    report["model_mape"] = mape(y_test, y_pred)

    # 6. Bootstrap 95% CI (Efron & Tibshirani 1993)
    mae_lo, mae_hi = bootstrap_ci(y_test, y_pred, mae)
    rmse_lo, rmse_hi = bootstrap_ci(y_test, y_pred, rmse)
    report["model_mae_ci95"] = (mae_lo, mae_hi)
    report["model_rmse_ci95"] = (rmse_lo, rmse_hi)

    # 7. Baseline metrics
    report["baseline_mae"] = mae(y_test, baseline_pred)
    report["baseline_rmse"] = rmse(y_test, baseline_pred)
    report["baseline_mape"] = mape(y_test, baseline_pred)

    # 8. Improvement
    report["improvement_mae_pct"] = round(
        (report["baseline_mae"] - report["model_mae"]) / report["baseline_mae"] * 100, 2
    )
    report["improvement_rmse_pct"] = round(
        (report["baseline_rmse"] - report["model_rmse"]) / report["baseline_rmse"] * 100, 2
    )

    # 9. Error distribution
    errors = y_test - y_pred
    report["error_mean"] = float(np.mean(errors))
    report["error_std"] = float(np.std(errors))

    # 10. Corridor-wise MAE
    test_copy = test_df.copy()
    test_copy["_pred"] = y_pred
    report["corridor_mae"] = (
        test_copy.groupby("corridor")
        .apply(lambda g: mae(g[target_col].values, g["_pred"].values))
        .to_dict()
    )

    # 11. Sanity check
    if report["model_mae"] >= report["baseline_mae"]:
        logging.warning(
            "[EVAL] Model MAE >= baseline MAE — model fails to improve over naive baseline"
        )

    return report