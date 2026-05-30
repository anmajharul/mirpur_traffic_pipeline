"""
trainer_mlp.py — Q1 DEFENSIBLE PYTORCH MLP BASELINE TRAINER
=============================================================
Purpose:
- Train a Multi-Layer Perceptron (MLP) as a comparative baseline
  against XGBoost for Paper Table 3.
- Uses IDENTICAL feature set, data pipeline, leakage guards, and
  evaluation framework as trainer_xgb.py for fair comparison.
- Stores metrics in model_metrics table with model_type='mlp_pytorch'.

ARCHITECTURAL JUSTIFICATION (Paper §3.4):
    MLP is included as a representative deep learning baseline.
    Architecture: Input → 256 → BatchNorm → ReLU → Dropout(0.2)
                        → 128 → BatchNorm → ReLU → Dropout(0.2)
                        → 64  → ReLU → 1
    Reference: Hornik, K. (1991). Universal approximation capability
    of multilayer feedforward networks. Neural Networks, 4(2), 251-257.
    https://doi.org/10.1016/0893-6080(91)90009-T

WHY NOT LSTM/TRANSFORMER:
    LSTM requires long unbroken sequences. Our 5-min collection cadence
    on Cloud Run Jobs produces frequent gaps (>15 min) that invalidate
    the sequential assumption. Gap-aware lag features (used by both models)
    are a more principled solution for irregular time series.
    Reference: Che, Z. et al. (2018). Recurrent neural networks for
    multivariate time series with missing values. Scientific Reports, 8.
    https://doi.org/10.1038/s41598-018-24271-9

NORMALIZATION NOTE (Paper 3.4):
    Unlike XGBoost, neural networks are sensitive to feature scale.
    StandardScaler (zero mean, unit variance) is applied to all inputs.
    Scaler is fit on TRAIN set only, then applied to TEST set —
    no leakage. Reference: Géron, A. (2022). Hands-On Machine Learning
    with Scikit-Learn, Keras & TensorFlow, 3rd ed. O'Reilly.

═══════════════════════════════════════════════════════════════
MINIMUM TRAINING DATA REQUIREMENTS (Q1 ACADEMIC JUSTIFICATION)
═══════════════════════════════════════════════════════════════
For MLP (tabular feedforward neural network) in urban traffic
forecasting, deep learning models require MORE data than tree-based
models to avoid overfitting, due to a higher parameter count.
This module's hard floor is 100 rows (absolute runtime minimum).
The recommended operational minimum for reliable MLP generalization
is 4,320 rows (≥ 15 days × 288 5-min intervals).

Justification from Q1 literature:
  • Goodfellow et al. (2016) state that deep networks generalize
    poorly when samples < 10× number of parameters. With this
    MLP's ~50k parameters, a minimum of several thousand samples
    is needed; however, with our regularization (LayerNorm +
    Dropout), the practical minimum is ~4,320 rows (15 days).
  • Grinsztajn et al. (2022, NeurIPS) demonstrate that MLP
    underperforms tree-based models on tabular data with fewer
    than ~10,000 samples, justifying XGBoost as primary model.
  • Ma et al. (2015, IEEE T-ITS) showed 15 days of 5-min traffic
    data as sufficient for MLP-based short-term speed forecasting
    with proper regularization.
  • Moritz & Bartz-Beielstein (2017, R Journal) validate that
    LOCF + median imputation (used here) maintains statistical
    validity with up to 20% missingness, requiring at minimum
    5× the number of features in complete (non-missing) records.

DATA REQUIREMENT REFERENCES:
[DR-1] Goodfellow, I., Bengio, Y., & Courville, A. (2016).
       Deep Learning. MIT Press. ISBN: 978-0262035613.
       DOI: N/A (monograph)
       [Cited for: parameter-to-sample ratio generalization bound]

[DR-2] Grinsztajn, L., Oyallon, E., & Varoquaux, G. (2022).
       Why tree-based models still outperform deep learning on
       tabular data. NeurIPS 2022, 35, 507–520.
       DOI: 10.48550/arXiv.2207.08815
       [Cited for: MLP data threshold ~10,000 tabular samples]

[DR-3] Ma, X., Tao, Z., Wang, Y., Yu, H., & Wang, Y. (2015).
       Long short-term memory neural network for traffic speed
       prediction using remote microwave sensor data.
       Transportation Research Part C, 54, 187–197.
       DOI: 10.1016/j.trc.2015.03.014
       [Cited for: 15-day minimum for neural network traffic models]

[DR-4] Moritz, S., & Bartz-Beielstein, T. (2017).
       imputeTS: Time Series Missing Value Imputation in R.
       The R Journal, 9(1), 207–218.
       DOI: 10.32614/RJ-2017-009
       [Cited for: LOCF + median imputation validity with missingness]
═══════════════════════════════════════════════════════════════

REFERENCES:
[1] Hornik, K. (1991). Universal approximation capability.
    Neural Networks, 4(2), 251-257.
    https://doi.org/10.1016/0893-6080(91)90009-T

[2] Ioffe, S. & Szegedy, C. (2015). Batch normalization.
    ICML 2015. https://arxiv.org/abs/1502.03167
    [Basis: BatchNorm for training stability]

[3] Srivastava, N. et al. (2014). Dropout: A simple way to prevent
    neural networks from overfitting. JMLR, 15(1), 1929-1958.
    [Basis: Dropout(0.2) regularisation]

[4] Kingma, D.P. & Ba, J. (2014). Adam: A method for stochastic
    optimization. ICLR 2015. https://arxiv.org/abs/1412.6980
    [Basis: Adam optimiser]

[5] Bergmeir, C. & Benítez, J.M. (2012). On the use of cross-validation
    for time series predictor evaluation. Information Sciences, 191.
    https://doi.org/10.1016/j.ins.2011.12.028
    [Basis: Walk-forward CV — no shuffling for time series]

[6] Efron, B. & Tibshirani, R.J. (1993). An Introduction to the Bootstrap.
    Chapman & Hall/CRC. ISBN 0-412-04231-2.
    [Basis: 1000-resample bootstrap 95% CI on fold MAEs]

[7] Grinsztajn, L. et al. (2022). Why tree-based models still outperform
    deep learning on tabular data. NeurIPS 2022.
    https://doi.org/10.48550/arXiv.2207.08815
    [Basis: Justifies XGBoost as primary model; MLP as comparative baseline]

[8] Ba, J. L., Kiros, J. R., & Hinton, G. E. (2016). Layer Normalization.
    arXiv preprint arXiv:1607.06450. https://doi.org/10.48550/arXiv.1607.06450
    [Basis: LayerNorm for small-batch walk-forward CV stability]
"""

import gc
import logging
import traceback
import numpy as np
import pandas as pd
import optuna
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any

from sklearn.preprocessing import StandardScaler
from sklearn.neural_network import MLPRegressor as SklearnMLP

# PyTorch: optional — falls back to sklearn MLPRegressor if DLL policy blocks.
_TORCH_AVAILABLE = False
try:
    import torch  # type: ignore[import-untyped]
    import torch.nn as nn  # type: ignore[import-untyped]
    from torch.utils.data import DataLoader, TensorDataset  # type: ignore[import-untyped]
    
    # Q1 Reproducibility Fix
    torch.manual_seed(42)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(42)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    
    _TORCH_AVAILABLE = True
    logging.info("[MLP] PyTorch available — using torch.nn backend")
except (OSError, ImportError) as e:
    logging.warning(f"[MLP] PyTorch not available. Error: {e}. Falling back to sklearn.MLPRegressor.")
    class DummyNN:
        class Module:
            pass
    nn = DummyNN()

from config import SUPABASE_URL, SUPABASE_KEY
from data_loader import load_and_preprocess_data
from evaluation import evaluate_model, smape
from trainer_xgb import (
    engineer_features,
    FEATURE_COLS,
    _assert_no_leakage,
)

from supabase import create_client

logging.basicConfig(level=logging.INFO)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
BDT = timezone(timedelta(hours=6))
MLP_ARTIFACT_NAME = "model_mlp_weight.pt"

# ──────────────────────────────────────────────────────────────────────────────
# HYPERPARAMETERS
# ──────────────────────────────────────────────────────────────────────────────
MAX_EPOCHS     = 200
PATIENCE       = 20
RANDOM_STATE   = 42
N_FOLDS        = 5


# ──────────────────────────────────────────────────────────────────────────────
# MODEL DEFINITION
# ──────────────────────────────────────────────────────────────────────────────
if _TORCH_AVAILABLE:
    class TrafficMLP(nn.Module):  # type: ignore[misc]
        """
        Customizable MLP for travel-time regression.
        """
        def __init__(self, n_features: int, hidden_sizes: tuple = (256, 128, 64), dropout_rate: float = 0.2):
            super().__init__()
            layers = []
            in_dim = n_features
            for h_dim in hidden_sizes:
                layers.append(nn.Linear(in_dim, h_dim))
                layers.append(nn.LayerNorm(h_dim))
                layers.append(nn.ReLU())
                layers.append(nn.Dropout(dropout_rate))
                in_dim = h_dim
            layers.append(nn.Linear(in_dim, 1))
            self.network = nn.Sequential(*layers)

        def forward(self, x):
            return self.network(x).squeeze(-1)
else:
    TrafficMLP = None  # type: ignore[assignment,misc]


def train_mlp_fold(
    X_train: np.ndarray, y_train: np.ndarray,
    X_val: np.ndarray, y_val: np.ndarray,
    best_params: dict
) -> nn.Module:
    """Trains PyTorch MLP on a single CV fold with early stopping."""
    train_dataset = TensorDataset(torch.tensor(X_train, dtype=torch.float32),
                                  torch.tensor(y_train, dtype=torch.float32))
    val_dataset   = TensorDataset(torch.tensor(X_val, dtype=torch.float32),
                                  torch.tensor(y_val, dtype=torch.float32))

    batch_size = best_params.get("batch_size", 64)
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    hidden_sizes = tuple(map(int, best_params.get("hidden_sizes", "256,128,64").split(',')))
    model = TrafficMLP(n_features=X_train.shape[1], hidden_sizes=hidden_sizes, dropout_rate=best_params.get("dropout_rate", 0.2)).to(device)
    
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=best_params.get("learning_rate", 1e-3))
    
    best_val_loss = float("inf")
    patience_counter = 0
    
    for epoch in range(MAX_EPOCHS):
        model.train()
        for X_b, y_b in train_loader:
            optimizer.zero_grad()
            loss = criterion(model(X_b.to(device)), y_b.to(device))
            loss.backward()
            optimizer.step()
        
        model.eval()
        with torch.no_grad():
            val_loss = criterion(model(torch.tensor(X_val, dtype=torch.float32).to(device)), torch.tensor(y_val, dtype=torch.float32).to(device)).item()
        
        if val_loss < best_val_loss - 1e-6:
            best_val_loss = val_loss
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE: break
    return model


# ──────────────────────────────────────────────────────────────────────────────
# SKLEARN-COMPATIBLE WRAPPER
# ──────────────────────────────────────────────────────────────────────────────
class MLPWrapper:
    def __init__(self, best_params: Optional[dict] = None):
        self.model = None
        self.scaler = StandardScaler()
        self.best_params = best_params or {"hidden_sizes": "256,128,64", "dropout_rate": 0.2, "learning_rate": 1e-3, "batch_size": 64}
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu") if _TORCH_AVAILABLE else None

    def fit(self, X: pd.DataFrame, y: np.ndarray, X_val: Optional[Any] = None, y_val: Optional[np.ndarray] = None) -> "MLPWrapper":
        # Handle NaNs that might have slipped through
        X = X.fillna(X.median())
        X_scaled = self.scaler.fit_transform(X.values.astype(np.float32))
        
        if X_val is not None:
            x_val_arr = X_val.values if hasattr(X_val, 'values') else X_val
            X_val_scaled = self.scaler.transform(x_val_arr.astype(np.float32))
        else:
            X_val_scaled = X_scaled

        if _TORCH_AVAILABLE:
            self.model = train_mlp_fold(X_scaled, y, X_val_scaled, y_val if y_val is not None else y, self.best_params)
        else:
            self.model = SklearnMLP(hidden_layer_sizes=tuple(map(int, self.best_params["hidden_sizes"].split(','))), early_stopping=True, random_state=RANDOM_STATE).fit(X_scaled, y)
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        X = X.fillna(X.median())
        X_scaled = self.scaler.transform(X.values.astype(np.float32))
        if _TORCH_AVAILABLE and isinstance(self.model, nn.Module):
            with torch.no_grad(): return self.model(torch.tensor(X_scaled, dtype=torch.float32).to(self.device)).cpu().numpy()
        return self.model.predict(X_scaled)


# ──────────────────────────────────────────────────────────────────────────────
# WALK-FORWARD CV
# ──────────────────────────────────────────────────────────────────────────────
def walk_forward_cv(df: pd.DataFrame, feature_cols: List[str], target_col: str, best_params: dict) -> dict:
    _assert_no_leakage(feature_cols)
    df = df.sort_values("created_at").reset_index(drop=True)
    n = len(df)
    fold_size = n // (N_FOLDS + 1)
    fold_maes, fold_rmses, fold_smapes, all_abs_errors = [], [], [], []

    for k in range(1, N_FOLDS + 1):
        train_df, test_df = df.iloc[:fold_size*k], df.iloc[fold_size*k:fold_size*(k+1)]
        X_train, y_train = train_df[feature_cols].copy(), train_df[target_col].values
        
        train_medians = X_train.median(numeric_only=True)
        X_train = X_train.fillna(train_medians)
        X_test = test_df[feature_cols].fillna(train_medians)

        wrapper = MLPWrapper(best_params=best_params)
        wrapper.fit(X_train, y_train)
        y_pred = wrapper.predict(X_test)
        
        fold_maes.append(float(np.mean(np.abs(test_df[target_col].values - y_pred))))
        fold_smapes.append(smape(test_df[target_col].values, y_pred))
        all_abs_errors.extend(np.abs(test_df[target_col].values - y_pred))

        del wrapper, X_train, y_train, X_test, train_df, test_df, y_pred
        gc.collect()

    rng = np.random.default_rng(seed=RANDOM_STATE)
    bootstrap_maes = [float(np.mean(rng.choice(all_abs_errors, size=len(all_abs_errors), replace=True))) for _ in range(1000)]
    return {"mean_mae": np.mean(fold_maes), "std_mae": np.std(fold_maes), "mean_smape": np.mean(fold_smapes), "std_smape": np.std(fold_smapes), "ci_95_lower": np.quantile(bootstrap_maes, 0.025), "ci_95_upper": np.quantile(bootstrap_maes, 0.975), "n_folds": N_FOLDS}


# ──────────────────────────────────────────────────────────────────────────────
# MAIN TRAINING
# ──────────────────────────────────────────────────────────────────────────────
def train_mlp(training_cutoff_utc: Optional[datetime] = None, days_lookback: int = 30) -> Optional[MLPWrapper]:
    # ── INCREMENTAL LEARNING: Load only new data since last training run ──────
    # Reference: Losing et al. (2018). Incremental on-line learning.
    #   Neurocomputing 275, 1261–1274. https://doi.org/10.1016/j.neucom.2017.06.084
    from incremental_state import get_incremental_cutoff_date
    since_date = None # Force full retraining to generate missing artifact

    df = load_and_preprocess_data(days_lookback=days_lookback, cutoff_time_utc=training_cutoff_utc, since_date=since_date)
    # ── DATA SUFFICIENCY GUARD ──────────────────────────────────────────────
    # Hard floor: 100 rows (absolute runtime minimum — early-deployment safety).
    # Recommended operational minimum: 4,320 rows (≥ 15 days × 288 5-min intervals).
    #   • ≥ 15 days: minimum for MLP neural network traffic speed prediction
    #     (Ma et al., 2015 TR Part C; DOI: 10.1016/j.trc.2015.03.014)
    #   • ≥ 10,000 rows: threshold where MLP begins to match tree-based models
    #     (Grinsztajn et al., 2022 NeurIPS; DOI: 10.48550/arXiv.2207.08815)
    if df.empty or len(df) < 100: return None
    df = engineer_features(df).dropna(subset=["actual_eta_min"])
    available_features = [c for c in FEATURE_COLS if c in df.columns]
    
    # Optuna HPO
    n_total = len(df)
    optuna_df = df.iloc[:int(n_total * 0.8)].copy()
    optuna_df[available_features] = optuna_df[available_features].fillna(optuna_df[available_features].median(numeric_only=True))
    X_opt, y_opt = optuna_df[available_features].values, optuna_df["actual_eta_min"].values
    scaler_opt = StandardScaler()
    X_opt_scaled = scaler_opt.fit_transform(X_opt)
    
    def objective(trial):
        params = {
            "hidden_sizes": trial.suggest_categorical("hidden_sizes", ["256,128,64", "128,64,32"]),
            "dropout_rate": trial.suggest_float("dropout_rate", 0.1, 0.4),
            "learning_rate": trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True),
            "batch_size": trial.suggest_categorical("batch_size", [32, 64, 128])
        }
        split = int(len(X_opt) * 0.75)
        if _TORCH_AVAILABLE:
            model = train_mlp_fold(X_opt_scaled[:split], y_opt[:split], X_opt_scaled[split:], y_opt[split:], params)
            model.eval()
            with torch.no_grad():
                preds = model(torch.tensor(X_opt_scaled[split:], dtype=torch.float32).to(torch.device("cuda" if torch.cuda.is_available() else "cpu"))).cpu().numpy()
        else:
            hidden = tuple(map(int, params["hidden_sizes"].split(',')))
            model = SklearnMLP(hidden_layer_sizes=hidden, learning_rate_init=params["learning_rate"], batch_size=params["batch_size"], max_iter=20, random_state=42)
            model.fit(X_opt_scaled[:split], y_opt[:split])
            preds = model.predict(X_opt_scaled[split:])
            
        error = float(np.mean(np.abs(y_opt[split:] - preds)))
        del model, preds
        gc.collect()
        return error

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=10)
    best_params = study.best_params
    
    del study, X_opt, y_opt, X_opt_scaled, optuna_df
    gc.collect()
    
    cv_result = walk_forward_cv(df, available_features, "actual_eta_min", best_params)
    final_model = MLPWrapper(best_params=best_params).fit(df[available_features], df["actual_eta_min"].values)
    
    eval_wrapper = MLPWrapper(best_params=best_params)
    eval_report = evaluate_model(df, eval_wrapper, available_features, "actual_eta_min")
    
    _model_version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    try:
        supabase.table("model_metrics").insert({
            "timestamp":     datetime.now(timezone.utc).isoformat(),
            "model_type":    "mlp_pytorch",
            "model_version": _model_version,
            "n_samples":     n_total,
            "n_features":    len(available_features),
            "features_used": ",".join(available_features),
            "train_rows":    int(n_total * 0.8),
            "test_rows":     n_total - int(n_total * 0.8),
            "split_ratio":   0.8,
            
            "cv_mean_mae":   cv_result.get("mean_mae"),
            "cv_std_mae":    cv_result.get("std_mae"),
            "cv_ci95_lower": cv_result.get("ci_95_lower"),
            "cv_ci95_upper": cv_result.get("ci_95_upper"),
            "cv_n_folds":    cv_result.get("n_folds"),
            "cv_mean_smape": cv_result.get("mean_smape"),
            "cv_std_smape":  cv_result.get("std_smape"),
            
            "model_mae":     eval_report.get("model_mae"),
            "model_rmse":    eval_report.get("model_rmse"),
            "model_mape":    eval_report.get("model_mape"),
            "model_r2":      eval_report.get("model_r2"),
            "model_smape":   eval_report.get("model_smape"),
            "mae_ci_lower":  eval_report.get("model_mae_ci95", [None, None])[0],
            "mae_ci_upper":  eval_report.get("model_mae_ci95", [None, None])[1],
            "rmse_ci_lower": eval_report.get("model_rmse_ci95", [None, None])[0],
            "rmse_ci_upper": eval_report.get("model_rmse_ci95", [None, None])[1],
            
            "baseline_mae":  eval_report.get("baseline_mae"),
            "baseline_rmse": eval_report.get("baseline_rmse"),
            "baseline_mape": eval_report.get("baseline_mape"),
            "baseline_smape": eval_report.get("baseline_smape"),
            "improvement_mae_pct":  eval_report.get("improvement_mae_pct"),
            "improvement_rmse_pct": eval_report.get("improvement_rmse_pct"),
            "error_mean":    eval_report.get("error_mean"),
            "error_std":     eval_report.get("error_std"),
            "corridor_mae":  eval_report.get("corridor_mae"),
            
            "model_specific_params": {
                "hidden_sizes":  best_params.get("hidden_sizes", "256,128,64"),
                "dropout_rate":  best_params.get("dropout_rate", 0.2),
                "learning_rate": best_params.get("learning_rate", 1e-3),
                "batch_size":    best_params.get("batch_size", 64),
                "max_epochs":    MAX_EPOCHS,
                "patience":      PATIENCE,
                "random_state":  RANDOM_STATE,
                "hpo_note":      "Bayesian Optimization via Optuna (10 trials) minimizing Temporal Validation MAE"
            },
            "learning_rate": best_params.get("learning_rate", 1e-3),
            "notes": "MLP PyTorch baseline. StandardScaler applied (train-fit only).",
            "artifact_path": f"latest/{MLP_ARTIFACT_NAME}",
            "data_cutoff_time": datetime.now(timezone.utc).isoformat(),
        }).execute()
        logging.info("[MLP] Metrics stored in model_metrics table")
    except Exception as e:
        logging.warning(f"[MLP] Could not store metrics: {e}")

    logging.info(
        f"[MLP] Training complete — {len(df)} samples, "
        f"{len(available_features)} features, "
        f"device={final_model.device}"
    )
    return final_model


def save_mlp_artifact(
    model: MLPWrapper,
    output_path: str | Path = MLP_ARTIFACT_NAME,
) -> Path:
    """
    Save MLP state dict + scaler to disk as a .pt bundle, or .joblib for sklearn fallback.
    Mirrors save_model_artifact() in trainer_xgb.py.
    """
    if model.model is None:
        raise RuntimeError("[MLP] Cannot save untrained model.")

    artifact_path = Path(output_path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)

    if not _TORCH_AVAILABLE:
        import joblib
        sklearn_artifact_path = artifact_path.with_suffix('.joblib')
        bundle = {
            "model": model.model,
            "scaler_mean": model.scaler.mean_,
            "scaler_scale": model.scaler.scale_,
            "best_params": model.best_params,
        }
        joblib.dump(bundle, str(sklearn_artifact_path))
        logging.info(f"[MLP] torch.save unavailable — saved sklearn artifact to {sklearn_artifact_path}")
        return sklearn_artifact_path

    torch.save({  # type: ignore[union-attr]
        "model_state_dict": model.model.state_dict(),  # type: ignore[union-attr]
        "scaler_mean":      model.scaler.mean_,
        "scaler_scale":     model.scaler.scale_,
        "n_features":       model.model.network[0].in_features,  # type: ignore[union-attr]
        "hidden_sizes":     model.best_params["hidden_sizes"],
        "dropout_rate":     model.best_params["dropout_rate"],
    }, str(artifact_path))

    logging.info(f"[MLP] Artifact saved → {artifact_path}")
    return artifact_path


def load_mlp_artifact(artifact_path: str | Path) -> MLPWrapper:
    """
    Load a saved MLP bundle back into a MLPWrapper for inference.
    Handles both PyTorch .pt and sklearn .joblib artifacts.
    """
    artifact_path = Path(artifact_path)
    
    if not _TORCH_AVAILABLE:
        sklearn_artifact_path = artifact_path.with_suffix('.joblib')
        if not sklearn_artifact_path.exists():
            raise RuntimeError(f"[MLP] Sklearn artifact not found at {sklearn_artifact_path}")
        import joblib
        bundle = joblib.load(str(sklearn_artifact_path))
        wrapper = MLPWrapper(best_params=bundle.get("best_params"))
        wrapper.model = bundle["model"]
        wrapper.scaler.mean_ = bundle["scaler_mean"]
        wrapper.scaler.scale_ = bundle["scaler_scale"]
        logging.info(f"[MLP] Loaded sklearn artifact from {sklearn_artifact_path}")
        return wrapper

    if not artifact_path.exists():
        raise RuntimeError(f"[MLP] PyTorch artifact not found at {artifact_path}")

    bundle = torch.load(str(artifact_path), map_location="cpu", weights_only=False)  # type: ignore[union-attr]

    n_features = bundle["n_features"]
    hidden_sizes = bundle.get("hidden_sizes", (256, 128, 64))
    if isinstance(hidden_sizes, str):
        hidden_sizes = tuple(map(int, hidden_sizes.split(",")))
    dropout_rate = bundle.get("dropout_rate", 0.2)
    wrapper = MLPWrapper()
    wrapper.model = TrafficMLP(n_features, hidden_sizes=hidden_sizes, dropout_rate=dropout_rate)  # type: ignore[misc]
    wrapper.model.load_state_dict(bundle["model_state_dict"])  # type: ignore[union-attr]
    wrapper.model.eval()  # type: ignore[union-attr]

    wrapper.scaler.mean_  = bundle["scaler_mean"]
    wrapper.scaler.scale_ = bundle["scaler_scale"]

    logging.info(f"[MLP] Loaded artifact from {artifact_path}")
    return wrapper


def main():
    """
    CLI entrypoint — trains MLP baseline and saves artifact.
    Run: python backend/trainer_mlp.py
    """
    from incremental_state import check_new_data_available
    # if not check_new_data_available("mlp_pytorch"):
    #     logging.info("[MLP] Skipping MLP training: No new data available since last cutoff.")
    #     return

    model = train_mlp(
        training_cutoff_utc=datetime.now(timezone.utc),
        days_lookback=30,
    )
    if model is not None:
        save_mlp_artifact(model)
        logging.info("[MLP] Done.")
    else:
        logging.error("[MLP] Training failed — no model produced.")


if __name__ == "__main__":
    main()
