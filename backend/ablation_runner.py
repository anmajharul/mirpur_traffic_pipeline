"""
ablation_runner.py — AUTOMATED ABLATION STUDY (PAPER TABLE 4)
=============================================================
This script systematically disables components of the TCN-TFT SOTA
model to prove the architectural necessity of each piece. 

Experiments:
1. Full Model (TCN + Attention + Weather)
2. No Weather (Proves Multimodal Fusion value)
3. No Attention (Proves Global Temporal Memory value)
4. No TCN (Proves Local Temporal Convolutions value)
"""

import os
import logging
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from data_loader import load_and_preprocess_data
from trainer_tcn_tft import (
    TimeWindowDataset, TCN_TFT_Hybrid, QuantileLoss,
    SEQ_LEN, PRED_LEN, QUANTILES, HIDDEN_SIZE, NUM_HEADS,
    EARLY_STOP_PATIENCE, MAX_EPOCHS, RANDOM_STATE
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def run_experiment(name: str, df: pd.DataFrame, target_col: str, 
                   use_tcn: bool, use_attention: bool, exclude_weather: bool):
    logging.info(f"\n{'='*50}\nSTARTING EXPERIMENT: {name}\n{'='*50}")
    
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # ── 1. Feature Setup ──
    drop_cols = ["id", "created_at", "prediction_time", "corridor_id", "direction", 
                 "weather_condition", "status", "severity_status", "time_slot", "day_of_week", 
                 "reason", "pcu_source", "geom",
                 "speed_kmh", "speed_ratio", "congestion_percent", "tti", "travel_time_sec",
                 "anomaly_score", "emission_congestion_cross", "is_anomaly", "osrm_divergence", "pcu_index"]
    feature_cols = [c for c in df.columns if c not in drop_cols and c != target_col]
    
    # Also explicitly drop any remaining object/string columns just to be safe
    feature_cols = [c for c in feature_cols if df[c].dtype not in ['object', 'string']]
    
    if exclude_weather:
        weather_cols = ["temperature", "humidity", "wind_speed", "visibility_km", "pm2_5", "pm10", "co_level", "no2_level", "aqi"]
        feature_cols = [c for c in feature_cols if c not in weather_cols]
        
    # ── 2. Train/Test Split (Chronological 80/20) ──
    n = len(df)
    train_end = int(n * 0.8)
    
    train_df = df.iloc[:train_end].copy()
    test_df = df.iloc[train_end:].copy()
    
    X_train = train_df[feature_cols].copy()
    y_train = train_df[target_col].values
    X_test = test_df[feature_cols].copy()
    y_test = test_df[target_col].values
    
    # Imputation
    medians = X_train.median(numeric_only=True)
    X_train = X_train.fillna(medians)
    X_test = X_test.fillna(medians)
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Validation split for early stopping
    val_size = int(len(X_train_scaled) * 0.2)
    X_t_sub = X_train_scaled[:-val_size]
    y_t_sub = y_train[:-val_size]
    X_v_sub = X_train_scaled[-val_size:]
    y_v_sub = y_train[-val_size:]
    
    # Datasets
    train_dataset = TimeWindowDataset(X_t_sub, y_t_sub, SEQ_LEN, PRED_LEN)
    val_dataset = TimeWindowDataset(X_v_sub, y_v_sub, SEQ_LEN, PRED_LEN)
    test_dataset = TimeWindowDataset(X_test_scaled, y_test, SEQ_LEN, PRED_LEN)
    
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False)
    
    # Model Setup
    model = TCN_TFT_Hybrid(
        num_features=len(feature_cols),
        hidden_size=HIDDEN_SIZE,
        num_heads=NUM_HEADS,
        pred_len=PRED_LEN,
        num_quantiles=len(QUANTILES),
        use_tcn=use_tcn,
        use_attention=use_attention
    ).to(device)
    
    criterion = QuantileLoss(QUANTILES)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    
    # Training Loop
    best_val_loss = float('inf')
    patience_counter = 0
    
    for epoch in range(MAX_EPOCHS):
        model.train()
        for bx, by in train_loader:
            bx, by = bx.to(device), by.to(device)
            optimizer.zero_grad()
            pred, _ = model(bx)
            loss = criterion(pred, by)
            loss.backward()
            optimizer.step()
            
        model.eval()
        val_loss = 0
        with torch.no_grad():
            for bx, by in val_loader:
                bx, by = bx.to(device), by.to(device)
                pred, _ = model(bx)
                val_loss += criterion(pred, by).item()
        
        val_loss /= len(val_loader)
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), f"temp_{name}.pth")
        else:
            patience_counter += 1
            if patience_counter >= EARLY_STOP_PATIENCE:
                break
                
    # Evaluation
    model.load_state_dict(torch.load(f"temp_{name}.pth", weights_only=True))
    model.eval()
    
    fold_preds, fold_actuals = [], []
    with torch.no_grad():
        for bx, by in test_loader:
            bx, by = bx.to(device), by.to(device)
            pred, _ = model(bx)
            fold_preds.extend(pred.cpu().numpy())
            fold_actuals.extend(by.cpu().numpy())
            
    y_pred = np.array(fold_preds)
    y_true = np.array(fold_actuals)
    
    y_pred_p10 = y_pred[:, :, 0]
    y_pred_p50 = y_pred[:, :, 1]
    y_pred_p90 = y_pred[:, :, 2]
    
    mae = float(np.mean(np.abs(y_true - y_pred_p50)))
    picp = float(np.mean((y_true >= y_pred_p10) & (y_true <= y_pred_p90)))
    
    logging.info(f"Result [{name}]: MAE = {mae:.4f}, PICP = {picp:.4f}")
    return {"Experiment": name, "MAE": mae, "PICP": picp}

def run_ablation():
    df = load_and_preprocess_data(days_lookback=30)
    df = df.sort_values("created_at").reset_index(drop=True)
    df = df[df["direction"] == "North (Mirpur-11 to 10)"]
    
    if len(df) < 500:
        logging.error("Insufficient data.")
        return
        
    results = []
    
    # 1. Full Model
    results.append(run_experiment("1_Full_Model", df, "actual_eta_min", 
                                  use_tcn=True, use_attention=True, exclude_weather=False))
    
    # 2. No Weather
    results.append(run_experiment("2_No_Weather", df, "actual_eta_min", 
                                  use_tcn=True, use_attention=True, exclude_weather=True))
    
    # 3. No Attention
    results.append(run_experiment("3_No_Attention", df, "actual_eta_min", 
                                  use_tcn=True, use_attention=False, exclude_weather=False))
    
    # 4. No TCN
    results.append(run_experiment("4_No_TCN", df, "actual_eta_min", 
                                  use_tcn=False, use_attention=True, exclude_weather=False))
    
    # Print Table 4
    res_df = pd.DataFrame(results)
    print("\n" + "="*60)
    print("TABLE 4: ABLATION STUDY RESULTS (NORTH CORRIDOR: MIRPUR-11 TO 10)")
    print("="*60)
    print(res_df.to_string(index=False))
    print("="*60)

if __name__ == "__main__":
    run_ablation()
