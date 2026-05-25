import os
path = r'e:\Mirpur 10 research\backend codes of mirpur 10\backend\trainer_tcn_tft.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add _assert_no_leakage import
content = content.replace("from evaluation import smape", "from evaluation import smape\nfrom trainer_xgb import _assert_no_leakage, FEATURE_COLS\nfrom torch.utils.data import ConcatDataset")

# 2. Remove single corridor filter and change 500 to 2000
filter_code = """    # Filter specific direction for corridor-level analysis
    # Selected a highly congested primary arterial corridor for the ablation study
    df = df[df["direction"] == "North (Mirpur-11 to 10)"]
    
    # ── DATA SUFFICIENCY GUARD (per corridor direction) ──────────────────
    # Hard floor: 500 rows per corridor direction."""

new_filter_code = """    # No single direction filter; evaluate on all corridors for consistent methodology
    
    # ── LEAKAGE GUARD ────────────────────────────────────────────────────────
    _assert_no_leakage(feature_cols)

    # ── DATA SUFFICIENCY GUARD ───────────────────────────────────────────────
    # Hard floor: 2000 rows (all corridors)."""

content = content.replace(filter_code, new_filter_code)

content = content.replace("if len(df) < 500:", "if len(df) < 2000:")
content = content.replace("minimum is 500.", "minimum is 2000.")

# 3. Modify CV Loop Data Prep
old_cv_data_prep = """        X_train = train_df[feature_cols].copy()
        y_train = train_df[target_col].values
        X_test = test_df[feature_cols].copy()
        y_test = test_df[target_col].values

        # LOCF + Median Imputation
        ffill_cols = ["temperature", "humidity", "wind_speed", "visibility_km", "pm2_5", "pm10", "co_level", "no2_level", "aqi"]
        ffill_cols = [c for c in ffill_cols if c in X_train.columns]
        if ffill_cols:
            X_train[ffill_cols] = X_train[ffill_cols].ffill()
            last_row = X_train[ffill_cols].iloc[-1:]
            X_test[ffill_cols] = pd.concat([last_row, X_test[ffill_cols]]).ffill().iloc[1:]

        medians = X_train.median(numeric_only=True)
        X_train = X_train.fillna(medians)
        X_test = X_test.fillna(medians)

        # Standard Scaling (Fit on Train only)
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Validation split (last 20% of train) for early stopping
        val_size = int(len(X_train_scaled) * 0.2)
        X_t_sub = X_train_scaled[:-val_size]
        y_t_sub = y_train[:-val_size]
        X_v_sub = X_train_scaled[-val_size:]
        y_v_sub = y_train[-val_size:]

        # Create Datasets
        train_dataset = TimeWindowDataset(X_t_sub, y_t_sub, SEQ_LEN, PRED_LEN)
        val_dataset = TimeWindowDataset(X_v_sub, y_v_sub, SEQ_LEN, PRED_LEN)
        test_dataset = TimeWindowDataset(X_test_scaled, y_test, SEQ_LEN, PRED_LEN)"""

new_cv_data_prep = """        # LOCF + Median Imputation with CAUSAL ISOLATION (Grouped by direction)
        ffill_cols = ["temperature", "humidity", "wind_speed", "visibility_km", "pm2_5", "pm10", "co_level", "no2_level", "aqi"]
        ffill_cols = [c for c in ffill_cols if c in feature_cols]
        if ffill_cols:
            train_df[ffill_cols] = train_df.groupby("direction")[ffill_cols].ffill()
            # To strictly ffill test, we combine train and test per direction
            test_df_copy = test_df.copy()
            for d in df["direction"].unique():
                d_train = train_df[train_df["direction"] == d]
                if d_train.empty: continue
                last_row = d_train[ffill_cols].iloc[-1:]
                d_test = test_df[test_df["direction"] == d]
                if d_test.empty: continue
                filled_test = pd.concat([last_row, d_test[ffill_cols]]).ffill().iloc[1:]
                test_df_copy.loc[test_df["direction"] == d, ffill_cols] = filled_test
            test_df = test_df_copy

        medians = train_df[feature_cols].median(numeric_only=True)
        train_df[feature_cols] = train_df[feature_cols].fillna(medians)
        test_df[feature_cols] = test_df[feature_cols].fillna(medians)

        # Standard Scaling (Fit on Train only)
        scaler = StandardScaler()
        train_df[feature_cols] = scaler.fit_transform(train_df[feature_cols])
        test_df[feature_cols] = scaler.transform(test_df[feature_cols])
        
        # Validation split by index (last 20% of train)
        val_size = int(len(train_df) * 0.2)
        train_sub = train_df.iloc[:-val_size]
        val_sub = train_df.iloc[-val_size:]

        # Create Datasets Isolated by Corridor
        def build_dataset(data_df):
            datasets = []
            for d in df["direction"].unique():
                dir_df = data_df[data_df["direction"] == d]
                if len(dir_df) >= SEQ_LEN + PRED_LEN:
                    X_arr = dir_df[feature_cols].values
                    y_arr = dir_df[target_col].values
                    datasets.append(TimeWindowDataset(X_arr, y_arr, SEQ_LEN, PRED_LEN))
            return ConcatDataset(datasets) if datasets else None

        train_dataset = build_dataset(train_sub)
        val_dataset = build_dataset(val_sub)
        test_dataset = build_dataset(test_df)
        
        if train_dataset is None or val_dataset is None or test_dataset is None:
            logging.error(f"[TCN-TFT] Fold {k}: insufficient data for windows. Skipping.")
            continue"""

content = content.replace(old_cv_data_prep, new_cv_data_prep)

# Write back
with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated trainer_tcn_tft.py successfully")
