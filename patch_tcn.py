import os

tcn_path = r'e:\Mirpur 10 research\backend codes of mirpur 10\backend\trainer_tcn_tft.py'
gen_path = r'e:\Mirpur 10 research\backend codes of mirpur 10\backend\generate_ml_forecast.py'

with open(tcn_path, 'r', encoding='utf-8') as f:
    tcn_content = f.read()

tcn_addition = """
from pathlib import Path

TCN_ARTIFACT_NAME = "model_tcn_weight.pt"

class TCNWrapper:
    def __init__(self, n_features, best_params=None):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.n_features = n_features
        self.best_params = best_params or {
            "hidden_size": HIDDEN_SIZE,
            "num_heads": NUM_HEADS,
            "pred_len": PRED_LEN,
            "num_quantiles": len(QUANTILES)
        }
        self.model = TCN_TFT_Hybrid(
            num_features=n_features,
            hidden_size=self.best_params["hidden_size"],
            num_heads=self.best_params["num_heads"],
            pred_len=self.best_params["pred_len"],
            num_quantiles=self.best_params["num_quantiles"]
        ).to(self.device)
        self.scaler = StandardScaler()

    def predict(self, X):
        X_filled = X.fillna(X.median())
        X_scaled = self.scaler.transform(X_filled.values.astype(np.float32))
        X_seq = np.repeat(X_scaled[:, np.newaxis, :], SEQ_LEN, axis=1)
        self.model.eval()
        with torch.no_grad():
            preds, _ = self.model(torch.tensor(X_seq, dtype=torch.float32).to(self.device))
            eta_pred = preds[:, 0, 1].cpu().numpy()
        return eta_pred

def load_tcn_artifact(artifact_path):
    artifact_path = Path(artifact_path)
    if not artifact_path.exists():
        raise RuntimeError(f"TCN artifact not found at {artifact_path}")
    bundle = torch.load(str(artifact_path), map_location="cpu", weights_only=False)
    wrapper = TCNWrapper(bundle["n_features"])
    wrapper.model.load_state_dict(bundle["model_state_dict"])
    wrapper.scaler.mean_ = bundle["scaler_mean"]
    wrapper.scaler.scale_ = bundle["scaler_scale"]
    return wrapper

def save_tcn_artifact(model_state, scaler, n_features, output_path=TCN_ARTIFACT_NAME):
    torch.save({
        "model_state_dict": model_state,
        "scaler_mean": scaler.mean_,
        "scaler_scale": scaler.scale_,
        "n_features": n_features
    }, output_path)
"""

if "TCN_ARTIFACT_NAME" not in tcn_content:
    target_loop = "        global_vsn_weights.extend(fold_weights)"
    replacement_loop = """        global_vsn_weights.extend(fold_weights)
        
        if k == n_folds:
            try:
                save_tcn_artifact(model.state_dict(), scaler, len(feature_cols))
                logging.info(f"Saved TCN artifact to {TCN_ARTIFACT_NAME}")
            except Exception as e:
                logging.warning(f"Could not save TCN artifact: {e}")"""
    
    tcn_content = tcn_content.replace(target_loop, replacement_loop)
    
    target_end = "if __name__ == \"__main__\":"
    tcn_content = tcn_content.replace(target_end, tcn_addition + "\n\n" + target_end)
    
    with open(tcn_path, 'w', encoding='utf-8') as f:
        f.write(tcn_content)
    print("Updated trainer_tcn_tft.py")

with open(gen_path, 'r', encoding='utf-8') as f:
    gen_content = f.read()

gen_imports = """try:
    from trainer_mlp import load_mlp_artifact, MLP_ARTIFACT_NAME
except ImportError:
    logging.warning("Could not import MLP functions. MLP forecasts will be skipped.")
    load_mlp_artifact = None"""

new_gen_imports = """try:
    from trainer_mlp import load_mlp_artifact, MLP_ARTIFACT_NAME
except ImportError:
    logging.warning("Could not import MLP functions. MLP forecasts will be skipped.")
    load_mlp_artifact = None

try:
    from trainer_tcn_tft import load_tcn_artifact, TCN_ARTIFACT_NAME
except ImportError:
    logging.warning("Could not import TCN functions. TCN forecasts will be skipped.")
    load_tcn_artifact = None"""

gen_content = gen_content.replace(gen_imports, new_gen_imports)

gen_model_load = """    mlp_model = None
    if load_mlp_artifact is not None:
        mlp_path = os.path.join(os.path.dirname(__file__), MLP_ARTIFACT_NAME)
        if os.path.exists(mlp_path):
            try:
                mlp_model = load_mlp_artifact(mlp_path)
                logging.info("Loaded MLP baseline model.")
            except Exception as e:
                logging.error(f"Failed to load MLP model: {e}")
        else:
            logging.warning(f"MLP model not found at {mlp_path}")"""

new_gen_model_load = """    mlp_model = None
    if load_mlp_artifact is not None:
        mlp_path = os.path.join(os.path.dirname(__file__), MLP_ARTIFACT_NAME)
        if os.path.exists(mlp_path):
            try:
                mlp_model = load_mlp_artifact(mlp_path)
                logging.info("Loaded MLP baseline model.")
            except Exception as e:
                logging.error(f"Failed to load MLP model: {e}")
        else:
            logging.warning(f"MLP model not found at {mlp_path}")

    tcn_model = None
    if load_tcn_artifact is not None:
        tcn_path = os.path.join(os.path.dirname(__file__), TCN_ARTIFACT_NAME)
        if os.path.exists(tcn_path):
            try:
                tcn_model = load_tcn_artifact(tcn_path)
                logging.info("Loaded TCN-TFT model.")
            except Exception as e:
                logging.error(f"Failed to load TCN model: {e}")
        else:
            logging.warning(f"TCN model not found at {tcn_path}")"""

gen_content = gen_content.replace(gen_model_load, new_gen_model_load)

gen_preds = """        # Predict actual_eta_min
        preds_eta = model.predict(X)
        mlp_preds_eta = mlp_model.predict(X) if mlp_model is not None else None
        if mlp_preds_eta is not None:
            mlp_preds_eta = mlp_preds_eta.flatten()"""

new_gen_preds = """        # Predict actual_eta_min
        preds_eta = model.predict(X)
        mlp_preds_eta = mlp_model.predict(X) if mlp_model is not None else None
        if mlp_preds_eta is not None:
            mlp_preds_eta = mlp_preds_eta.flatten()
            
        tcn_preds_eta = tcn_model.predict(X) if tcn_model is not None else None
        if tcn_preds_eta is not None:
            tcn_preds_eta = tcn_preds_eta.flatten()"""

gen_content = gen_content.replace(gen_preds, new_gen_preds)

gen_logic = """            # MLP logic
            if mlp_preds_eta is not None:
                m_eta = max(1.0, mlp_preds_eta[i])
                m_speed = max(3.0, min(80.0, (distance / (m_eta / 60.0))))
                m_cng = max(0.0, min(100.0, (1 - m_speed/40.0)*100))
                record["mlp_predicted_speed_kmh"] = round(float(m_speed), 1)
                record["mlp_predicted_congestion_percent"] = round(float(m_cng), 1)"""

new_gen_logic = """            # MLP logic
            if mlp_preds_eta is not None:
                m_eta = max(1.0, mlp_preds_eta[i])
                m_speed = max(3.0, min(80.0, (distance / (m_eta / 60.0))))
                m_cng = max(0.0, min(100.0, (1 - m_speed/40.0)*100))
                record["mlp_predicted_speed_kmh"] = round(float(m_speed), 1)
                record["mlp_predicted_congestion_percent"] = round(float(m_cng), 1)
                
            # TCN logic
            if tcn_preds_eta is not None:
                t_eta = max(1.0, tcn_preds_eta[i])
                t_speed = max(3.0, min(80.0, (distance / (t_eta / 60.0))))
                t_cng = max(0.0, min(100.0, (1 - t_speed/40.0)*100))
                record["tcn_predicted_speed_kmh"] = round(float(t_speed), 1)
                record["tcn_predicted_congestion_percent"] = round(float(t_cng), 1)"""

gen_content = gen_content.replace(gen_logic, new_gen_logic)

# In the Supabase upsert payload, the on_conflict is target_time_utc,direction. Supabase will update the inserted columns. 
# Also need to make sure the upsert statement includes columns if there's any logic preventing it. But Supabase python client inserts exactly the keys of the dict. So adding the keys to the dict is sufficient.

with open(gen_path, 'w', encoding='utf-8') as f:
    f.write(gen_content)
print("Updated generate_ml_forecast.py")
