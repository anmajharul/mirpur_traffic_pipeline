# Physics-Informed Hybrid Machine Learning for Traffic Forecasting in Heterogeneous Unstructured Networks Under Meteorological Disruptions

[![Status](https://img.shields.io/badge/Status-Q1_Publication_Ready-success?style=for-the-badge)](#)
[![Methodology](https://img.shields.io/badge/Methodology-XGBoost_%7C_TCN--TFT-blue?style=for-the-badge)](#)
[![Validation](https://img.shields.io/badge/Validation-Walk--Forward_CV-orange?style=for-the-badge)](#)

This repository contains the official codebase and data pipeline for the research paper: **"Physics-Informed Hybrid Machine Learning for Traffic Forecasting in Heterogeneous Unstructured Networks Under Meteorological Disruptions"**. 

The system provides an end-to-end, scientifically defensible traffic forecasting pipeline tailored for **Mirpur-10**, Dhaka—a highly heterogeneous, non-lane-based traffic environment. It aggregates real-time probe data, meteorological variables, and public transit schedules to predict short-term travel times using a robust Gradient Boosting setup (XGBoost) alongside Deep Learning baselines (TCN-TFT and PyTorch MLP).

The codebase has been engineered to meet the stringent reproducible and theoretical demands of **Q1 academic transportation journals** (e.g., *Transportation Research Part C*, *IEEE Transactions on Intelligent Transportation Systems*).

---

## 🔬 Scientific Highlights & Methodological Rigor

This framework rectifies several common methodological flaws found in applied traffic forecasting literature:

1. **Strict Sensor Independence (El Faouzi et al. 2011)** 
   We explicitly avoid multi-routing-engine fusion (e.g., Mapbox + OSRM) to prevent violating sensor independence assumptions required for Kalman or inverse-variance weighting algorithms. Instead, we compute divergence against an **OSRM static routing baseline**.
2. **Temporal Z-Score Anomaly Detection (Ahmed & Cook 1979)** 
   Spatially computed anomaly thresholds have been replaced with a rolling temporal baseline. An event is flagged as an anomaly only if current $v_t$ deviates by $> 2\sigma$ from recent history.
3. **Dynamic PCU Scaling (Chandra & Sikdar 2000)** 
   HCM 7th Ed. capacity multipliers are explicitly designed for lane-based traffic and fail in highly unstructured flows. We employ a dynamic Congestion Intensity (CI) scaled Passenger Car Unit proxy.
4. **Leakage-Safe Walk-Forward CV (Bergmeir & Benítez 2012)**
   Traditional $K$-fold cross-validation suffers from temporal data leakage. We use a 5-fold temporal walk-forward split and strictly apply median imputations and baselines derived **only** from the training partition.
5. **Robust Deep Learning Baselines (Lim et al. 2021)**
   In addition to XGBoost, the repository implements a Temporal Fusion Transformer (TCN-TFT) hybrid with Variable Selection Networks (VSN) to extract native explainable AI (XAI) feature weights without post-hoc SHAP dependencies.

---

## 📐 System Architecture

### 1. Multi-Source Ingestion Pipeline
- **Real-Time Data**: Mapbox Directions API (Driving-Traffic Profile).
- **Static Baseline**: Project-OSRM Public API.
- **Meteorology**: WeatherAPI (including EPA NowCast AQI).
- **Transit Schedule**: Local MRT operational status & headway tracking.

### 2. Hybrid MLOps & Data Persistence
- **Google Cloud Run**: Serverless container execution for data ingestion cron-jobs (`pipeline.py`).
- **Supabase PostgreSQL**: Database for `smart_eta_logs` (PostGIS geometry) and `model_metrics`.
- **GitHub Actions**: Offloads resource-heavy XGBoost and deep learning cross-validation to CI/CD pipelines, automatically committing generating metrics and XAI weights back to the repository (`reports/*.csv`).

### 3. Machine Learning Stack
- **XGBoost**: Primary inference model with Bayesian Hyperparameter Optimization (Optuna).
- **TCN-TFT**: Deep learning ablation baseline for sequence-to-sequence forecasting.
- **MLP**: Feed-forward PyTorch baseline.
- **Weather ML**: Polynomial Ridge Regression modeling precipitation vs. speed decay bounds based on Pregnolato et al. (2017).

---

## 📂 Project Structure

```text
mirpur_traffic_ai/
│
├── backend/
│   ├── pipeline.py             # Data orchestration entrypoint (GCP Cloud Run)
│   ├── trainer_xgb.py          # Primary XGBoost Walk-forward CV
│   ├── trainer_mlp.py          # PyTorch MLP Baseline Trainer
│   ├── trainer_tcn_tft.py      # TCN-TFT Deep Learning Ablation
│   ├── train_weather_ml.py     # Weather vulnerability curve generation
│   ├── evaluation.py           # Statistical metrics (MAE, RMSE, SMAPE)
│   └── reports/                # CI/CD generated CSV metric logs
│
├── sql/
│   ├── schema_smart_eta_logs.sql # TSDB schema optimized with BTrees
│   └── schema_model_metrics.sql  # Hyperparameter and logging trace schema
│
├── .github/workflows/          # Automated model retraining & evaluation actions
├── web_app.py                  # FastAPI inference API
└── requirements.txt            # Core dependencies
```

---

## ⚙️ Local Setup & Execution

### Prerequisites
- Python 3.10+
- A Supabase PostgreSQL instance

### Installation
```bash
git clone https://github.com/anmajharul/mirpur_traffic_pipeline.git
cd mirpur_traffic_pipeline
python -m venv .venv
source .venv/bin/activate  # (or .venv\Scripts\Activate.ps1 on Windows)
pip install -r backend/requirements.txt
```

### Environment Variables (`.env`)
```ini
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_KEY="your-service-role-key"
MAPBOX_ACCESS_TOKEN="pk.your_mapbox_key"
WEATHER_API_KEY="your_weather_api_key"
```

### Reproducibility
The ML pipeline runs autonomously via GitHub Actions. To run manually:
```bash
python backend/trainer_xgb.py
```

---

## 👨‍🔬 Author & Research Affiliation

**Majharul Islam**  
*Civil Engineering, Bangladesh University of Business and Technology (BUBT)*  

**Research Focus:** Transportation Engineering | Travel Behavior Analysis | Discrete Choice Modeling | AI in ITS  

[![Portfolio](https://img.shields.io/badge/Website-anmajharul.bd-blue?style=for-the-badge&logo=googlechrome)](https://anmajharul.bd) 
<br>
© Majharul Islam – Research Portfolio
