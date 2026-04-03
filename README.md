# Mirpur-10 Traffic AI: Robust Short-Term Traffic Forecasting for Heterogeneous Arterials

[![Status](https://img.shields.io/badge/Status-Q1_Publication_Ready-success?style=for-the-badge)](#)
[![Methodology](https://img.shields.io/badge/Methodology-XGBoost_%7C_Time_Series-blue?style=for-the-badge)](#)
[![Validation](https://img.shields.io/badge/Validation-Walk--Forward_CV-orange?style=for-the-badge)](#)

This project provides an end-to-end, scientifically defensible traffic forecasting pipeline tailored for **Mirpur-10**, Dhaka—a highly heterogeneous, non-lane-based traffic environment. It aggregates real-time probe data, meteorological variables, and public transit schedules to predict short-term travel times using a robust Gradient Boosting setup (XGBoost).

The codebase has been engineered to meet the stringent reproducible and theoretical demands of **Q1 academic transportation journals** (e.g., *Transportation Research Part C*, *IEEE Transactions on Intelligent Transportation Systems*).

---

## 🔬 Scientific Highlights & Methodological Rigor

This framework rectifies several common methodological flaws found in applied traffic forecasting literature:

1. **Strict Sensor Independence (El Faouzi et al. 2011)** 
   We explicitly avoid multi-routing-engine fusion (e.g., Mapbox + Waze) to prevent violating sensor independence assumptions required for Kalman or inverse-variance weighting algorithms. Instead, we compute divergence against an **OSRM static routing baseline**.
2. **Temporal Z-Score Anomaly Detection (Ahmed & Cook 1979)** 
   Spatially computed anomaly thresholds have been replaced with a rolling temporal baseline. An event is flagged as an anomaly only if current $v_t$ deviates by $> 2\sigma$ from recent history ($\left| v_t - \mu_{history} \right| / \sigma_{history} > 2.0$).
3. **Dynamic PCU Scaling (Chandra & Sikdar 2000)** 
   HCM 7th Ed. capacity multipliers are explicitly designed for lane-based traffic and fail in Dhaka. We employ a dynamic Congestion Intensity (CI) scaled Passenger Car Unit proxy: 
   $PCU_d = \text{Density Proxy} \times \text{Fleet PCU} \times (1 + \alpha \cdot CI)$.
4. **Leakage-Safe Walk-Forward CV (Bergmeir & Benítez 2012)**
   Traditional $K$-fold cross-validation suffers from temporal data leakage. We use a 5-fold temporal walk-forward split and strictly apply median imputations and baselines derived **only** from the training partition.
5. **OSRM Static Naive Baseline (Luxen & Vetter 2011)**
   A forecasting model is only as good as the naive baseline it beats. We validate performance against **OSRM**, an open-source static routing engine that utilizes historical OpenStreetMap data with *no* real-time traffic awareness.

---

## 📐 System Architecture

### 1. Multi-Source Ingestion Pipeline
- **Real-Time Data**: Mapbox Directions API (Driving-Traffic Profile).
- **Static Baseline**: Project-OSRM Public API.
- **Meteorology**: WeatherAPI (including EPA NowCast AQI).
- **Transit Schedule**: Local MRT operational status & headway tracking.

### 2. Data Persistence (Supabase PostgreSQL)
- **`smart_eta_logs`**: Primary analytical table with PostGIS geometry support. Asserts strict type binding and OGC compliant LINESTRING topology.
- **`model_metrics`**: Reproducibility table capturing Hyperparameters, 95% Bootstrap Confidence Intervals (Efron & Tibshirani 1993), and baseline vs. model error metrics per experiment.

### 3. Machine Learning (XGBoost)
- **Feature Pipeline**: Gap-aware lags (nullified via Markov assumption if the observation gap exceeds 15 minutes), hour-of-day bounding, precipitation interaction (`rain_x_peak_hour`).
- **Target**: `actual_eta_min` directly sourced from probe trajectory.
- **Metrics**: MAE, RMSE, MAPE (with strictly positive guard rails).

---

## 📂 Project Structure

```text
mirpur_traffic_ai/
│
├── backend/
│   ├── pipeline.py             # Data orchestration entrypoint (GCP Cloud Run)
│   ├── run_collection.py       # Loop execution logic wrapper for extraction
│   ├── data_collector.py       # Core ETL, temporal logic, & OSRM integration
│   ├── data_loader.py          # PostgreSQL fetcher & hard-guard leakage filter
│   ├── data_validator.py       # Range and monotonicity checks (avoiding tech debt)
│   ├── fusion.py               # Z-Score anomaly calculations & PCU indexing
│   ├── trainer_xgb.py          # Time-series walk-forward CV and XGB fit
│   ├── evaluation.py           # OSRM vs XGBoost statistical assessment & CIs
│   └── weather.py / mrt.py     # External API proxies
│
├── sql/
│   ├── schema_smart_eta_logs.sql # TSDB schema optimized with BTrees
│   └── schema_model_metrics.sql  # Hyperparameter and logging trace schema
│
├── web_app.py                  # FastAPI inference/dashboard endpoints
├── Dockerfile.collector        # Isolated GCP image definitions
└── requirements.txt            # Dep. lockfile
```

---

## ⚙️ Local Setup & Execution

### Prerequisites
- Python 3.10+
- A Supabase PostgreSQL instance
- API Keys: Mapbox, WeatherAPI

### Installation

```powershell
# 1. Clone the repository and initialize virtual environment
cd e:\mirpur_traffic_ai
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Install Dependencies
pip install -r requirements.txt
pip install -r requirements.collector.txt
```

### Environment Variables
Create a `.env` file containing:
```ini
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_KEY="your-service-role-key"
MAPBOX_ACCESS_TOKEN="pk.your_mapbox_key"
WEATHER_API_KEY="your_weather_api_key"
```

### Running the System
```powershell
# Step 1: Run collection (Fetch probe data & environmental metadata)
python backend/run_collection.py

# Step 2: Train the XGBoost model and execute Walk-Forward CV
python backend/trainer_xgb.py

# Step 3: Serve the API
uvicorn web_app:app --host 0.0.0.0 --port 8000
```

---

## 📝 Performance Table 3 format (Sample Output)

Automatically generated through `evaluation.py` and synced to `model_metrics`:

| Method               | MAE (min) | RMSE (min) | MAPE (%) |
|----------------------|-----------|------------|----------|
| Hist. Avg (baseline) | 4.2       | 5.8        | 18.0%    |
| OSRM (static)        | 3.9       | 5.1        | 15.5%    |
| **XGBoost (ours)**   | **1.8**   | **2.4**    | **8.2%** |

*(Example metrics intended for illustration)*

---

# 👨‍🔬 Author & Research Affiliation

**Majharul Islam**  
*Civil Engineering, Bangladesh University of Business and Technology (BUBT)*  

**Research Focus:** Transportation Engineering | Travel Behavior Analysis | Discrete Choice Modeling | AI in ITS  

[![Portfolio](https://img.shields.io/badge/Website-anmajharul.bd-blue?style=for-the-badge&logo=googlechrome)](https://anmajharul.bd) 
<br>
© Majharul Islam – Research Portfolio
