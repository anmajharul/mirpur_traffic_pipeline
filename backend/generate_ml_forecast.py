"""
generate_ml_forecast.py — Q1 DEFENSIBLE 24H ML FORECAST GENERATOR
===================================================================
Purpose:
- Fetch 48-hour future weather covariates (Open-Meteo).
- Load trained XGBoost model and generate 24-h predictions.
- Upsert predictions to Supabase `ml_forecasts` table.
"""

import os
import json
import logging
import requests
import pandas as pd
import numpy as np
import xgboost as xgb
from datetime import datetime, timezone, timedelta
from supabase import create_client
from dotenv import load_dotenv
from config import CORRIDORS, MAPBOX_TOKEN
from data_collector import get_mapbox_data

# Suppress SettingWithCopyWarning
pd.options.mode.chained_assignment = None

# We must mock or import the exact FEATURE_COLS list
try:
    from trainer_xgb import FEATURE_COLS
except ImportError:
    logging.error("Could not import FEATURE_COLS from trainer_xgb.py")
    exit(1)

try:
    from trainer_mlp import load_mlp_artifact, MLP_ARTIFACT_NAME
except ImportError:
    logging.warning("Could not import MLP functions. MLP forecasts will be skipped.")
    load_mlp_artifact = None

try:
    from trainer_tcn_tft import load_tcn_artifact, TCN_ARTIFACT_NAME
except ImportError:
    logging.warning("Could not import TCN functions. TCN forecasts will be skipped.")
    load_tcn_artifact = None

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
BDT = timezone(timedelta(hours=6))

# Mirpur 10 Approximate Coordinates
LAT, LON = 23.807, 90.368

def fetch_weather_forecast():
    """Fetch 48h weather and AQI forecast from Open-Meteo."""
    logging.info("Fetching Open-Meteo 48h weather forecast...")
    weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&hourly=temperature_2m,precipitation,visibility,wind_speed_10m,relative_humidity_2m&forecast_days=2&timezone=auto"
    aqi_url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={LAT}&longitude={LON}&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,us_aqi&forecast_days=2&timezone=auto"
    
    w_data = requests.get(weather_url).json()
    a_data = requests.get(aqi_url).json()
    
    df_w = pd.DataFrame(w_data['hourly'])
    df_a = pd.DataFrame(a_data['hourly'])
    
    # Merge on time
    df = pd.merge(df_w, df_a, on='time')
    df['time'] = pd.to_datetime(df['time']).dt.tz_localize(BDT)
    
    # Rename to match XGBoost training features
    df = df.rename(columns={
        "temperature_2m": "temperature",
        "precipitation": "rain_mm",
        "visibility": "visibility_km",
        "wind_speed_10m": "wind_speed",
        "relative_humidity_2m": "humidity",
        "carbon_monoxide": "co_level",
        "nitrogen_dioxide": "no2_level",
        "us_aqi": "aqi"
    })
    
    # Visibility API gives meters, convert to km
    if "visibility_km" in df.columns and df["visibility_km"].max() > 100:
        df["visibility_km"] = df["visibility_km"] / 1000.0
        
    return df

def generate_forecasts():
    model_path = os.path.join(os.path.dirname(__file__), "model_ml_weight.json")
    if not os.path.exists(model_path):
        logging.error(f"XGBoost model not found at {model_path}")
        return

    model = xgb.XGBRegressor()
    model.load_model(model_path)
    logging.info("Loaded XGBoost model.")

    mlp_model = None
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
            logging.warning(f"TCN model not found at {tcn_path}")

    df_weather = fetch_weather_forecast()
    
    # Get current time and find the start of 'today'
    now = datetime.now(BDT)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = today_start + timedelta(days=2)
    
    # Filter weather to today and tomorrow (48 hours)
    mask = (df_weather['time'] >= today_start) & (df_weather['time'] < end_time)
    df_forecast = df_weather[mask].copy().reset_index(drop=True)
    
    if df_forecast.empty:
        logging.error("No weather forecast data for the next 48 hours.")
        return
        
    records_to_insert = []
    
    # Generate predictions per direction
    for direction, coords in CORRIDORS.items():
        origin = coords["origin"]
        dest = coords["dest"]
        
        # Measure accurate distance using Mapbox API
        mapbox_info = get_mapbox_data(origin, dest, MAPBOX_TOKEN)
        if mapbox_info and mapbox_info.get("distance_km"):
            distance = mapbox_info["distance_km"]
            logging.info(f"Mapbox measured distance for {direction}: {distance} km")
        else:
            logging.warning(f"Failed to fetch Mapbox distance for {direction}. Skipping.")
            continue
            
        num_hours = len(df_forecast)
        # Initialize feature dataframe
        df_feats = pd.DataFrame(index=range(num_hours))
        
        # 1. Temporal Features
        df_feats['hour_of_day'] = df_forecast['time'].dt.hour
        df_feats['minute'] = 0
        df_feats['day_of_week_num'] = df_forecast['time'].dt.dayofweek
        df_feats['is_weekend'] = df_feats['day_of_week_num'].isin([4, 5]).astype(int)
        
        time_frac = (df_feats['hour_of_day'] * 60) / 1440.0
        df_feats['hour_sin'] = np.sin(2 * np.pi * time_frac)
        df_feats['hour_cos'] = np.cos(2 * np.pi * time_frac)
        df_feats['is_peak_hour'] = ((df_feats['hour_of_day'].between(7, 10)) | (df_feats['hour_of_day'].between(16, 20))).astype(int)
        df_feats['month'] = df_forecast['time'].dt.month
        df_feats['is_monsoon'] = df_feats['month'].between(6, 9).astype(int)
        
        # 2. Weather Features
        df_feats['temperature'] = df_forecast['temperature']
        df_feats['rain_mm'] = df_forecast['rain_mm']
        df_feats['humidity'] = df_forecast['humidity']
        df_feats['wind_speed'] = df_forecast['wind_speed']
        df_feats['visibility_km'] = df_forecast['visibility_km']
        df_feats['pm2_5'] = df_forecast['pm2_5']
        df_feats['pm10'] = df_forecast['pm10']
        df_feats['co_level'] = df_forecast['co_level']
        df_feats['no2_level'] = df_forecast['no2_level']
        df_feats['aqi'] = df_forecast['aqi']
        
        # Derived Weather
        df_feats['rain_lag_1'] = df_feats['rain_mm'].shift(1).fillna(0)
        df_feats['rain_lag_2'] = df_feats['rain_mm'].shift(2).fillna(0)
        df_feats['rain_accumulation_3h'] = df_feats['rain_mm'].rolling(3, min_periods=1).sum()
        df_feats['wmo_rain_category'] = pd.cut(df_feats['rain_mm'], bins=[-1, 0.1, 2.5, 10, 50, 999], labels=[0, 1, 2, 3, 4]).astype(int)
        df_feats['visibility_penalty'] = (10 - df_feats['visibility_km']).clip(lower=0)
        df_feats['weather_condition_encoded'] = (df_feats['rain_mm'] > 0).astype(int)
        df_feats['rain_x_peak_hour'] = df_feats['rain_mm'] * df_feats['is_peak_hour']
        df_feats['is_extreme_weather'] = (df_feats['rain_mm'] > 10.0).astype(int)
        
        # 3. Context & Imputed Features (Median/Static fallback)
        df_feats['distance_km'] = distance
        df_feats['mrt_status'] = 0 # Normal
        df_feats['mrt_headway'] = 6.0
        df_feats['is_holiday'] = df_feats['is_weekend']
        
        # Lag imputation (Injecting diurnal variation so the forecast isn't a flat line)
        def get_speed_proxy(hour):
            if 7 <= hour <= 10 or 16 <= hour <= 20:
                return 10.0  # Peak hour congestion
            elif 22 <= hour <= 23 or 0 <= hour <= 6:
                return 25.0  # Night free-flow
            else:
                return 15.0  # Off-peak day
                
        speed_proxies = df_feats['hour_of_day'].apply(get_speed_proxy)
        eta_proxies = (distance / speed_proxies) * 60.0
        
        df_feats['actual_eta_min_lag1'] = eta_proxies
        df_feats['actual_eta_min_lag2'] = eta_proxies
        df_feats['actual_eta_min_lag3'] = eta_proxies
        df_feats['emission_congestion_cross_lag1'] = 0.5
        df_feats['is_anomaly_lag1'] = 0
        df_feats['osrm_divergence_lag1'] = 1.2
        
        # Ensure exact columns
        missing_cols = set(FEATURE_COLS) - set(df_feats.columns)
        for c in missing_cols:
            df_feats[c] = 0.0
            
        X = df_feats[FEATURE_COLS]
        
        # Predict actual_eta_min
        preds_eta = model.predict(X)
        mlp_preds_eta = mlp_model.predict(X) if mlp_model is not None else None
        if mlp_preds_eta is not None:
            mlp_preds_eta = mlp_preds_eta.flatten()
            
        tcn_preds_eta = tcn_model.predict(X) if tcn_model is not None else None
        if tcn_preds_eta is not None:
            tcn_preds_eta = tcn_preds_eta.flatten()
        
        for i in range(num_hours):
            # XGBoost logic
            eta = max(1.0, preds_eta[i])
            speed = (distance / (eta / 60.0))
            speed = max(3.0, min(80.0, speed)) # clamp
            congestion = max(0.0, min(100.0, (1 - speed/40.0)*100))
            
            record = {
                "target_time_utc": df_forecast['time'].iloc[i].astimezone(timezone.utc).isoformat(),
                "target_hour": int(df_feats['hour_of_day'].iloc[i]),
                "direction": direction,
                "predicted_speed_kmh": round(float(speed), 1),
                "predicted_congestion_percent": round(float(congestion), 1)
            }
            
            # MLP logic
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
                record["tcn_predicted_congestion_percent"] = round(float(t_cng), 1)
            
            records_to_insert.append(record)

    # Upsert to Supabase
    logging.info(f"Upserting {len(records_to_insert)} ML forecast records to Supabase...")
    try:
        supabase.table("ml_forecasts").upsert(records_to_insert, on_conflict="target_time_utc,direction").execute()
        logging.info("ML Forecast generated and saved successfully.")
    except Exception as e:
        logging.error(f"Failed to upsert ML forecasts: {e}")

if __name__ == "__main__":
    generate_forecasts()
