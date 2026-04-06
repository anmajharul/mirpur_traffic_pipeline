import os
import numpy as np
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from datetime import datetime, timezone

# Resolve .env relative to this file (works from any working directory)
_BASE = Path(__file__).resolve().parent.parent   # → backend codes of mirpur 10/
load_dotenv(_BASE / '.env', override=True)
load_dotenv(_BASE / '.env.local', override=False)  # optional override

# Backend .env uses SUPABASE_URL / SUPABASE_KEY (service role)
# Fallback to VITE_ prefix for compatibility when run from frontend folder
SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("VITE_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("VITE_SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials. Check backend/.env (SUPABASE_URL, SUPABASE_KEY).")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def train_weather_ml():
    print("Fetching historical weather/traffic data from Supabase...")
    
    # Needs a large limit to capture meaningful historic patterns
    response = supabase.table("smart_eta_logs") \
        .select("speed_kmh, congestion_percent, rain_mm, aqi, data_confidence") \
        .not_is("speed_kmh", "null") \
        .gte("data_confidence", 0.6) \
        .order("created_at", desc=True) \
        .limit(10000) \
        .execute()
        
    data = response.data
    if not data or len(data) < 50:
        print("Not enough data to train confident ML curves. Ensure database has populated logs.")
        return
        
    df = pd.DataFrame(data)
    
    # We enforce numeric processing securely
    df['rain_mm'] = pd.to_numeric(df['rain_mm'], errors='coerce').fillna(0.0)
    df['speed_kmh'] = pd.to_numeric(df['speed_kmh'], errors='coerce')
    df['congestion_percent'] = pd.to_numeric(df['congestion_percent'], errors='coerce')
    df['aqi'] = pd.to_numeric(df['aqi'], errors='coerce')
    
    # Drop completely broken rows
    df = df.dropna(subset=['speed_kmh'])

    print(f"Training on {len(df)} empirical records...")
    
    # -- 1. Learn Speed vs Rain --
    # Polynomial Regression allows curve fitting (e.g. slight rain = minor drop, heavy rain = drastic drop)
    X = df[['rain_mm']].values
    y_speed = df['speed_kmh'].values
    
    # Fit Speed Model (Degree 2 is optimal to avoid over-fitting on sparse heavy rain data)
    model_speed = make_pipeline(PolynomialFeatures(2), Ridge(alpha=1.0))
    model_speed.fit(X, y_speed)
    
    # -- 2. Learn Congestion vs Rain --
    valid_cong = df.dropna(subset=['congestion_percent'])
    if not valid_cong.empty:
        model_cong = make_pipeline(PolynomialFeatures(2), Ridge(alpha=1.0))
        model_cong.fit(valid_cong[['rain_mm']].values, valid_cong['congestion_percent'].values)
    else:
        model_cong = None
        
    # -- 3. Learn AQI vs Rain (Wet Scavenging effect) --
    valid_aqi = df.dropna(subset=['aqi'])
    if not valid_aqi.empty:
        model_aqi = make_pipeline(PolynomialFeatures(1), Ridge(alpha=1.0)) # Linear decay is safer for AQI/Rain limit
        model_aqi.fit(valid_aqi[['rain_mm']].values, valid_aqi['aqi'].values)
    else:
        model_aqi = None
        
    # Standard Rain Evaluation Buckets (mm/hr)
    test_rain_buckets = np.array([[0], [1], [2.5], [5], [10], [15], [20], [25]])
    
    predicted_speeds = model_speed.predict(test_rain_buckets)
    predicted_congs = model_cong.predict(test_rain_buckets) if model_cong else np.zeros(len(test_rain_buckets))
    predicted_aqis = model_aqi.predict(test_rain_buckets) if model_aqi else np.zeros(len(test_rain_buckets))
    
    # Baseline checks (Rain = 0)
    base_speed = predicted_speeds[0] if predicted_speeds[0] > 0 else 40.0
    base_aqi = predicted_aqis[0] if predicted_aqis[0] > 0 else 150.0
    
    records_to_insert = []
    
    for i, rain_val in enumerate(test_rain_buckets.flatten()):
        # Calculate percentage drops/bumps relative to baseline (0mm rain)
        speed_pred = max(5.0, min(60.0, predicted_speeds[i])) # Clamp speeds
        cong_pred = max(0.0, min(100.0, predicted_congs[i]))
        aqi_pred = max(10.0, min(500.0, predicted_aqis[i]))
        
        speed_drop_pct = ((base_speed - speed_pred) / base_speed) * 100
        aqi_drop_pct = ((base_aqi - aqi_pred) / base_aqi) * 100
        
        # We ensure it doesn't represent bizarre physics (rain shouldn't vastly increase speed)
        speed_drop_pct = max(0.0, speed_drop_pct) 
        aqi_drop_pct = max(0.0, aqi_drop_pct) # Rain washes out pollution -> AQI drops (improves)
        
        row = {
            "rain_bucket_mm": float(rain_val),
            "predicted_speed_drop_pct": float(speed_drop_pct),
            "predicted_aqi_drop_pct": float(aqi_drop_pct),
            "predicted_congestion_bump_pct": float(max(0, cong_pred - predicted_congs[0])),
            "confidence_score": float(len(df) / 10000.0),
            "sample_size": int(len(df)),
            "trained_at": datetime.now(timezone.utc).isoformat(),
        }
        records_to_insert.append(row)
        
    print("Deleting stale ML weights from Supabase...")
    try:
        supabase.table("weather_ml_insights").delete().neq("rain_bucket_mm", -1).execute()
    except Exception as e:
        print(f"Warning on clearing table: {e}")

    print("Uploading new ML predicted curves...")
    response = supabase.table("weather_ml_insights").insert(records_to_insert).execute()
    
    print(f"Success! Inserted {len(response.data)} ML prediction points into DB.")

if __name__ == "__main__":
    train_weather_ml()
