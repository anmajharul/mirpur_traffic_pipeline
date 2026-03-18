import os
import requests
import json
import time
from datetime import datetime
from supabase import create_client, Client

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
# Get these from your Supabase Project Settings -> API
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://fojusiwszuetnibdgbze.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "sb_publishable_mUMIUt23GmSVnbHk_AzB8w_xzRQvjvv")

# APIs you already have
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "nQ4cIRr27XiPbXFQMmMQicsNa0hBOm8w")

# Mirpur-10 Coordinates
LAT = 23.8103
LON = 90.4125

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🚦 FETCH REAL-TIME DATA
# ==========================================
def get_traffic_speed():
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={LAT},{LON}&unit=KMPH&key={TOMTOM_API_KEY}"
    try:
        res = requests.get(url)
        data = res.json()
        return data.get('flowSegmentData', {}).get('currentSpeed', None)
    except Exception as e:
        print(f"TomTom error: {e}")
        return None

def get_weather():
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current=temperature_2m,precipitation,weathercode"
    try:
        res = requests.get(url)
        data = res.json().get('current', {})
        return {
            'temp_c': data.get('temperature_2m', 0),
            'rain_mm': data.get('precipitation', 0),
            'code': data.get('weathercode', 0)
        }
    except Exception as e:
        print(f"Meteo error: {e}")
        return None

# ==========================================
# 💾 SAVE TO DATABASE
# ==========================================
def collect_and_save():
    print(f"[{datetime.now()}] Collecting Mirpur-10 ML Data...")
    
    speed = get_traffic_speed()
    weather = get_weather()
    
    if speed is None or weather is None:
        print("Failed to fetch APIs. Skipping this interval.")
        return
        
    # Tell strict linters (like Pyre/MyPy) that these are definitely not None
    assert isinstance(speed, (int, float))
    assert isinstance(weather, dict)

    # Assuming a linear relationship for raw congestion metric (0-100 scale based on speed)
    # A base speed of 40 km/h is 0% congestion. Speed of 5 km/h is ~80% congestion.
    congestion = float(max(0.0, min(100.0, 100.0 - (float(speed) * 2.2))))

    # Construct the data row
    rain_val = float(weather.get('rain_mm', 0.0))
    temp_val = float(weather.get('temp_c', 0.0))
    
    record = {
        "timestamp": datetime.utcnow().isoformat(),  # Supabase uses UTC timestamps
        "speed_kmh": float(speed),
        "congestion_pct": float(f"{congestion:.2f}"),
        "rain_mm": rain_val,
        "temp_c": temp_val
    }
    
    # Push to Supabase
    try:
        response = supabase.table("traffic_records").insert(record).execute()
        print(f"✅ Successfully inserted 1 row. Speed: {speed}km/h, Rain: {rain_val}mm")
    except Exception as e:
        print(f"❌ Failed to push to Supabase: {e}")

# ==========================================
# 🕒 ENTRY POINT
# ==========================================
if __name__ == "__main__":
    # If deploying on a crontab / Heroku scheduler, just run collect_and_save()
    collect_and_save()
