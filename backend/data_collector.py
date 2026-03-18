import os
import requests
import json
import time
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS (NO HARDCODED SECRETS)
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, TOMTOM_API_KEY]):
    print("❌ API Keys missing! Make sure they are set in GitHub Secrets.")
    exit(1)

# Mirpur-10 Coordinates (Matched with your previous data_pipeline.py)
LAT = 23.8067
LON = 90.3687


def supabase_insert(table_name: str, payload: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    res = requests.post(url, json=payload, headers=headers, timeout=15)
    if not res.ok:
        raise RuntimeError(f"Supabase insert failed ({res.status_code}): {res.text}")

# ==========================================
# 🚦 FETCH REAL-TIME DATA
# ==========================================
def get_traffic_speed():
    # Fixed URL format for TomTom API
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={LAT}%2C{LON}&key={TOMTOM_API_KEY}"
    try:
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()
            flow = data.get('flowSegmentData', {})
            return {
                'speed': flow.get('currentSpeed', None),
                'freeFlowSpeed': flow.get('freeFlowSpeed', 50)
            }
        else:
            print(f"TomTom API Error: {res.status_code}")
            return None
    except Exception as e:
        print(f"TomTom exception: {e}")
        return None

def get_weather():
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true&hourly=rain&timezone=Asia/Dhaka"
    try:
        res = requests.get(url)
        data = res.json()
        if "current_weather" in data:
            return {
                'temp_c': data["current_weather"].get("temperature", 0),
                'wind_speed': data["current_weather"].get("windspeed", 0),
                'rain_mm': data["hourly"]["rain"][0] if "hourly" in data else 0.0
            }
        return None
    except Exception as e:
        print(f"Meteo error: {e}")
        return None

# ==========================================
# 💾 SAVE TO DATABASE
# ==========================================
def collect_and_save():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Collecting Mirpur-10 ML Data...")
    
    traffic = get_traffic_speed()
    weather = get_weather()
    
    if traffic is None or traffic['speed'] is None or weather is None:
        print("Failed to fetch APIs. Skipping this interval.")
        return
        
    speed = float(traffic['speed'])
    free_flow_speed = float(traffic['freeFlowSpeed'])
    
    # Calculate congestion exactly like your previous script
    congestion = max(0.0, min(100.0, 100.0 - (speed / free_flow_speed) * 100))

    rain_val = float(weather.get('rain_mm', 0.0))
    temp_val = float(weather.get('temp_c', 0.0))
    
    record = {
        # 'timestamp' column was changed to 'created_at' in the previous discussion
        # Make sure your Supabase table has a 'created_at' column (timestamp with time zone)
        "speed_kmh": speed,
        "congestion_percent": round(congestion, 1),
        "rain_mm": rain_val,
        "temperature": temp_val,
        "destination": "Mirpur-10 Node" # Added to match your previous schema
    }
    
    # Push to Supabase
    try:
        # Pushing to traffic_records as requested
        supabase_insert("traffic_records", record)
        print(f"✅ Successfully inserted 1 row. Speed: {speed}km/h, Rain: {rain_val}mm, Congestion: {round(congestion, 1)}%")
    except Exception as e:
        print(f"❌ Failed to push to Supabase: {e}")

# ==========================================
# 🕒 ENTRY POINT
# ==========================================
if __name__ == "__main__":
    collect_and_save()
