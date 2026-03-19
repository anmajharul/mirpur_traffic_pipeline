import os
import requests
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, TOMTOM_API_KEY]):
    print("❌ API Keys missing! Make sure they are set in GitHub Secrets.")
    exit(1)

# মিরপুর-১০ এর চারদিকের এপ্রোচ লেগ
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "lat": "23.8115", "lon": "90.3686"},
    {"direction": "South (Kazipara)", "lat": "23.8035", "lon": "90.3686"},
    {"direction": "East (Mirpur-14)", "lat": "23.8071", "lon": "90.3736"},
    {"direction": "West (Mirpur-2)", "lat": "23.8071", "lon": "90.3636"}
]

# গোলচত্বরের সেন্টার (শুধু আবহাওয়ার জন্য)
CENTER_LAT = "23.8071"
CENTER_LON = "90.3686"

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
def get_traffic_speed(lat, lon):
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={lat}%2C{lon}&key={TOMTOM_API_KEY}"
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
            print(f"TomTom API Error at {lat},{lon}: {res.status_code}")
            return None
    except Exception as e:
        print(f"TomTom exception: {e}")
        return None

def get_weather():
    url = f"https://api.open-meteo.com/v1/forecast?latitude={CENTER_LAT}&longitude={CENTER_LON}&current_weather=true&hourly=rain&timezone=Asia/Dhaka"
    try:
        res = requests.get(url)
        data = res.json()
        if "current_weather" in data:
            return {
                'temp_c': data["current_weather"].get("temperature", 0),
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
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Collecting Mirpur-10 Multi-Directional Data...")
    
    # আবহাওয়া একবারই নেব
    weather = get_weather()
    rain_val = float(weather.get('rain_mm', 0.0)) if weather else 0.0
    temp_val = float(weather.get('temp_c', 0.0)) if weather else 0.0

    # ৪ দিকের জন্য লুপ
    for loc in LOCATIONS:
        traffic = get_traffic_speed(loc['lat'], loc['lon'])
        
        if traffic is None or traffic['speed'] is None:
            print(f"Failed to fetch traffic for {loc['direction']}. Skipping.")
            continue
            
        speed = float(traffic['speed'])
        free_flow_speed = float(traffic['freeFlowSpeed'])
        congestion = max(0.0, min(100.0, 100.0 - (speed / free_flow_speed) * 100))
        
        record = {
            "speed_kmh": speed,
            "congestion_percent": round(congestion, 1),
            "rain_mm": rain_val,
            "temperature": temp_val,
            "destination": "Mirpur-10 Node",
            "direction": loc['direction']  # নতুন কলাম
        }
        
        try:
            supabase_insert("traffic_records", record)
            print(f"✅ {loc['direction']}: Speed: {speed}km/h, Congestion: {round(congestion, 1)}%")
        except Exception as e:
            print(f"❌ Failed to push {loc['direction']} to Supabase: {e}")

if __name__ == "__main__":
    collect_and_save()