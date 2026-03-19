import os
import requests
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "").strip()

if not all([SUPABASE_URL, SUPABASE_KEY, TOMTOM_API_KEY]):
    print("❌ API Keys missing in GitHub Secrets!")
    exit(1)

# মিরপুর-১০ গোলচত্বরের ৪টি এপ্রোচ রোড (রাস্তার ওপরের বিন্দু)
LOCATIONS = [
    {"direction": "North (Mirpur-11 Road)", "lat": "23.8078", "lon": "90.3686"},
    {"direction": "South (Kazipara Road)", "lat": "23.8064", "lon": "90.3686"},
    {"direction": "East (Mirpur-14 Road)", "lat": "23.8071", "lon": "90.3694"},
    {"direction": "West (Mirpur-2 Road)", "lat": "23.8071", "lon": "90.3678"}
]

def get_traffic_speed(lat, lon):
    # জুম লেভেল ১৮ রাস্তার নিখুঁত সেগমেন্ট পাওয়ার জন্য সেরা
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/18/json"
    params = {
        "point": f"{lat},{lon}",
        "key": TOMTOM_API_KEY
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            data = res.json()
            flow = data.get('flowSegmentData', {})
            return {
                'speed': flow.get('currentSpeed', 0),
                'freeFlowSpeed': flow.get('freeFlowSpeed', 50)
            }
        else:
            print(f"⚠️ API Error {res.status_code} at {lat},{lon}: {res.text}")
            return None
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None

def supabase_insert(payload):
    url = f"{SUPABASE_URL}/rest/v1/traffic_records"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        if not res.ok:
            print(f"❌ DB Error: {res.text}")
    except Exception as e:
        print(f"❌ DB Network Error: {e}")

def collect():
    print(f"🚀 Data collection started at {datetime.now().strftime('%H:%M:%S')}")
    success_count = 0
    
    for loc in LOCATIONS:
        data = get_traffic_speed(loc['lat'], loc['lon'])
        if data:
            speed = float(data['speed'])
            record = {
                "speed_kmh": speed,
                "direction": loc['direction'],
                "destination": "Mirpur-10 Circle",
                "timestamp": datetime.now().isoformat()
            }
            supabase_insert(record)
            print(f"✅ {loc['direction']}: {speed} km/h")
            success_count += 1
    
    print(f"📊 Finished. {success_count}/4 locations saved.")

if __name__ == "__main__":
    collect()