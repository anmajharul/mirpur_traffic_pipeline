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

# মিরপুর-১০ গোলচত্বর থেকে ২০০ মিটার দূরের ৪টি পয়েন্ট (More stable for TomTom)
LOCATIONS = [
    {"direction": "North (Towards Mirpur-11)", "lat": "23.8102", "lon": "90.3686"},
    {"direction": "South (Towards Kazipara)", "lat": "23.8045", "lon": "90.3686"},
    {"direction": "East (Towards Mirpur-14)", "lat": "23.8071", "lon": "90.3721"},
    {"direction": "West (Towards Mirpur-2)", "lat": "23.8071", "lon": "90.3652"}
]

def get_traffic_speed(lat, lon):
    # Zoom level 10 ব্যবহার করা হচ্ছে যাতে বড় রোড সেগমেন্ট সহজে ডিটেক্ট হয়
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
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
            # ডিবাগিং এর জন্য এরর টেক্সট প্রিন্ট করা হচ্ছে
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
                "destination": "Mirpur-10 Area",
                "timestamp": datetime.now().isoformat()
            }
            supabase_insert(record)
            print(f"✅ {loc['direction']}: {speed} km/h")
            success_count += 1
    
    print(f"📊 Finished. {success_count}/4 locations saved to Supabase.")

if __name__ == "__main__":
    collect()