import os
import requests
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()

if not all([SUPABASE_URL, SUPABASE_KEY, HERE_API_KEY]):
    print("❌ API Keys missing! Check GitHub Secrets.")
    exit(1)

# মিরপুর-১০ এর ৪টি ডিরেকশন (এগুলো এখন সার্কেল হিসেবে কাজ করবে)
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "lat": "23.8115", "lon": "90.3686"},
    {"direction": "South (Kazipara)", "lat": "23.8035", "lon": "90.3686"},
    {"direction": "East (Mirpur-14)", "lat": "23.8071", "lon": "90.3736"},
    {"direction": "West (Mirpur-2)", "lat": "23.8071", "lon": "90.3636"}
]

def get_traffic_speed_here(lat, lon):
    # HERE Traffic v8: ১০০ মিটার ব্যাসার্ধের ভেতরের ট্রাফিক ডেটা নেবে
    url = "https://traffic.hereapi.com/v8/flow"
    params = {
        "location": f"circle:{lat},{lon};r=100",
        "apiKey": HERE_API_KEY
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        if res.status_code == 200:
            data = res.json()
            if "results" in data and len(data["results"]) > 0:
                # গড় গতিবেগ (km/h) বের করা হচ্ছে
                speed = data["results"][0].get("currentFlow", {}).get("speed", 0)
                return round(float(speed), 2)
            return 0
        else:
            print(f"⚠️ HERE API Error {res.status_code}: {res.text}")
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
    requests.post(url, json=payload, headers=headers)

def collect():
    print(f"🚀 Data collection started with HERE Maps at {datetime.now().strftime('%H:%M:%S')}")
    now_iso = datetime.now().isoformat()
    success_count = 0
    
    for loc in LOCATIONS:
        speed = get_traffic_speed_here(loc['lat'], loc['lon'])
        if speed is not None:
            record = {
                "speed_kmh": speed,
                "direction": loc['direction'],
                "destination": "Mirpur-10 Circle",
                "timestamp": now_iso
            }
            supabase_insert(record)
            print(f"✅ {loc['direction']}: {speed} km/h (Radius 100m)")
            success_count += 1
            
    print(f"📊 Finished. {success_count}/4 locations saved.")

if __name__ == "__main__":
    collect()