import os
import requests
import time
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

# মিরপুর-১০ এর ৪টি এপ্রোচ রোডের কোঅর্ডিনেট
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "lat": "23.8115", "lon": "90.3686"},
    {"direction": "South (Kazipara)", "lat": "23.8035", "lon": "90.3686"},
    {"direction": "East (Mirpur-14)", "lat": "23.8071", "lon": "90.3736"},
    {"direction": "West (Mirpur-2)", "lat": "23.8071", "lon": "90.3636"}
]

def get_traffic_speed_here(lat, lon):
    # ls.hereapi.com সাধারণত গ্লোবাল ট্রাফিক সার্ভিসের জন্য বেশি স্ট্যাবল
    url = "https://traffic.ls.hereapi.com/v8/flow"
    params = {
        "location": f"circle:{lat},{lon};r=100",
        "apiKey": HERE_API_KEY
    }
    
    # DNS বা নেটওয়ার্ক এরর হ্যান্ডেল করার জন্য ২ বার ট্রাই করবে
    for attempt in range(2):
        try:
            res = requests.get(url, params=params, timeout=20)
            if res.status_code == 200:
                data = res.json()
                if "results" in data and len(data["results"]) > 0:
                    speed = data["results"][0].get("currentFlow", {}).get("speed", 0)
                    return round(float(speed), 2)
                return 0
            else:
                print(f"⚠️ Attempt {attempt+1}: API Error {res.status_code}")
        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed: DNS/Network Error. Retrying in 5s...")
            time.sleep(5) # ৫ সেকেন্ড অপেক্ষা করবে
            
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
        requests.post(url, json=payload, headers=headers, timeout=15)
    except:
        print("❌ Failed to push to Supabase.")

def collect():
    print(f"🚀 Starting collection with HERE Maps: {datetime.now().strftime('%H:%M:%S')}")
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
            print(f"✅ {loc['direction']}: {speed} km/h (Saved)")
            success_count += 1
            
    print(f"📊 Summary: {success_count}/4 locations saved to Supabase.")

if __name__ == "__main__":
    collect()