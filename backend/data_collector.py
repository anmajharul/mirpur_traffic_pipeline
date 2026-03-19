import os
import requests
import time
from datetime import datetime

# ==========================================
# 🔑 API KEYS FROM ENV (GitHub Secrets)
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()

if not all([SUPABASE_URL, SUPABASE_KEY, HERE_API_KEY]):
    print("❌ Missing API keys! Check GitHub Secrets.")
    exit(1)

# মিরপুর-১০ গোলচত্বর (Destination)
CENTER = "23.8071318,90.3686089"

# ৪টি এপ্রোচ রোড থেকে গোলচত্বরের দিকে আসা
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "coord": "23.8115,90.3686"},
    {"direction": "South (Kazipara)", "coord": "23.8035,90.3686"},
    {"direction": "East (Mirpur-14)", "coord": "23.8071,90.3736"},
    {"direction": "West (Mirpur-2)", "coord": "23.8071,90.3636"},
]

session = requests.Session()

# ==========================================
# 🚗 GET TRAFFIC SPEED FROM HERE ROUTING API
# ==========================================
def get_traffic_speed(origin):
    url = "https://router.hereapi.com/v8/routes"
    params = {
        "transportMode": "car",
        "origin": origin,
        "destination": CENTER,
        "return": "summary",
        "routingMode": "fast",
        "departureTime": "now", # রিয়েল-টাইম ট্রাফিক নিশ্চিত করতে
        "apikey": HERE_API_KEY
    }

    for attempt in range(3):
        try:
            res = session.get(url, params=params, timeout=25)
            if res.status_code != 200:
                print(f"⚠️ API Error {res.status_code} for {origin}")
                time.sleep(3)
                continue

            data = res.json()
            routes = data.get("routes", [])
            
            if not routes:
                print(f"⚠️ No route found for {origin}")
                return None

            # সেফলি সামারি ডেটা বের করা
            sections = routes[0].get("sections", [])
            summary = sections[0].get("summary", {}) if sections else {}
            
            duration = summary.get("duration", 0) # সেকেন্ডে
            length = summary.get("length", 0)     # মিটারে

            if duration > 0:
                # মিটার/সেকেন্ড থেকে কিমি/ঘণ্টায় রূপান্তর
                speed_kmh = (length / duration) * 3.6
                return round(speed_kmh, 2)
            
            return 0

        except Exception as e:
            print(f"⚠️ Network error on attempt {attempt+1}: {e}")
            time.sleep(5)

    return None

# ==========================================
# 🗄️ INSERT INTO SUPABASE
# ==========================================
def supabase_insert(payload):
    url = f"{SUPABASE_URL}/rest/v1/traffic_records"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    try:
        res = session.post(url, json=payload, headers=headers, timeout=20)
        if not res.ok:
            print(f"❌ Supabase Error: {res.status_code} — {res.text}")
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")

# ==========================================
# 🧠 MAIN COLLECTION LOGIC
# ==========================================
def collect():
    print(f"🚀 Starting collection: {datetime.now().strftime('%H:%M:%S')}")
    
    # ISO 8601 ফরম্যাটে বর্তমান সময়
    now_iso = datetime.now().isoformat()
    success = 0

    for loc in LOCATIONS:
        speed = get_traffic_speed(loc["coord"])

        if speed is None:
            print(f"⚠️ Skipped {loc['direction']} due to API issues.")
            continue

        record = {
            "speed_kmh": speed,
            "direction": loc["direction"],
            "destination": "Mirpur-10 Circle",
            "timestamp": now_iso
        }

        supabase_insert(record)
        print(f"✅ {loc['direction']}: {speed} km/h")
        
        success += 1
        time.sleep(1) # API লিমিট এড়াতে ছোট বিরতি

    print(f"📊 Summary: {success}/4 locations successfully processed.")

if __name__ == "__main__":
    collect()