import os
import requests
import time
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()

if not all([SUPABASE_URL, SUPABASE_KEY, HERE_API_KEY]):
    print("❌ Missing API keys")
    exit(1)

# 📍 Mirpur-10 nearby roads
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "lat": "23.8115", "lon": "90.3686"},
    {"direction": "South (Kazipara)", "lat": "23.8035", "lon": "90.3686"},
    {"direction": "East (Mirpur-14)", "lat": "23.8071", "lon": "90.3736"},
    {"direction": "West (Mirpur-2)", "lat": "23.8071", "lon": "90.3636"},
]

session = requests.Session()

# ==========================================
# 🚦 GET TRAFFIC SPEED (REAL FLOW DATA)
# ==========================================

def get_traffic_speed(lat, lon):

    url = "https://traffic.ls.hereapi.com/v8/flow"

    params = {
        "location": f"circle:{lat},{lon};r=300",
        "apiKey": HERE_API_KEY
    }

    try:
        res = session.get(url, params=params, timeout=25)

        if res.status_code != 200:
            print(f"⚠️ API Error {res.status_code}")
            return None

        data = res.json()

        results = data.get("results")
        if not results:
            print("⚠️ No traffic data")
            return None

        speed = results[0].get("currentFlow", {}).get("speed")

        if speed is None:
            print("⚠️ No speed info")
            return None

        return round(float(speed), 2)

    except Exception as e:
        print(f"⚠️ Network error: {e}")
        return None


# ==========================================
# 🗄️ SUPABASE INSERT
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
        session.post(url, json=payload, headers=headers, timeout=20)
    except Exception as e:
        print(f"❌ DB Error: {e}")


# ==========================================
# 🚀 MAIN
# ==========================================

def collect():

    print(f"🚀 Starting collection: {datetime.now().strftime('%H:%M:%S')}")

    now_iso = datetime.utcnow().isoformat()
    success = 0

    for loc in LOCATIONS:

        speed = get_traffic_speed(loc["lat"], loc["lon"])

        if speed is None:
            print(f"⚠️ Skipped {loc['direction']}")
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

        time.sleep(1)

    print(f"📊 Summary: {success}/4 locations processed.")


if __name__ == "__main__":
    collect()