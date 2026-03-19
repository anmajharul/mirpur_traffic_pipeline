import os
import requests
import time
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, HERE_API_KEY]):
    print("❌ API Keys missing! Check GitHub Secrets.")
    exit(1)

# Mirpur-10 center
CENTER = "23.8071318,90.3686089"

# 4 approach roads
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "coord": "23.8115,90.3686"},
    {"direction": "South (Kazipara)", "coord": "23.8035,90.3686"},
    {"direction": "East (Mirpur-14)", "coord": "23.8071,90.3736"},
    {"direction": "West (Mirpur-2)", "coord": "23.8071,90.3636"},
]

# ==========================================
# 🌐 SESSION WITH RETRIES (Network-safe)
# ==========================================

session = requests.Session()


# ==========================================
# 🚦 TRAFFIC SPEED FROM HERE ROUTING API
# ==========================================

def get_traffic_speed(destination):

    url = "https://router.hereapi.com/v8/routes"

    params = {
        "transportMode": "car",
        "origin": CENTER,
        "destination": destination,
        "routingMode": "fast",
        "traffic": "true",
        "apikey": HERE_API_KEY
    }

    for attempt in range(3):
        try:
            res = session.get(url, params=params, timeout=25)

            if res.status_code == 200:

                data = res.json()

                summary = data["routes"][0]["sections"][0]["summary"]

                duration = summary["duration"]   # seconds
                distance = summary["length"]     # meters

                if duration > 0:
                    speed_kmh = (distance / duration) * 3.6
                    return round(speed_kmh, 2)

                return 0

            else:
                print(f"⚠️ API Error {res.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error (attempt {attempt+1}): {e}")
            time.sleep(5)

    return None


# ==========================================
# 💾 INSERT INTO SUPABASE
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

        if res.ok:
            print("🗄️ Saved to Supabase")
        else:
            print(f"❌ Supabase Error: {res.status_code} — {res.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ DB Connection Error: {e}")


# ==========================================
# 🚀 MAIN COLLECTION LOGIC
# ==========================================

def collect():

    print(f"🚀 Starting collection: {datetime.now().strftime('%H:%M:%S')}")

    now_iso = datetime.utcnow().isoformat()
    success = 0

    for loc in LOCATIONS:

        speed = get_traffic_speed(loc["coord"])

        if speed is not None:

            record = {
                "speed_kmh": speed,
                "direction": loc["direction"],
                "destination": "Mirpur-10 Circle",
                "timestamp": now_iso
            }

            supabase_insert(record)

            print(f"✅ {loc['direction']}: {speed} km/h")

            success += 1

        else:
            print(f"❌ Failed: {loc['direction']}")

    print(f"📊 Summary: {success}/4 locations processed.")


# ==========================================
# ▶️ ENTRY POINT
# ==========================================

if __name__ == "__main__":
    collect()