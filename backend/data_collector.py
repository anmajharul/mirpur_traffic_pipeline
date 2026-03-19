import os
import requests
import time
from datetime import datetime, timezone

# ==========================================
# 🔑 API KEYS FROM ENV (GitHub Secrets)
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()

if not all([SUPABASE_URL, SUPABASE_KEY, HERE_API_KEY]):
    print("❌ Missing API keys! Check GitHub Secrets.")
    exit(1)

# মিরপুর-১০ গোলচত্বর
CENTER = "23.8071318,90.3686089"

LOCATIONS = [
    {"direction": "North (Mirpur-11)", "coord": "23.8115,90.3686"},
    {"direction": "South (Kazipara)", "coord": "23.8035,90.3686"},
    {"direction": "East (Mirpur-14)", "coord": "23.8071,90.3736"},
    {"direction": "West (Mirpur-2)", "coord": "23.8071,90.3636"},
]

session = requests.Session()

# ==========================================
# 🌤️ GET WEATHER DATA (Open-Meteo)
# ==========================================
def get_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=23.8071&longitude=90.3686&current_weather=true&hourly=rain"
    try:
        res = session.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if "current_weather" in data:
                return {
                    "temp": data["current_weather"].get("temperature", 0),
                    "wind": data["current_weather"].get("windspeed", 0),
                    "rain": data.get("hourly", {}).get("rain", [0.0])[0]
                }
    except Exception as e:
        print(f"⚠️ Weather API failed: {e}")
    # 🚨 API ফেইল করলে এই সেফটি ভ্যালুগুলো যাবে, তাই কোনো NULL আসবে না
    return {"temp": 0, "wind": 0, "rain": 0.0}

# ==========================================
# 🚗 GET TRAFFIC SPEED
# ==========================================
def get_traffic_speed(origin):
    url = "https://router.hereapi.com/v8/routes"
    now_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    params = {
        "transportMode": "car",
        "origin": origin,
        "destination": CENTER,
        "return": "summary",
        "routingMode": "fast",
        "departureTime": now_iso,
        "apiKey": HERE_API_KEY
    }

    for attempt in range(3):
        try:
            res = session.get(url, params=params, timeout=25)
            if res.status_code == 200:
                data = res.json()
                routes = data.get("routes", [])
                if routes:
                    summary = routes[0].get("sections", [{}])[0].get("summary", {})
                    duration = summary.get("duration", 0)
                    length = summary.get("length", 0)
                    if duration > 0:
                        speed_kmh = (length / duration) * 3.6
                        return round(speed_kmh, 2)
                return 0
            else:
                print(f"⚠️ API Error {res.status_code} for {origin}")
                time.sleep(3)
        except Exception as e:
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
    now_db = datetime.now().isoformat()
    success = 0

    weather = get_weather()

    for loc in LOCATIONS:
        speed = get_traffic_speed(loc["coord"])
        if speed is not None:
            free_flow_speed = 40.0
            congestion = max(0.0, min(100.0, 100.0 - (speed / free_flow_speed) * 100))

            record = {
                "timestamp": now_db,
                "speed_kmh": speed,
                "congestion_percent": round(congestion, 1),
                "rain_mm": weather["rain"],
                "temperature": weather["temp"],
                "wind_speed": weather["wind"], 
                "direction": loc["direction"],
                "destination": "Mirpur-10 Circle"
            }
            
            supabase_insert(record)
            print(f"✅ {loc['direction']}: Speed {speed} km/h | Wind {weather['wind']} km/h")
            success += 1
        time.sleep(1)

    print(f"📊 Summary: {success}/4 processed.")

if __name__ == "__main__":
    collect()