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

# মিরপুর-১০ এর ৪টি মেইন পয়েন্ট (North, South, East, West)
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "lat": "23.8115", "lon": "90.3686"},
    {"direction": "South (Kazipara)", "lat": "23.8035", "lon": "90.3686"},
    {"direction": "East (Mirpur-14)", "lat": "23.8071", "lon": "90.3736"},
    {"direction": "West (Mirpur-2)", "lat": "23.8071", "lon": "90.3636"}
]

# ==========================================
# 🚦 CORE LOGIC
# ==========================================

def get_weather():
    # আবহাওয়ার ডেটা (Open-Meteo API)
    url = "https://api.open-meteo.com/v1/forecast?latitude=23.8071&longitude=90.3686&current_weather=true&hourly=rain"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if "current_weather" in data:
            return {
                "temp": data["current_weather"].get("temperature", 25),
                "wind": data["current_weather"].get("windspeed", 5),
                "rain": data.get("hourly", {}).get("rain", [0])[0]
            }
    except: return {"temp": 25, "wind": 5, "rain": 0}

def get_traffic_speed_here(lat, lon):
    url = "https://traffic.hereapi.com/v8/flow"
    # ব্যাসার্ধ ৫০০ মিটার করা হলো যাতে ম্যাপের সেগমেন্ট মিস না হয়
    params = {
        "location": f"circle:{lat},{lon};r=500", 
        "apiKey": HERE_API_KEY
    }
    try:
        res = requests.get(url, params=params, timeout=20)
        data = res.json()
        if "results" in data and len(data["results"]) > 0:
            speed = data["results"][0].get("currentFlow", {}).get("speed", 0)
            return round(float(speed), 2)
        else:
            print(f"⚠️ No summary data for {lat},{lon}. It might be a data gap in HERE Maps.")
            return 0
    except Exception as e:
        print(f"❌ API Call failed: {e}")
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
        res = requests.post(url, json=payload, headers=headers, timeout=15)
        if not res.ok:
            print(f"❌ Supabase Error: {res.text}")
    except:
        print("❌ DB Push Failed.")

def collect():
    print(f"🚀 Starting Collection: {datetime.now().strftime('%H:%M:%S')}")
    weather = get_weather()
    now_iso = datetime.now().isoformat()
    success_count = 0
    
    for loc in LOCATIONS:
        speed = get_traffic_speed_here(loc['lat'], loc['lon'])
        
        if speed is not None:
            # তোমার ডাটাবেসের সব কলাম এখানে সেট করা হয়েছে
            record = {
                "timestamp": now_iso,
                "speed_kmh": speed,
                "direction": loc['direction'],
                "temperature": weather["temp"],
                "wind_speed": weather["wind"],
                "rain_mm": weather["rain"],
                "destination": "Mirpur-10 Circle"
            }
            supabase_insert(record)
            print(f"✅ {loc['direction']}: {speed} km/h (Saved to DB)")
            success_count += 1
            
    print(f"📊 Summary: {success_count}/4 saved to Supabase.")

if __name__ == "__main__":
    collect()