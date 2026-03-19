import os
import requests
import time
import csv
from datetime import datetime, timezone

# ==========================================
# 🔑 API KEYS & CONFIG (GitHub Secrets)
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

# মিরপুর-১০ গোলচত্বর (Destination)
CENTER = "23.8071318,90.3686089"

# কালেকশন পয়েন্টসমূহ
LOCATIONS = [
    {"direction": "North (Mirpur-11)", "coord": "23.8115,90.3686"},
    {"direction": "South (Kazipara)", "coord": "23.8035,90.3686"},
    {"direction": "East (Mirpur-14)", "coord": "23.8071,90.3736"},
    {"direction": "West (Mirpur-2)", "coord": "23.8071,90.3636"},
]

# Session reuse for 5-min intervals performance
session = requests.Session()

# ==========================================
# 📁 CSV BACKUP UTILITY (Academic Standard)
# ==========================================
def save_to_csv(record):
    """রেকর্ডটি GitHub রিপোজিটরির CSV ফাইলে অ্যাপেন্ড করবে"""
    file_path = 'backend/traffic_data_backup.csv'
    file_exists = os.path.isfile(file_path)
    
    try:
        # utf-8-sig ব্যবহার করা হয়েছে যাতে Excel এ ফাইলটি ঠিকমতো ওপেন হয়
        with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=record.keys())
            if not file_exists or os.stat(file_path).st_size == 0:
                writer.writeheader()
            writer.writerow(record)
        print(f"📁 Local CSV backup updated.")
    except Exception as e:
        print(f"⚠️ CSV backup failed: {e}")

# ==========================================
# 🌤️ COMPREHENSIVE WEATHER & AQI FETCH
# ==========================================
def get_comprehensive_weather():
    coords = "23.807,90.368"
    current_url = f"https://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={coords}&aqi=yes"
    astro_url = f"https://api.weatherapi.com/v1/astronomy.json?key={WEATHER_API_KEY}&q={coords}"
    
    try:
        curr_res = session.get(current_url, timeout=10)
        curr_res.raise_for_status()
        astro_res = session.get(astro_url, timeout=10)
        astro_res.raise_for_status()

        curr_data = curr_res.json()
        astro_data = astro_res.json()

        curr = curr_data.get('current', {})
        air = curr.get('air_quality', {})
        astro = astro_data.get('astronomy', {}).get('astro', {})

        return {
            "temp_c": curr.get('temp_c'),
            "precip_mm": curr.get('precip_mm'),
            "wind_kph": curr.get('wind_kph'),
            "wind_dir": curr.get('wind_dir'),
            "humidity": curr.get('humidity'),
            "visibility_km": curr.get('vis_km'),
            "uv_index": curr.get('uv'),
            "weather_condition": curr.get('condition', {}).get('text', "Unknown"),
            "aqi": air.get('us-epa-index'),
            "pm2_5": air.get('pm2_5'),
            "pm10": air.get('pm10'),
            "co_level": air.get('co'),
            "no2_level": air.get('no2'),
            "sunrise": astro.get('sunrise'),
            "sunset": astro.get('sunset'),
            "moon_phase": astro.get('moon_phase')
        }
    except Exception as e:
        print(f"⚠️ Weather API error: {e}")
        return None

# ==========================================
# 🚗 TRAFFIC SPEED FETCH (HERE API)
# ==========================================
def get_traffic_speed(origin):
    url = "https://router.hereapi.com/v8/routes"
    params = {
        "transportMode": "car",
        "origin": origin,
        "destination": CENTER,
        "return": "summary",
        "routingMode": "fast",
        "apiKey": HERE_API_KEY
    }
    try:
        res = session.get(url, params=params, timeout=12)
        if res.status_code == 200:
            data = res.json()
            if data.get("routes"):
                summary = data["routes"][0]["sections"][0].get("summary", {})
                duration = summary.get("duration", 0)
                length = summary.get("length", 0)
                if duration > 0:
                    return round((length / duration) * 3.6, 2)
    except Exception as e:
        print(f"⚠️ Traffic API error: {e}")
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
        res = session.post(url, json=payload, headers=headers, timeout=15)
        if res.ok:
            print(f"✅ Supabase updated for {payload['direction']}")
        else:
            print(f"❌ DB Error {res.status_code}: {res.text}")
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")

# ==========================================
# 🧠 MAIN COLLECTION LOGIC
# ==========================================
def collect():
    print(f"🚀 Starting collection: {datetime.now().strftime('%H:%M:%S')}")
    weather = get_comprehensive_weather()
    w_data = weather if weather else {} # Weather না থাকলে empty dict যাতে KeyError না হয়
    
    # সবার জন্য একই টাইমস্ট্যাম্প নিশ্চিত করা
    now_db = datetime.now(timezone.utc).isoformat()
    
    for loc in LOCATIONS:
        speed = get_traffic_speed(loc["coord"])
        if speed is not None:
            # ৪০ কিমি/ঘণ্টা ফ্রি-ফ্লো স্পিড ধরে কনজেশন ক্যালকুলেশন
            congestion = max(0.0, min(100.0, 100.0 - (speed / 40.0) * 100))
            
            record = {
                "timestamp": now_db,
                "direction": loc["direction"],
                "speed_kmh": speed,
                "congestion_percent": round(congestion, 1),
                "destination": "Mirpur-10 Circle",
                "temperature": w_data.get('temp_c'), # Default None (Academic Standard)
                "rain_mm": w_data.get('precip_mm'),
                "wind_speed": w_data.get('wind_kph'),
                "wind_dir": w_data.get('wind_dir'),
                "visibility_km": w_data.get('visibility_km'),
                "uv_index": w_data.get('uv_index'),
                "weather_condition": w_data.get('weather_condition', "Unknown"),
                "aqi": w_data.get('aqi'),
                "pm2_5": w_data.get('pm2_5'),
                "pm10": w_data.get('pm10'),
                "co_level": w_data.get('co_level'),
                "no2_level": w_data.get('no2_level'),
                "sunrise": w_data.get('sunrise'),
                "sunset": w_data.get('sunset'),
                "moon_phase": w_data.get('moon_phase')
            }
            
            # ১. সুপাবেসে পুশ করা
            supabase_insert(record)
            
            # ২. সিএসভিতে সেভ করা
            save_to_csv(record)
            
        time.sleep(1.5) # Anti-throttle delay for stable 5-min runs

if __name__ == "__main__":
    collect()