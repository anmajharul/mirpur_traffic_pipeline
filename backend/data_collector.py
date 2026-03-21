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

# মিরপুর-১০ গোলচত্বর (Geometric Center)
CENTER_COORDS = "23.807128,90.368799"

# চূড়ান্ত ৪টি ডিরেকশন (মিরপুর-১০ এ প্রবেশের ইনफ्লো রুট)
LOCATION_PAIRS = [
    {
        "direction": "North (Mirpur-11 to 10)", 
        "inner": "23.807220,90.368727", 
        "outer": "23.818833,90.365443" 
    },
    {
        "direction": "South (Kazipara to 10)", 
        "inner": "23.806750,90.368750", 
        "outer": "23.803850,90.368800" 
    },
    {
        "direction": "East (Mirpur-14 to 10)", 
        "inner": "23.806954,90.368737", 
        "outer": "23.798539,90.386764" 
    },
    {
        "direction": "West (Mirpur-1 to 10)", 
        "inner": "23.807086,90.368372", 
        "outer": "23.801584,90.357905" 
    }
]

session = requests.Session()

# ==========================================
# 📊 SEVERITY CALCULATOR
# ==========================================
def calculate_severity(inner_speed, outer_speed):
    """ইনার এবং আউটার স্পিড তুলনা করে জ্যামের ভয়াবহতা নির্ণয়"""
    if inner_speed < 10 and outer_speed < 10:
        return "Critical (Queue > 350m)", 3
    elif inner_speed < 12 or outer_speed < 12:
        return "Moderate (Bottleneck)", 2
    elif inner_speed > 25 and outer_speed > 25:
        return "Free Flow", 0
    else:
        return "Normal Traffic", 1

# ==========================================
# 📁 CSV BACKUP UTILITY (Academic Standard)
# ==========================================
def save_to_csv(record):
    file_path = 'backend/traffic_data_backup.csv'
    file_exists = os.path.isfile(file_path)
    try:
        with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=record.keys())
            if not file_exists or os.stat(file_path).st_size == 0:
                writer.writeheader()
            writer.writerow(record)
    except Exception as e:
        print(f"⚠️ CSV backup failed: {e}")

# ==========================================
# 🌤️ WEATHER, AQI & ASTRONOMY FETCH
# ==========================================
def get_comprehensive_weather():
    coords = "23.807,90.368"
    c_url = f"https://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={coords}&aqi=yes"
    a_url = f"https://api.weatherapi.com/v1/astronomy.json?key={WEATHER_API_KEY}&q={coords}"
    try:
        curr_res = session.get(c_url, timeout=10).json()
        astro_res = session.get(a_url, timeout=10).json()
        
        curr = curr_res.get('current', {})
        air = curr.get('air_quality', {})
        astro = astro_res.get('astronomy', {}).get('astro', {})
        
        return {
            "temp": curr.get('temp_c'),
            "precip": curr.get('precip_mm'),
            "w_spd": curr.get('wind_kph'),
            "w_dir": curr.get('wind_dir'),
            "vis": curr.get('vis_km'),
            "uv": curr.get('uv'),
            "cond": curr.get('condition', {}).get('text', "Unknown"),
            "aqi": air.get('us-epa-index'),
            "pm25": air.get('pm2_5'),
            "pm10": air.get('pm10'),
            "co": air.get('co'),
            "no2": air.get('no2'),
            "rise": astro.get('sunrise'),
            "set": astro.get('sunset'),
            "moon": astro.get('moon_phase')
        }
    except Exception:
        return {}

# ==========================================
# 🚗 TRAFFIC SPEED FETCH (HERE API)
# ==========================================
def get_traffic_speed(origin):
    url = "https://router.hereapi.com/v8/routes"
    params = {
        "transportMode": "car",
        "origin": origin,
        "destination": CENTER_COORDS,
        "return": "summary",
        "routingMode": "fast",
        "apiKey": HERE_API_KEY
    }
    try:
        res = session.get(url, params=params, timeout=12).json()
        if 'routes' in res and len(res['routes']) > 0:
            summary = res['routes'][0]['sections'][0].get('summary', {})
            duration = summary.get('duration', 0)
            length = summary.get('length', 0)
            if duration > 0:
                return round((length / duration) * 3.6, 2)
    except Exception:
        pass
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
            print(f"✅ Supabase: {payload['direction']} Updated")
        else:
            print(f"❌ DB Error: {res.status_code}")
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")

# ==========================================
# 🧠 MAIN COLLECTION LOGIC
# ==========================================
def collect():
    print(f"🚀 Data Collection Started: {datetime.now().strftime('%H:%M:%S')}")
    weather = get_comprehensive_weather()
    now_db = datetime.now(timezone.utc).isoformat()
    
    FREE_FLOW_SPEED = 40.0

    for pair in LOCATION_PAIRS:
        i_speed = get_traffic_speed(pair["inner"])
        o_speed = get_traffic_speed(pair["outer"])
        
        if i_speed is not None and o_speed is not None:
            # severity লজিক
            status_text, status_idx = calculate_severity(i_speed, o_speed)

            # Dashboard speed: intersection congestion shows inner speed
            dashboard_speed = i_speed

            # Academic congestion formula (baseline 40 km/h)
            cong_pct = max(0.0, min(100.0, 100.0 - (dashboard_speed / FREE_FLOW_SPEED) * 100))

            # Queue/Bottleneck ratio for research
            bottleneck_ratio = round(i_speed / o_speed, 2) if o_speed > 0 else 1.0

            record = {
                "timestamp": now_db,
                "speed_kmh": dashboard_speed,
                "congestion_percent": round(cong_pct, 1),
                "rain_mm": weather.get('precip'),
                "temperature": weather.get('temp'),
                "destination": "Mirpur-10 Circle",
                "direction": pair["direction"],
                "wind_speed": weather.get('w_spd'),
                "visibility_km": weather.get('vis'),
                "uv_index": weather.get('uv'),
                "wind_dir": weather.get('w_dir'),
                "weather_condition": weather.get('cond'),
                "aqi": weather.get('aqi'),
                "pm2_5": weather.get('pm25'),
                "pm10": weather.get('pm10'),
                "co_level": weather.get('co'),
                "no2_level": weather.get('no2'),
                "sunrise": weather.get('rise'),
                "sunset": weather.get('set'),
                "moon_phase": weather.get('moon'),
                "inner_speed": i_speed,
                "outer_speed": o_speed,
                "bottleneck_ratio": bottleneck_ratio,
                "severity_status": status_text,
                "severity_index": status_idx
            }
            
            supabase_insert(record)
            save_to_csv(record)
            
        time.sleep(2) 

if __name__ == "__main__":
    collect()
