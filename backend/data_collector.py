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

# কালেকশন পেয়ার (Inner & Outer Nodes - ৩৫০ মিটার গ্যাপ)
LOCATION_PAIRS = [
    {
        "direction": "North (Mirpur-11)",
        "inner": "23.807271,90.368554",
        "outer": "23.810272,90.368799"
    },
    {
        "direction": "South (Kazipara)",
        "inner": "23.806895,90.368611",
        "outer": "23.803984,90.368799"
    },
    {
        "direction": "East (Mirpur-14)",
        "inner": "23.807116,90.368789",
        "outer": "23.807128,90.372235"
    },
    {
        "direction": "West (Mirpur-2)",
        "inner": "23.807025,90.368412",
        "outer": "23.807128,90.365363"
    }
]

session = requests.Session()

# ==========================================
# 📊 SEVERITY CALCULATOR (Research Grade)
# ==========================================
def calculate_severity(inner_speed, outer_speed):
    """স্পিড তুলনা করে জ্যামের ভয়াবহতা এবং ইনডেক্স বের করে"""
    if inner_speed < 10 and outer_speed < 10:
        return "Critical (Queue > 350m)", 3
    elif inner_speed < 12 or outer_speed < 12:
        return "Moderate (Bottleneck)", 2
    elif inner_speed > 25 and outer_speed > 25:
        return "Free Flow", 0
    else:
        return "Normal Traffic", 1

# ==========================================
# 📁 CSV BACKUP UTILITY (Synced with Supabase)
# ==========================================
def save_to_csv(record):
    file_path = 'backend/traffic_data_backup.csv'
    file_exists = os.path.isfile(file_path)
    try:
        with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
            # ডিকশনারির কি (Keys) অনুযায়ী কলাম তৈরি হবে (অর্ডার বজায় থাকবে)
            writer = csv.DictWriter(f, fieldnames=record.keys())
            if not file_exists or os.stat(file_path).st_size == 0:
                writer.writeheader()
            writer.writerow(record)
        print(f"📁 CSV updated: {record['direction']}")
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
        curr_res = session.get(c_url, timeout=10)
        astro_res = session.get(a_url, timeout=10)
        
        curr = curr_res.json().get('current', {})
        air = curr.get('air_quality', {})
        astro = astro_res.json().get('astronomy', {}).get('astro', {})
        
        return {
            "temp_c": curr.get('temp_c'),
            "precip_mm": curr.get('precip_mm'),
            "wind_kph": curr.get('wind_kph'),
            "wind_dir": curr.get('wind_dir'),
            "vis_km": curr.get('vis_km'),
            "weather_condition": curr.get('condition', {}).get('text', "Unknown"),
            "aqi": air.get('us-epa-index'),
            "pm2_5": air.get('pm2_5'),
            "sunrise": astro.get('sunrise'),
            "sunset": astro.get('sunset'),
            "moon_phase": astro.get('moon_phase')
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
        res = session.get(url, params=params, timeout=12)
        if res.status_code == 200:
            data = res.json()
            summary = data['routes'][0]['sections'][0].get('summary', {})
            duration, length = summary.get('duration', 0), summary.get('length', 0)
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
            print(f"✅ Supabase Updated: {payload['direction']}")
        else:
            print(f"❌ DB Error: {res.status_code}")
    except Exception as e:
        print(f"❌ Connection Error: {e}")

# ==========================================
# 🧠 MAIN COLLECTION LOGIC
# ==========================================
def collect():
    print(f"🚀 Data Collection Started: {datetime.now().strftime('%H:%M:%S')}")
    weather = get_comprehensive_weather()
    now_db = datetime.now(timezone.utc).isoformat()
    
    for pair in LOCATION_PAIRS:
        i_speed = get_traffic_speed(pair["inner"])
        o_speed = get_traffic_speed(pair["outer"])
        
        if i_speed is not None and o_speed is not None:
            # ভয়াবহতা ক্যালকুলেশন
            status_text, status_idx = calculate_severity(i_speed, o_speed)
            
            # সুপাবেস এবং সিএসভি কলামের সিকোয়েন্স অনুযায়ী রেকর্ড তৈরি
            record = {
                "timestamp": now_db,
                "direction": pair["direction"],
                "speed_kmh": i_speed,
                "temperature": weather.get('temp_c'),
                "rain_mm": weather.get('precip_mm'),
                "wind_speed": weather.get('wind_kph'),
                "wind_dir": weather.get('wind_dir'),
                "visibility_km": weather.get('vis_km'), # সুপাবেসের কলাম নামের সাথে সিঙ্ক করা
                "aqi": weather.get('aqi'),
                "pm2_5": weather.get('pm2_5'),
                "weather_condition": weather.get('weather_condition', "Unknown"),
                "sunrise": weather.get('sunrise'),
                "sunset": weather.get('sunset'),
                "moon_phase": weather.get('moon_phase'),
                "inner_speed": i_speed,
                "outer_speed": o_speed,
                "severity_status": status_text,
                "severity_index": status_idx
            }
            
            # ১. সুপাবেসে পুশ
            supabase_insert(record)
            # ২. সিএসভিতে পুশ
            save_to_csv(record)
            
        time.sleep(2) # API Rate Limit Protection

if __name__ == "__main__":
    collect()