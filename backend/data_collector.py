import os
import requests
import time
import csv
from datetime import datetime, timezone

# ==========================================
# 🔑 API KEYS & CONFIG
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

CENTER_COORDS = "23.807128,90.368799"

LOCATION_PAIRS = [
    {"direction": "North (Mirpur-11)", "inner": "23.807271,90.368554", "outer": "23.810272,90.368799"},
    {"direction": "South (Kazipara)", "inner": "23.806895,90.368611", "outer": "23.803984,90.368799"},
    # মিরপুর-১৪ এর নতুন কোঅর্ডিনেট (এপিআই ফ্রেন্ডলি)
    {"direction": "East (Mirpur-14)", "inner": "23.807150,90.369300", "outer": "23.807150,90.372500"},
    {"direction": "West (Mirpur-2)", "inner": "23.807025,90.368412", "outer": "23.807128,90.365363"}
]

session = requests.Session()

def calculate_severity(inner_speed, outer_speed):
    if inner_speed < 10 and outer_speed < 10: return "Critical (Queue > 350m)", 3
    elif inner_speed < 12 or outer_speed < 12: return "Moderate (Bottleneck)", 2
    elif inner_speed > 25 and outer_speed > 25: return "Free Flow", 0
    else: return "Normal Traffic", 1

def get_comprehensive_weather():
    coords = "23.807,90.368"
    url = f"https://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={coords}&aqi=yes"
    astro_url = f"https://api.weatherapi.com/v1/astronomy.json?key={WEATHER_API_KEY}&q={coords}"
    try:
        w_res = session.get(url, timeout=10).json()
        a_res = session.get(astro_url, timeout=10).json()
        curr = w_res.get('current', {})
        air = curr.get('air_quality', {})
        astro = a_res.get('astronomy', {}).get('astro', {})
        return {
            "temp": curr.get('temp_c'), "precip": curr.get('precip_mm'),
            "w_spd": curr.get('wind_kph'), "w_dir": curr.get('wind_dir'),
            "vis": curr.get('vis_km'), "uv": curr.get('uv'),
            "cond": curr.get('condition', {}).get('text', "Unknown"),
            "aqi": air.get('us-epa-index'), "pm25": air.get('pm2_5'),
            "pm10": air.get('pm10'), "co": air.get('co'), "no2": air.get('no2'),
            "rise": astro.get('sunrise'), "set": astro.get('sunset'), "moon": astro.get('moon_phase')
        }
    except: return {}

def get_speed(origin):
    url = "https://router.hereapi.com/v8/routes"
    params = {"transportMode": "car", "origin": origin, "destination": CENTER_COORDS, "return": "summary", "apiKey": HERE_API_KEY}
    try:
        res = session.get(url, params=params, timeout=12).json()
        summary = res['routes'][0]['sections'][0].get('summary', {})
        return round((summary.get('length', 0) / summary.get('duration', 1)) * 3.6, 2)
    except: return None

def collect():
    weather = get_comprehensive_weather()
    now_db = datetime.now(timezone.utc).isoformat()
    for pair in LOCATION_PAIRS:
        i_speed = get_speed(pair["inner"])
        o_speed = get_speed(pair["outer"])
        if i_speed is not None and o_speed is not None:
            status_text, status_idx = calculate_severity(i_speed, o_speed)
            cong_pct = max(0.0, min(100.0, 100.0 - (i_speed / 40.0) * 100))
            
            record = {
                "timestamp": now_db,
                "speed_kmh": i_speed,
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
                "severity_status": status_text,
                "severity_index": status_idx
            }
            # সুপাবেস ইনসার্ট লজিক (নিশ্চিত করো তোমার supabase_insert ফাংশনটি এখানে আছে)
            url = f"{SUPABASE_URL}/rest/v1/traffic_records"
            headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
            requests.post(url, json=record, headers=headers)
            
            # CSV ব্যাকআপ
            with open('backend/traffic_data_backup.csv', 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=record.keys())
                if os.stat('backend/traffic_data_backup.csv').st_size == 0: writer.writeheader()
                writer.writerow(record)
        time.sleep(2)

if __name__ == "__main__": collect()