import os
import requests
import time
import csv
import logging
from datetime import datetime, timezone
import numpy as np # স্ট্যাটিস্টিক্যাল ক্যালকুলেশনের জন্য

# ==========================================
# ⚙️ PROFESSIONAL LOGGING (Fixes Fault 5)
# ==========================================
# সব এরর এবং ওয়ার্নিং 'traffic_pipeline.log' ফাইলে সেভ হবে অডিট ট্রেইলের জন্য
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("traffic_pipeline.log"),
        logging.StreamHandler()
    ]
)

# ==========================================
# 🔑 API KEYS & CONFIG (GitHub Secrets)
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_API_KEY = os.getenv("HERE_API_KEY", "").strip()
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "").strip()
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

# মিরপুর-১০ গোলচত্বর (Geometric Center - Pinpointed)
CENTER_COORDS = "23.8071318,90.3686089"

# চূড়ান্ত ৪টি ডিরেকশন (মিরপুর-১০ এ প্রবেশের ইনফ্লো রুট)
LOCATION_PAIRS = [
    {"direction": "North (Mirpur-11 to 10)", "inner": "23.807220,90.368727", "outer": "23.818833,90.365443"},
    {"direction": "South (Kazipara to 10)", "inner": "23.806750,90.368750", "outer": "23.803850,90.368800"},
    {"direction": "East (Mirpur-14 to 10)", "inner": "23.806954,90.368737", "outer": "23.798539,90.386764"},
    {"direction": "West (Mirpur-1 to 10)", "inner": "23.807086,90.368372", "outer": "23.801584,90.357905"}
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
# 🛰️ DATA RETRIEVAL & FALLBACK (Fixes Fault 1)
# ==========================================
def get_historical_moving_avg(direction):
    """
    এপিআই ফেইল করলে ফিক্সড ১৫/২০ না বসিয়ে গত ৫টি রেকর্ডের গড় নিয়ে আসা।
    একে সায়েন্টিফিক্যালি 'Historical Imputation' বলা হয়।
    """
    url = f"{SUPABASE_URL}/rest/v1/traffic_records?direction=eq.{direction}&select=speed_kmh&order=timestamp.desc&limit=5"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    try:
        res = session.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
        if data:
            speeds = [item['speed_kmh'] for item in data if item['speed_kmh'] is not None]
            if speeds:
                return round(np.mean(speeds), 2)
    except Exception as e:
        logging.error(f"Fallback DB Lookup Error for {direction}: {e}")
    
    return 18.5 # একদম শেষ ভরসা (ঢাকার গড় স্পিড বেসলাইন)

def get_here_speed_data(origin):
    """HERE API থেকে স্পিড এবং কনফিডেন্স প্রক্সি ক্যালকুলেট করা (Fixes Fault 2 & 3)"""
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
                spd = round((length / duration) * 3.6, 2)
                # HERE-এর নিজস্ব কনফিডেন্স ইনডেক্স নেই, তাই স্পিড রেশিও দিয়ে একটি কনফিডেন্স প্রক্সি বানাচ্ছি
                # জাম ফ্যাক্টর বেশি থাকলে (গতি কমলে) কনফিডেন্স কিছুটা কমে কারণ ডেটা প্রোব কম থাকে
                confidence = 0.9 if spd > 12 else 0.75
                return spd, confidence
    except Exception as e:
        logging.error(f"HERE API Connection Error: {e}")
    return None, 0

def get_tomtom_data(origin):
    """TomTom Flow API থেকে স্পিড এবং কনফিডেন্স স্কোর আনা"""
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={TOMTOM_API_KEY}&point={origin}"
    try:
        res = session.get(url, timeout=12).json()
        flow = res.get('flowSegmentData', {})
        return flow.get('currentSpeed'), flow.get('confidence', 0.5)
    except Exception as e:
        logging.error(f"TomTom API Connection Error: {e}")
        return None, 0

# ==========================================
# 🧠 SMART FUSION & KINEMATIC FILTER (Fixes Fault 2 & 4)
# ==========================================
def fuse_and_filter(h_spd, h_conf, tt_spd, tt_conf, last_spd):
    """Weighted Average Fusion এবং সায়েন্টিফিক আউটলায়ার ডিটেকশন"""
    
    # ১. Dynamic Weighted Fusion (Fixes Fault 2)
    if h_spd is not None and tt_spd is not None:
        # HERE এবং TomTom উভয়ের কনফিডেন্স ব্যবহার করে ওয়েটেড এভারেজ
        fused = round(((h_spd * h_conf) + (tt_spd * tt_conf)) / (h_conf + tt_conf), 2)
    else:
        # যদি যেকোনো একটি ফেইল করে, তবে যেটা আছে সেটা নাও, অথবা হিস্টোরিক্যাল গড় নাও
        fused = h_spd or tt_spd or last_spd

    # ২. Kinematic Continuity Filter (Fixes Fault 4)
    # ২ মিনিটের কালেকশন গ্যাপে স্পিড ২০ কিমি/ঘণ্টার বেশি পরিবর্তন হওয়া ঢাকার ট্রাফিকে অস্বাভাবিক (Kinematics)
    max_delta = 20.0
    if abs(fused - last_spd) > max_delta:
        logging.warning(f"Outlier detected for {last_spd} -> {fused}. Smoothing logic applied.")
        # হার্ডক্যাপ না দিয়ে একটি 'Weighted Smoothing' (70/30) করা হয়েছে যাতে রিয়েল মুভমেন্ট থাকে
        fused = round((last_spd * 0.7) + (fused * 0.3), 2)
        
    return fused

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
    except Exception as e:
        logging.error(f"Weather Fetch Error: {e}")
        return {}

# ==========================================
# 📁 STORAGE UTILITIES
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
        logging.error(f"⚠️ CSV Backup Failure: {e}")

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
            logging.info(f"✅ Supabase: {payload['direction']} Data Synced")
        else:
            logging.error(f"❌ DB Error {res.status_code}: {res.text}")
    except Exception as e:
        logging.error(f"❌ DB Connection Critical Failure: {e}")

# ==========================================
# 🧠 MAIN COLLECTION LOGIC
# ==========================================
def collect():
    logging.info("🚀 Advanced Traffic Research Pipeline Started...")
    weather = get_comprehensive_weather()
    now_db = datetime.now(timezone.utc).isoformat()
    
    # মিরপুর-১০ এর জন্য রিসার্চ বেসলাইন ফ্রি-ফ্লো স্পিড ৩৫ কিমি/ঘণ্টা
    FREE_FLOW_SPEED = 35.0

    for pair in LOCATION_PAIRS:
        # ১. হিস্টোরিক্যাল মুভিং এভারেজ আনো (Fault 1 Fix)
        last_spd_avg = get_historical_moving_avg(pair["direction"])
        
        # ২. মাল্টি-সোর্স ডেটা ফেচ (Fault 2 Fix)
        h_spd, h_conf = get_here_speed_data(pair["inner"]) # Segment/Link speed proxy
        tt_spd, tt_conf = get_tomtom_data(pair["inner"])
        
        # ৩. সায়েন্টিফিক ফিউশন এবং আউটলায়ার ফিল্টারিং (Fault 2 & 4 Fix)
        fused_speed = fuse_and_filter(h_spd, h_conf, tt_spd, tt_conf, last_spd_avg)
        
        # ৪. আউটার পয়েন্ট ডেটা (বটলনেক অ্যানালাইসিসের জন্য)
        o_speed_raw, _ = get_here_speed_data(pair["outer"])
        o_speed = o_speed_raw or 20.0
        
        # ৫. ক্যালকুলেশনস
        status_text, status_idx = calculate_severity(fused_speed, o_speed)
        cong_pct = max(0.0, min(100.0, 100.0 - (fused_speed / FREE_FLOW_SPEED) * 100))
        bottleneck_ratio = round(fused_speed / o_speed, 2) if o_speed > 0 else 1.0

        record = {
            "timestamp": now_db,
            "direction": pair["direction"],
            "speed_kmh": fused_speed,       # Primary Average Speed
            "here_speed": h_spd,            # Source Audit
            "tomtom_speed": tt_spd,         # Source Audit
            "congestion_percent": round(cong_pct, 1),
            "rain_mm": weather.get('precip'),
            "temperature": weather.get('temp'),
            "destination": "Mirpur-10 Circle",
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
            "inner_speed": fused_speed,
            "outer_speed": o_speed,
            "bottleneck_ratio": bottleneck_ratio,
            "severity_status": status_text,
            "severity_index": status_idx,
            "data_confidence": round((h_conf + tt_conf) / 2, 2) # New audit column
        }
        
        # ৬. সিংক্রোনাইজেশন
        supabase_insert(record)
        save_to_csv(record)
        
        time.sleep(2) # এপিআই রেট লিমিট রক্ষা করতে

if __name__ == "__main__":
    try:
        collect()
    except KeyboardInterrupt:
        logging.info("Collection stopped by user.")
    except Exception as e:
        logging.critical(f"Pipeline crashed: {e}")