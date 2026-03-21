import os
import requests
import time
import logging
import csv
from datetime import datetime, timezone, timedelta
import numpy as np
import WazeRouteCalculator
from supabase import create_client, Client

# ==========================================
# ⚙️ LOGGING & AUDIT TRAIL
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("traffic_pipeline.log"), logging.StreamHandler()]
)
logging.getLogger('WazeRouteCalculator.WazeRouteCalculator').setLevel(logging.WARNING)

# ==========================================
# 🔑 CREDENTIALS & SYSTEM SETUP
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HERE_KEY     = os.getenv("HERE_API_KEY")
WEATHER_KEY  = os.getenv("WEATHER_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, HERE_KEY, WEATHER_KEY]):
    logging.critical("❌ API Keys missing! Pipeline Terminated.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BD_TZ = timezone(timedelta(hours=6))
session = requests.Session()

CORRIDORS = {
    "North (Mirpur-11 to 10)": {"origin": "23.818965,90.365406", "dest": "23.809450,90.368200"},
    "South (Kazipara to 10)": {"origin": "23.799301,90.371857", "dest": "23.808400,90.368250"},
    "East (Mirpur-14 to 10)": {"origin": "23.798457,90.386959", "dest": "23.808940,90.368850"},
    "West (Mirpur-1 to 10)":  {"origin": "23.802838,90.359814", "dest": "23.808900,90.367600"}
}

# ==========================================
# 📂 BACKUP LOGIC (CSV EXPORTER)
# ==========================================
def export_to_csv(table_name, filename, data):
    """
    ডেটা ব্যাকআপ নেওয়ার জন্য সায়েন্টিফিক মেথড। 
    এটি ফাইল না থাকলে হেডারসহ তৈরি করবে, আর থাকলে অ্যাপেন্ড (Append) করবে।
    """
    file_path = os.path.join("backend", filename)
    file_exists = os.path.isfile(file_path)
    
    try:
        with open(file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists or os.stat(file_path).st_size == 0:
                writer.writeheader()
            writer.writerow(data)
    except Exception as e:
        logging.error(f"❌ CSV Export Error for {table_name}: {e}")

def backup_ml_weights():
    """
    ml_weights টেবিলটি ছোট (মাত্র ৪টি রো), তাই এটি প্রতিবার ফ্রেশ ওভাররাইট করা হয় 
    যাতে গিটহাবে লেটেস্ট AI Weights থাকে।
    """
    try:
        res = supabase.table("ml_weights").select("*").execute()
        if res.data:
            file_path = os.path.join("backend", "ml_weights_backup.csv")
            with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=res.data[0].keys())
                writer.writeheader()
                writer.writerows(res.data)
            logging.info("✅ ML Weights Backup Completed.")
    except Exception as e:
        logging.error(f"❌ ML Weights Backup Failed: {e}")

# ==========================================
# 🛰️ DATA RETRIEVAL
# ==========================================
def get_traffic_data(origin, dest):
    h_url = "https://router.hereapi.com/v8/routes"
    params = {"apiKey": HERE_KEY, "transportMode": "car", "origin": origin, "destination": dest, "return": "summary,travelSummary", "traffic": "enabled"}
    try:
        h_res = session.get(h_url, params=params, timeout=12).json()
        summary = h_res["routes"][0]["sections"][0]["travelSummary"]
        return summary["baseDuration"]/60.0, summary["duration"]/60.0, summary["length"]/1000.0
    except: return None, None, None

def get_env_data():
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_KEY}&q=23.808,90.368&aqi=yes"
    try:
        r = session.get(url, timeout=10).json()
        curr, air = r.get("current", {}), r.get("current", {}).get("air_quality", {})
        return {
            "temp": curr.get("temp_c"), "rain": curr.get("precip_mm"), "w_spd": curr.get("wind_kph"),
            "vis": curr.get("vis_km"), "uv": curr.get("uv"), "cond": curr.get("condition", {}).get("text"),
            "aqi": air.get("us-epa-index"), "pm25": air.get("pm2_5"), "pm10": air.get("pm10"),
            "co": air.get("co"), "no2": air.get("no2")
        }
    except: return {}

# ==========================================
# 🧠 MAIN ENGINE
# ==========================================
def collect():
    logging.info("🚀 Master Smart Mobility Pipeline Initiated...")
    env = get_env_data()
    now_dt = datetime.now(BD_TZ)
    
    for name, coords in CORRIDORS.items():
        base, here, waze_min, dist = None, None, None, None
        try:
            base, here, dist = get_traffic_data(coords["origin"], coords["dest"])
            route = WazeRouteCalculator.WazeRouteCalculator(coords["origin"], coords["dest"], region='EU')
            waze_min, _ = route.calc_route_info()
        except: pass

        if not base or (not here and not waze_min): continue
            
        f_min = (0.4 * here) + (0.6 * waze_min) if here and waze_min else (here or waze_min)
        f_spd = round(dist / (f_min/60.0), 2)
        h_spd = round(dist / (here/60.0), 2) if here else 0.0
        jam_factor = round(f_min / base, 2)
        s_idx = 3 if f_spd < 10 else (2 if f_spd < 15 else (1 if f_spd < 25 else 0))
        
        lat, lon = coords["origin"].split(",")
        
        record = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "direction": name,
            "geom": f"POINT({lon.strip()} {lat.strip()})",
            "speed_kmh": f_spd,
            "here_speed": h_spd,
            "waze_speed": round(dist / (waze_min/60.0), 2) if waze_min else 0.0,
            "inner_speed": f_spd,
            "outer_speed": round(h_spd * 1.1, 2),
            "base_eta_min": round(base, 1),
            "congestion_percent": max(0.0, round((1 - (1/jam_factor)) * 100, 1)),
            "jam_factor": jam_factor,
            "bottleneck_ratio": round(f_spd / 25.0, 2),
            "severity_status": ["Free Flow", "Normal", "Moderate", "Critical"][s_idx],
            "severity_index": s_idx,
            "data_confidence": 0.9 if here and waze_min else 0.5,
            "rain_mm": env.get("rain", 0.0),
            "temperature": env.get("temp", 0.0),
            "wind_speed": env.get("w_spd", 0.0),
            "visibility_km": env.get("vis", 0.0),
            "uv_index": env.get("uv", 0.0),
            "weather_condition": env.get("cond", "Unknown"),
            "aqi": env.get("aqi"),
            "pm2_5": env.get("pm25"),
            "pm10": env.get("pm10"),
            "co_level": env.get("co"),
            "no2_level": env.get("no2")
        }

        try:
            # ১. সুপাবেসে ইনসার্ট
            supabase.table("smart_eta_logs").insert(record).execute()
            # ২. সিএসভি ব্যাকআপে অ্যাপেন্ড (Incremental Backup)
            export_to_csv("smart_eta_logs", "traffic_data_backup.csv", record)
            logging.info(f"✅ Synced & Backed Up: {name}")
        except Exception as e:
            logging.error(f"❌ Process Error: {e}")
        
        time.sleep(2)

    # ৩. ML Weights ব্যাকআপ (Full Overwrite to keep latest)
    backup_ml_weights()

if __name__ == "__main__":
    collect()