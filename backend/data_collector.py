import os
import requests
import time
import logging
import csv
import math  # গাণিতিক ক্যালকুলেশনের জন্য প্রয়োজনীয়
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
# 🧠 MAIN ENGINE (MNL INTEGRATED)
# ==========================================
def collect():
    logging.info("🚀 Master Smart Mobility Pipeline Initiated...")
    env = get_env_data()
    
    # --- DCM CONSTANTS (BUBT Student Survey Model) ---
    DIST_KM = 5.0           # Fixed commute distance for analysis
    COST_BUS = 20.0         # Estimated fare in BDT
    COST_METRO = 40.0       # Estimated fare in BDT
    TIME_METRO_FIXED = 10.0 # Consistent Metro reliability (min)
    STUDENT_DUMMY = 1       # Context: BUBT Student Population
    FREE_FLOW_SPEED = 35.0  # Standard free-flow speed for Mirpur-10
    
    for name, coords in CORRIDORS.items():
        base, here, waze_min, dist = None, None, None, None
        try:
            base, here, dist = get_traffic_data(coords["origin"], coords["dest"])
            route = WazeRouteCalculator.WazeRouteCalculator(coords["origin"], coords["dest"], region='EU')
            waze_min, _ = route.calc_route_info()
        except: pass

        if not base or (not here and not waze_min): continue
            
        f_min = (0.4 * here) + (0.6 * waze_min) if here and waze_min else (here or waze_min)
        
        # স্পিড ক্যালকুলেশন
        f_spd = round(dist / (f_min/60.0), 2)  # Fused Speed (Main Speed)
        h_spd = round(dist / (here/60.0), 2) if here else 0.0
        w_spd_kmh = round(dist / (waze_min/60.0), 2) if waze_min else 0.0

        # ==========================================
        # 📈 DISCRETE CHOICE MODEL (MNL) IMPLEMENTATION
        # ==========================================
        # ১. বাসের জন্য রিয়েল-টাইম ট্রাভেল টাইম ক্যালকুলেশন
        bus_travel_time = round((DIST_KM / f_spd) * 60, 2) if f_spd > 0 else 999
        
        # ২. ইউটিলিটি ফাংশন ক্যালকুলেশন (MNL Theory)
        u_bus = -0.5 + (-0.05 * bus_travel_time) + (-0.01 * COST_BUS)
        u_metro = -0.01 + (-0.02 * TIME_METRO_FIXED) + (-0.03 * COST_METRO) + (0.5 * STUDENT_DUMMY)
        
        # ৩. লগিট প্রোবাবিলিটি ক্যালকুলেশন
        try:
            exp_u_bus = math.exp(u_bus)
            exp_u_metro = math.exp(u_metro)
            p_metro = exp_u_metro / (exp_u_metro + exp_u_bus)
            prob_metro = round(p_metro * 100, 1)
            prob_bus = round((1 - p_metro) * 100, 1)
        except OverflowError:
            prob_metro = 100.0 if u_metro > u_bus else 0.0
            prob_bus = 100.0 - prob_metro

        # ==========================================
        # 🚦 TRAFFIC ENGINEERING LOGIC
        # ==========================================
        if f_spd >= FREE_FLOW_SPEED:
            congestion_percent = 0.0
        else:
            congestion_percent = round(max(0.0, ((FREE_FLOW_SPEED - f_spd) / FREE_FLOW_SPEED) * 100), 1)

        jam_factor = round(FREE_FLOW_SPEED / f_spd, 2) if f_spd > 0 else 10.0
        s_idx = 3 if f_spd < 10 else (2 if f_spd < 15 else (1 if f_spd < 25 else 0))
        
        lat, lon = coords["origin"].split(",")
        
        # Final Record Generation
        record = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "direction": name,
            "geom": f"POINT({lon.strip()} {lat.strip()})",
            "speed_kmh": f_spd,
            "here_speed": h_spd,
            "waze_speed": w_spd_kmh,
            "inner_speed": f_spd,
            "outer_speed": round(h_spd * 1.1, 2),
            "base_eta_min": round(base, 1),
            "congestion_percent": congestion_percent,
            "prob_metro": prob_metro,      # ✅ LIVE Choice
            "prob_bus": prob_bus,          # ✅ LIVE Choice
            "utility_bus": round(u_bus, 3), # ✅ For Research Trace
            "utility_metro": round(u_metro, 3),
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
            supabase.table("smart_eta_logs").insert(record).execute()
            export_to_csv("smart_eta_logs", "traffic_data_backup.csv", record)
            logging.info(f"✅ Synced: {name} | Metro: {prob_metro}% vs Bus: {prob_bus}% (Speed: {f_spd} km/h)")
        except Exception as e:
            logging.error(f"❌ Record Process Error: {e}")
        
        time.sleep(2)

    backup_ml_weights()

if __name__ == "__main__":
    collect()