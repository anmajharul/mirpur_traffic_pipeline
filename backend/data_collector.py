import os
import requests
import time
import logging
import csv
import math
from datetime import datetime, timezone, timedelta
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
session = requests.Session()

# ==========================================
# 📍 VALIDATED ARTERIAL CORRIDORS (UNIFIED DESTINATION)
# ==========================================
CORRIDORS = {
    "North (Mirpur-11 to 10)": {
        "origin": "23.818833,90.365443",  # Mirpur-11 Metro Station
        "dest":   "23.806932,90.368482"   # Mirpur-10 Roundabout
    },
    "South (Kazipara to 10)": {
        "origin": "23.795476,90.373516",  # Shewrapara, Begum Rokeya Ave
        "dest":   "23.806932,90.368482"   # Mirpur-10 Roundabout
    },
    "East (Mirpur-14 to 10)": {
        "origin": "23.801368,90.380476",  # Near Police Staff College, Ibrahimpur
        "dest":   "23.806932,90.368482"   # Mirpur-10 Roundabout
    },
    "West (Mirpur-1 to 10)": {
        "origin": "23.801584,90.357905",  # Mirpur Rd, Mirpur-1
        "dest":   "23.806932,90.368482"   # Mirpur-10 Roundabout
    }
}

# ==========================================
# 🧮 TIME SLOT CLASSIFICATION
# ==========================================
def classify_time_slot(bd_time):
    """Classifies current time into Dhaka's traffic flow slots."""
    hour = bd_time.hour
    if 8 <= hour < 11:
        return "Morning Peak"
    elif 11 <= hour < 16:
        return "Midday"
    elif 16 <= hour < 20:
        return "Evening Peak"
    else:
        return "Off-Peak / Night"

# ==========================================
# 🧮 3-POINT WEIGHTED FUSION ALGORITHM
# ==========================================
def calculate_fused_speed(here_speed, waze_speed, inner_speed, outer_speed):
    # Weights for API Fusion (Total = 1.0) calibrated for Dhaka
    w_here = 0.20
    w_waze = 0.30
    w_local = 0.50

    local_avg_speed = (inner_speed + outer_speed) / 2.0
    fused_speed = (here_speed * w_here) + (waze_speed * w_waze) + (local_avg_speed * w_local)
    return round(fused_speed, 2)

# ==========================================
# 📂 BACKUP LOGIC (CSV EXPORTER)
# ==========================================
def export_to_csv(table_name, filename, data):
    file_path = os.path.join("backend", filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_exists = os.path.isfile(file_path)
    try:
        with open(file_path, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists or os.stat(file_path).st_size == 0:
                writer.writeheader()
            writer.writerow(data)
    except Exception as e:
        logging.error(f"❌ CSV Export Error: {e}")

# ==========================================
# 🛰️ DATA RETRIEVAL
# ==========================================
def get_traffic_data(origin, dest):
    h_url = "https://router.hereapi.com/v8/routes"
    params = {
        "apiKey": HERE_KEY, 
        "transportMode": "car", 
        "origin": origin, 
        "destination": dest, 
        "return": "summary,travelSummary", 
        "traffic": "enabled"
    }
    try:
        h_res = session.get(h_url, params=params, timeout=12).json()
        summary = h_res["routes"][0]["sections"][0]["travelSummary"]
        return summary["baseDuration"]/60.0, summary["duration"]/60.0, summary["length"]/1000.0
    except Exception as e:
        logging.error(f"⚠️ HERE API Error: {e}")
        return None, None, None

def get_env_data():
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_KEY}&q=23.808,90.368&aqi=yes"
    try:
        r = session.get(url, timeout=10).json()
        curr = r.get("current", {})
        air = curr.get("air_quality", {})
        return {
            "temp": curr.get("temp_c"), 
            "rain": curr.get("precip_mm"), 
            "w_spd": curr.get("wind_kph"),
            "vis": curr.get("vis_km"), 
            "uv": curr.get("uv"), 
            "cond": curr.get("condition", {}).get("text"),
            "aqi": air.get("us-epa-index"), 
            "pm25": air.get("pm2_5"), 
            "pm10": air.get("pm10"),
            "co": air.get("co"), 
            "no2": air.get("no2")
        }
    except Exception as e:
        logging.error(f"⚠️ Weather API Error: {e}")
        return {}

# ==========================================
# 🧠 MAIN ENGINE (DATA-DRIVEN CONGESTION MODEL)
# ==========================================
def collect():
    logging.info("🚀 Master Congestion Modeling Pipeline Initiated...")
    env = get_env_data()
    FREE_FLOW_SPEED = 35.0  # Dhaka Arterial Standard
    
    # ⏱️ 1. TIME SLOT AUTOMATION
    bd_timezone = timezone(timedelta(hours=6))
    current_time_bd = datetime.now(bd_timezone)
    current_slot = classify_time_slot(current_time_bd)

    # 🌧️ 2. DYNAMIC WEATHER OVERRIDE & SIMULATION
    live_rain_mm = env.get("rain", 0.0) if env.get("rain") is not None else 0.0
    is_simulation_mode = False 
    effective_rain_rate = 100.0 if is_simulation_mode else live_rain_mm

    # 📉 3. BOUNDED LINEAR DECAY MODEL
    v_min_ratio = 0.65
    v_max_ratio = 1.0
    weather_penalty_factor = round(max(v_min_ratio, v_max_ratio - (0.0035 * effective_rain_rate)), 3)

    for name, coords in CORRIDORS.items():
        base, here, dist = get_traffic_data(coords["origin"], coords["dest"])
        w_min = None
        
        try:
            # Waze Route Info
            origin_lat, origin_lon = coords["origin"].split(",")
            dest_lat, dest_lon = coords["dest"].split(",")
            route = WazeRouteCalculator.WazeRouteCalculator(f"{origin_lat},{origin_lon}", f"{dest_lat},{dest_lon}", region='EU')
            w_min, _ = route.calc_route_info()
        except Exception as e:
            logging.warning(f"⚠️ Waze Error for {name}: {e}")

        if not base or not here: 
            logging.warning(f"⏭️ Skipping {name} due to missing primary data.")
            continue
            
        # ১. Raw API Speeds
        h_spd = round(dist / (here/60.0), 2)
        w_spd_kmh = round(dist / (w_min/60.0), 2) if w_min else h_spd
        
        # ২. Local Lane Speed Estimation
        inner_speed = round(w_spd_kmh * 1.15, 2)
        outer_speed = round(w_spd_kmh * 0.80, 2)
        
        # ৩. 3-Point Fusion Equation (Base Speed)
        f_spd_base = calculate_fused_speed(h_spd, w_spd_kmh, inner_speed, outer_speed)

        # 🚦 4. APPLY WEATHER PENALTY
        final_speed = round(f_spd_base * weather_penalty_factor, 2)

        # 5. TRAFFIC ENGINEERING CONGESTION METRICS
        if final_speed >= FREE_FLOW_SPEED:
            congestion_percent = 0.0
        else:
            congestion_percent = round(max(0.0, ((FREE_FLOW_SPEED - final_speed) / FREE_FLOW_SPEED) * 100), 1)

        jam_factor = round(FREE_FLOW_SPEED / final_speed, 2) if final_speed > 0 else 10.0
        s_idx = 3 if final_speed < 10 else (2 if final_speed < 15 else (1 if final_speed < 25 else 0))
        
        # Parse Geometry
        lat, lon = coords["origin"].split(",")
        
        # Final Record Generation
        record = {
            "created_at": current_time_bd.isoformat(),
            "time_slot": current_slot,
            "is_simulation": is_simulation_mode,
            "direction": name,
            "geom": f"POINT({lon.strip()} {lat.strip()})",
            "speed_kmh": final_speed,
            "here_speed": h_spd,
            "waze_speed": w_spd_kmh,
            "inner_speed": inner_speed,
            "outer_speed": outer_speed,
            "base_eta_min": round(base, 1),
            "congestion_percent": congestion_percent,
            "jam_factor": jam_factor,
            "bottleneck_ratio": round(final_speed / 25.0, 2),
            "severity_status": ["Free Flow", "Normal", "Moderate", "Critical"][s_idx],
            "severity_index": s_idx,
            "data_confidence": 0.9 if here and w_min else 0.5,
            "rain_mm": effective_rain_rate,
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
            # Sync to Supabase
            supabase.table("smart_eta_logs").insert(record).execute()
            # Sync to local CSV backup
            export_to_csv("smart_eta_logs", "traffic_data_backup.csv", record)
            logging.info(f"✅ {name} | Slot: {current_slot} | Speed: {final_speed} km/h | Rain: {effective_rain_rate}mm")
        except Exception as e:
            logging.error(f"❌ Database Insertion Error: {e}")
        
        time.sleep(2) # Anti-spam delay

if __name__ == "__main__":
    collect()