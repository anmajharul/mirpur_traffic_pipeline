import os
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
import WazeRouteCalculator
from supabase import create_client

# ==========================================
# ⚙️ LOGGING & SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('WazeRouteCalculator.WazeRouteCalculator').setLevel(logging.WARNING)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN") 
WEATHER_KEY  = os.getenv("WEATHER_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, MAPBOX_TOKEN, WEATHER_KEY]):
    logging.critical("❌ API Keys missing! Pipeline Terminated.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
session = requests.Session()

# ==========================================
# 📍 VALIDATED ARTERIAL CORRIDORS (Exact Stop-lines)
# ==========================================
CORRIDORS = {
    "North (Mirpur-11 to 10)": {
        "origin": "23.818833,90.365443",  # Mirpur-11 Metro Station
        "dest":   "23.807247,90.368658"   # Exact Stop-line (Begum Rokeya Ave, Inbound)
    },
    "South (Kazipara to 10)": {
        "origin": "23.795476,90.373516",  # Shewrapara, Begum Rokeya Ave
        "dest":   "23.806925,90.368497"   # Exact Stop-line (Begum Rokeya Ave, Inbound)
    },
    "East (Mirpur-14 to 10)": {
        "origin": "23.801368,90.380476",  # Near Police Staff College, Ibrahimpur
        "dest":   "23.807028,90.368790"   # Exact Stop-line (Mirpur-14 Approach, Inbound)
    },
    "West (Mirpur-1 to 10)": {
        "origin": "23.801584,90.357905",  # Mirpur Rd, Mirpur-1
        "dest":   "23.807144,90.368412"   # Exact Stop-line (Mirpur Rd, Inbound)
    }
}

# ==========================================
# 🧮 TIME SLOT & WEATHER LOGIC
# ==========================================
def classify_time_slot(bd_time):
    weekday = bd_time.weekday()
    hour = bd_time.hour
    if weekday == 4:
        if 12 <= hour < 14: return "Jumu'ah Prayer Peak"
        elif 16 <= hour < 21: return "Weekend Evening Peak"
        else: return "Weekend Off-Peak"
    elif weekday == 5:
        if 16 <= hour < 21: return "Weekend Evening Peak"
        else: return "Weekend Off-Peak"
    else:
        if 8 <= hour < 11: return "Morning Peak"
        elif 11 <= hour < 16: return "Midday"
        elif 16 <= hour < 20: return "Evening Peak"
        else: return "Off-Peak / Night"

def get_env_data():
    url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_KEY}&q=23.808,90.368&aqi=yes"
    try:
        r = session.get(url, timeout=10).json()
        curr = r.get("current", {})
        air = curr.get("air_quality", {})
        return {
            "temp": curr.get("temp_c"), "rain": curr.get("precip_mm"), 
            "w_spd": curr.get("wind_kph"), "vis": curr.get("vis_km"), 
            "uv": curr.get("uv"), "cond": curr.get("condition", {}).get("text"),
            "aqi": air.get("us-epa-index"), "pm25": air.get("pm2_5"), 
            "pm10": air.get("pm10"), "co": air.get("co"), "no2": air.get("no2")
        }
    except Exception as e:
        logging.error(f"⚠️ Weather API Error: {e}")
        return {}

# ==========================================
# 🧠 MAIN ENGINE (CONGESTION MODELING)
# ==========================================
FREE_FLOW_SPEED = 40.5  # Formally calibrated for Mirpur-10
MAX_DHAKA_SPEED = 45.0  # Physical ceiling

def collect():
    logging.info("🚀 Master Congestion Modeling Pipeline Initiated...")
    env = get_env_data()
    bd_time = datetime.now(timezone(timedelta(hours=6)))
    current_slot = classify_time_slot(bd_time)
    
    # Weather penalty
    live_rain_mm = env.get("rain", 0.0) if env.get("rain") is not None else 0.0
    weather_penalty_factor = round(max(0.65, 1.0 - (0.0035 * live_rain_mm)), 3)

    for name, coords in CORRIDORS.items():
        o_lat, o_lon = coords["origin"].split(",")
        d_lat, d_lon = coords["dest"].split(",")
        
        # 🛰️ 1. MAPBOX DATA FETCH
        m_min, dist_km = None, None
        mb_url = f"https://api.mapbox.com/directions/v5/mapbox/driving-traffic/{o_lon.strip()},{o_lat.strip()};{d_lon.strip()},{d_lat.strip()}"
        try:
            res = session.get(mb_url, params={"access_token": MAPBOX_TOKEN}, timeout=10).json()
            if "routes" in res and res["routes"]:
                m_min = res["routes"][0]["duration"] / 60.0
                dist_km = res["routes"][0]["distance"] / 1000.0
        except Exception as e:
            logging.error(f"⚠️ Mapbox Error for {name}: {e}")

        if not m_min or not dist_km or m_min <= 0:
            logging.warning(f"⏭️ Skipping {name} due to invalid Mapbox data.")
            continue

        # 🚙 2. WAZE DATA FETCH
        w_min = None
        try:
            route = WazeRouteCalculator.WazeRouteCalculator(coords["origin"], coords["dest"], region='EU')
            w_min, _ = route.calc_route_info()
        except Exception as e:
            logging.warning(f"⚠️ Waze Error for {name}: {e}")

        # 🚗 3. RAW SPEEDS & CROSS VALIDATION
        m_spd = round(dist_km / (m_min / 60.0), 2)
        w_spd = round(dist_km / (w_min / 60.0), 2) if w_min and w_min > 0 else m_spd

        if m_spd > MAX_DHAKA_SPEED:
            if w_min and w_spd > MAX_DHAKA_SPEED:
                logging.info(f"🌙 High-Speed Confirmed (Mapbox: {m_spd}, Waze: {w_spd}).")
            else:
                logging.warning(f"⚠️ Mapbox Glitch Detected ({m_spd} km/h). Capping...")
                m_spd = w_spd if (0 < w_spd <= MAX_DHAKA_SPEED) else MAX_DHAKA_SPEED

        # 🧠 4. DATA FUSION (Unbiased Mean) + WEATHER PENALTY
        f_spd_base = round((m_spd + w_spd) / 2.0, 2)
        final_speed = round(f_spd_base * weather_penalty_factor, 2)

        # 📈 5. DATA CONFIDENCE (Statistical Variance via RPD)
        if m_spd > 0 and w_spd > 0:
            rpd = (abs(m_spd - w_spd) / f_spd_base) * 100.0
            data_confidence = round(max(0.0, 100.0 - rpd), 2)
        else:
            data_confidence = 50.0

        # 📊 6. CORE METRICS
        jam_factor = round(max(0.0, min(10.0, ((FREE_FLOW_SPEED - final_speed) / FREE_FLOW_SPEED) * 10)), 2)
        congestion_percent = round(max(0.0, ((FREE_FLOW_SPEED - final_speed) / FREE_FLOW_SPEED) * 100), 1) if final_speed < FREE_FLOW_SPEED else 0.0
        
        s_idx = 3 if final_speed < 10 else (2 if final_speed < 15 else (1 if final_speed < 25 else 0))
        base_eta = round((dist_km / FREE_FLOW_SPEED) * 60.0, 1)

        # 📦 7. SUPABASE FULL SCHEMA INGESTION
        record = {
            "created_at": bd_time.isoformat(),
            "time_slot": current_slot,
            "is_simulation": False,
            "direction": name,
            "geom": f"POINT({o_lon.strip()} {o_lat.strip()})",
            "destination": f"POINT({d_lon.strip()} {d_lat.strip()})",
            "speed_kmh": final_speed,
            "mapbox_speed": m_spd, 
            "waze_speed": w_spd,
            "base_eta_min": base_eta,
            "congestion_percent": congestion_percent,
            "jam_factor": jam_factor,
            "bottleneck_ratio": round(final_speed / 25.0, 2),
            "severity_status": ["Free Flow", "Normal", "Moderate", "Critical"][s_idx],
            "severity_index": s_idx,
            "data_confidence": data_confidence,
            "rain_mm": live_rain_mm,
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
            logging.info(f"✅ {name} | Speed: {final_speed} km/h | Jam: {jam_factor}/10")
        except Exception as e:
            logging.error(f"❌ Database Insertion Error: {e}")
        
        time.sleep(2)

if __name__ == "__main__":
    collect()