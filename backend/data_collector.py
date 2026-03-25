# ==========================================
# 📦 SECTION 1: SYSTEM ARCHITECTURE & SETUP
# ==========================================
# লজিক: 100% Legal REST APIs, Tri-Sensor Fusion.
import os
import time
import logging
import requests
import numpy as np
import holidays
from datetime import datetime, timezone, timedelta
from supabase import create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 🔐 API Credentials (Pulled from GitHub Secrets / .env)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN") 
WEATHER_KEY  = os.getenv("WEATHER_API_KEY")
TOMTOM_KEY   = os.getenv("TOMTOM_API_KEY")
HERE_KEY     = os.getenv("HERE_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
session = requests.Session()

# 📍 Research Corridors (Mirpur-10 Arteries)
CORRIDORS = {
    "North (Mirpur-11 to 10)": {"origin": "23.818833,90.365443", "dest": "23.807247,90.368658", "name": "Mirpur-10 Circle"},
    "South (Kazipara to 10)": {"origin": "23.795476,90.373516", "dest": "23.806925,90.368497", "name": "Mirpur-10 Circle"},
    "East (Mirpur-14 to 10)": {"origin": "23.801368,90.380476", "dest": "23.807028,90.368790", "name": "Mirpur-10 Circle"},
    "West (Mirpur-1 to 10)": {"origin": "23.801584,90.357905", "dest": "23.807144,90.368412", "name": "Mirpur-10 Circle"}
}

FREE_FLOW_SPEED = 40.0

# ⚖️ Tri-Sensor Fusion Weights (Initial Empirical Baseline)
W_MB = 0.40  # Mapbox (40%)
W_TT = 0.30  # TomTom (30%)
W_HR = 0.30  # HERE (30%)

# ==========================================
# 🚇 SECTION 2: MRT-6 OPERATIONAL ENGINE
# ==========================================
def get_mrt_status(bd_time, is_holiday):
    hm = bd_time.hour * 100 + bd_time.minute
    wd = bd_time.weekday()
    status, headway = False, 0

    if wd == 4: # Friday
        if 1500 <= hm <= 2140: status, headway = True, 10
    elif wd == 5 or is_holiday: # Sat & Holidays
        if 630 <= hm <= 2210: status, headway = True, (12 if hm <= 1037 else 10)
    else: # Weekdays
        if 630 <= hm <= 2210:
            status = True
            if 811 <= hm <= 953 or 1457 <= hm <= 1741: headway = 6
            else: headway = 8
    return status, headway

# ==========================================
# 📅 SECTION 3: ENGINEERING TIME SLOTS 
# ==========================================
def get_context(bd_time, supabase):
    date_str = bd_time.strftime('%Y-%m-%d')
    is_hol = bd_time.date() in holidays.Bangladesh()
    try:
        res = supabase.table("calendar_overrides").select("*").eq("override_date", date_str).execute()
        if res.data: is_hol = res.data[0]['is_holiday']
    except: pass
    return is_hol

def classify_time_slot(bd_time):
    t = bd_time.hour * 60 + bd_time.minute
    wd = bd_time.weekday() 

    # Friday Dynamics
    if wd == 4:
        if 720 <= t <= 840: return "Jumu'ah Prayer Peak"
        elif 960 <= t <= 1260: return "Weekend Evening Peak"
        else: return "Weekend Off-Peak"
    # Saturday Dynamics
    if wd == 5:
        if 960 <= t <= 1260: return "Weekend Evening Peak"
        else: return "Weekend Off-Peak"
    # Thursday Pre-weekend Out-migration
    if wd == 3 and 960 <= t <= 1380: 
        return "Thursday Special"

    # Standard Weekday Micro-segments
    if 450 <= t <= 630: return "Morning Peak"             
    elif 631 <= t <= 750: return "Midday (Shoulder)"      
    elif 751 <= t <= 870: return "School Peak"            
    elif 871 <= t <= 990: return "Afternoon Shoulder"     
    elif 991 <= t <= 1260: return "Evening Peak"          
    else: return "Off-Peak / Night"                       

# ==========================================
# 📉 SECTION 4: STATISTICAL ANOMALY
# ==========================================
def detect_anomaly(direction, slot, current_speed):
    try:
        res = supabase.table("smart_eta_logs").select("speed_kmh").eq("direction", direction).eq("time_slot", slot).limit(30).execute()
        speeds = [row['speed_kmh'] for row in res.data]
        if len(speeds) < 15: return False, 0.0
        z_score = (current_speed - np.mean(speeds)) / np.std(speeds) if np.std(speeds) > 0 else 0
        return (z_score < -2.0), round(z_score, 2)
    except: return False, 0.0

# ==========================================
# 🛰️ SECTION 5: ENTERPRISE API DATA FETCHERS
# ==========================================
def get_mapbox_data(o_lon, o_lat, d_lon, d_lat):
    url = f"https://api.mapbox.com/directions/v5/mapbox/driving-traffic/{o_lon},{o_lat};{d_lon},{d_lat}"
    try:
        res = session.get(url, params={"access_token": MAPBOX_TOKEN}).json()
        duration_min = res["routes"][0]["duration"] / 60.0
        dist_km = res["routes"][0]["distance"] / 1000.0
        incidents = len(res.get("incidents", [])) > 0
        speed = dist_km / (duration_min / 60.0)
        return speed, duration_min, incidents
    except Exception as e:
        logging.error(f"Mapbox Error: {e}")
        return None, None, False

def get_tomtom_data(origin, dest):
    url = f"https://api.tomtom.com/routing/1/calculateRoute/{origin}:{dest}/json"
    try:
        res = session.get(url, params={"key": TOMTOM_KEY, "traffic": "true"}).json()
        duration_min = res["routes"][0]["summary"]["travelTimeInSeconds"] / 60.0
        dist_km = res["routes"][0]["summary"]["lengthInMeters"] / 1000.0
        speed = dist_km / (duration_min / 60.0)
        return speed, duration_min
    except Exception as e:
        logging.error(f"TomTom Error: {e}")
        return None, None

def get_here_data(origin, dest):
    url = f"https://router.hereapi.com/v8/routes"
    try:
        params = {
            "transportMode": "car",
            "origin": origin,
            "destination": dest,
            "return": "summary",
            "apikey": HERE_KEY
        }
        res = session.get(url, params=params).json()
        duration_min = res["routes"][0]["sections"][0]["summary"]["duration"] / 60.0
        dist_km = res["routes"][0]["sections"][0]["summary"]["length"] / 1000.0
        speed = dist_km / (duration_min / 60.0)
        return speed, duration_min
    except Exception as e:
        logging.error(f"HERE Error: {e}")
        return None, None

# ==========================================
# 🚀 SECTION 6: TRI-SENSOR FUSION & LOGGER
# ==========================================
def collect():
    logging.info("🚀 V9.0 Enterprise Tri-Fusion (Mapbox/TomTom/HERE) Initiated...")
    bd_time = datetime.now(timezone(timedelta(hours=6)))
    is_hol = get_context(bd_time, supabase)
    mrt_active, headway = get_mrt_status(bd_time, is_hol)
    slot = classify_time_slot(bd_time)

    # Weather Update
    w_url = f"https://api.weatherapi.com/v1/current.json?key={WEATHER_KEY}&q=23.808,90.368&aqi=yes"
    try: env = session.get(w_url).json().get("current", {})
    except: env = {}
    precip = env.get("precip_mm", 0.0)

    for direction, coords in CORRIDORS.items():
        o_lat, o_lon = coords["origin"].split(",")
        d_lat, d_lon = coords["dest"].split(",")

        # 1. Fetch from all 3 Enterprise APIs
        mb_spd, mb_eta, mb_incident = get_mapbox_data(o_lon, o_lat, d_lon, d_lat)
        tt_spd, tt_eta = get_tomtom_data(coords["origin"], coords["dest"])
        hr_spd, hr_eta = get_here_data(coords["origin"], coords["dest"])

        # 2. Dynamic Weight Normalization (Fault Tolerance Logic)
        active_spds, active_etas, weights = [], [], []
        if mb_spd: active_spds.append(mb_spd); active_etas.append(mb_eta); weights.append(W_MB)
        if tt_spd: active_spds.append(tt_spd); active_etas.append(tt_eta); weights.append(W_TT)
        if hr_spd: active_spds.append(hr_spd); active_etas.append(hr_eta); weights.append(W_HR)

        if not active_spds:
            logging.error(f"❌ All 3 APIs failed for {direction}")
            continue

        total_w = sum(weights)
        norm_w = [w / total_w for w in weights]

        # 3. ⚖️ Tri-Sensor Fusion Equation
        f_spd = round(sum(s * w for s, w in zip(active_spds, norm_w)), 2)
        actual_eta = round(sum(e * w for e, w in zip(active_etas, norm_w)), 1)
        
        # 4. Multi-Sensor Confidence Score
        max_spd, min_spd = max(active_spds), min(active_spds)
        speed_diff = max_spd - min_spd
        confidence = round(max(0.0, 100.0 * (1 - (speed_diff / max(max_spd, 1.0)))), 2)

        # 5. Derived Traffic Metrics
        congestion_pct = round(max(0.0, ((FREE_FLOW_SPEED - f_spd) / FREE_FLOW_SPEED) * 100), 1)
        jam_factor = round(max(0.0, min(10.0, (congestion_pct / 10))), 2)
        bottleneck_ratio = round(FREE_FLOW_SPEED / max(f_spd, 1.0), 2)
        severity = "Critical" if f_spd < 10 else ("Moderate" if f_spd < 20 else "Normal")
        
        is_anomaly, z_val = detect_anomaly(direction, slot, f_spd)
        if mb_incident: reason = "Reported Incident/Accident"
        elif slot == "Jumu'ah Prayer Peak": reason = "Friday Religious Gathering Surge"
        elif slot == "School Peak": reason = "School Shift Dispersal"
        elif not mrt_active and (700 <= (bd_time.hour*100+bd_time.minute) <= 2200): reason = "MRT Gap Impact"
        elif precip > 5.0: reason = "Rainfall & Waterlogging"
        elif is_anomaly: reason = "Statistical Outlier"
        else: reason = "Standard Pattern"

        # 📦 Final Record Payload (Matching Supabase Schema)
        record = {
            "created_at": bd_time.isoformat(),
            "day_of_week": bd_time.strftime('%A'),  
            "time_slot": slot,
            "direction": direction,
            "destination": coords["name"],          
            "geom": f"POINT({o_lon} {o_lat})",
            "speed_kmh": f_spd,
            "mapbox_speed": round(mb_spd, 2) if mb_spd else None,       
            "tomtom_speed": round(tt_spd, 2) if tt_spd else None,         
            "here_speed": round(hr_spd, 2) if hr_spd else None,         
            "actual_eta_min": actual_eta,
            "data_confidence": confidence,
            "congestion_percent": congestion_pct,
            "jam_factor": jam_factor,
            "bottleneck_ratio": bottleneck_ratio,
            "severity_status": severity,
            "incident_reported": mb_incident,
            "mrt_status": mrt_active,
            "mrt_headway": headway,
            "is_holiday": is_hol,
            "is_anomaly": is_anomaly,
            "anomaly_score": z_val,
            "reason": reason,
            "rain_mm": precip,
            "flood_risk_level": "High" if precip > 20 else ("Medium" if precip > 10 else "Low"),
            "temperature": env.get("temp_c"),
            "humidity": env.get("humidity"),        
            "wind_speed": env.get("wind_kph"),      
            "visibility_km": env.get("vis_km"),     
            "uv_index": env.get("uv"),              
            "aqi": env.get("air_quality", {}).get("us-epa-index"),
            "pm2_5": env.get("air_quality", {}).get("pm2_5"),
            "pm10": env.get("air_quality", {}).get("pm10"),
            "weather_condition": env.get("condition", {}).get("text")
        }

        try:
            supabase.table("smart_eta_logs").insert(record).execute()
            logging.info(f"✅ {direction} | MB:{round(mb_spd or 0,1)} TT:{round(tt_spd or 0,1)} HR:{round(hr_spd or 0,1)} -> Fused:{f_spd}kmh | Conf: {confidence}%")
        except Exception as e:
            logging.error(f"❌ DB Insert Error: {e}")
        time.sleep(2)

if __name__ == "__main__":
    collect()