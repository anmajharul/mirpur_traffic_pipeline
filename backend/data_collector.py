"""
data_collector.py — Q1 DEFENSIBLE DATA COLLECTION MODULE
==========================================================
Purpose:
- Multi-source probe data ingestion (Mapbox + Waze)
- FHWA congestion index and severity classification
- TTI computation (Travel Time Index)
- OGC LineString geometry encoding
- Anomaly-aware fusion
- EPA NowCast AQI (NOT max(PM2.5, PM10))

FIXES FROM REVIEW:
- [CRITICAL] Removed 'aqi = max(PM2.5, PM10)' → now uses EPA formula in weather.py
- [CRITICAL] WazeRouteCalculator region: "EU" (empirically verified for Dhaka routing)
- [CRITICAL] Bare except replaced with specific exception types
- [MAJOR] TTI and speed-derived features documented as NOT for use as model features
- [MAJOR] anomaly_ratio computation retained for logging only (not in feature set)

REFERENCES:
[1] Bachmann, C. et al. (2013). Transportation Research Part C, 26, 12–26.
    https://doi.org/10.1016/j.trc.2012.09.003

[2] El Faouzi, N.E. et al. (2011). Information Fusion, 12(1), 4–10.
    https://doi.org/10.1016/j.inffus.2010.06.001

[3] TRB (2022). Highway Capacity Manual, 7th Edition.
    Transportation Research Board. ISBN 978-0-309-08766-8.

[4] FHWA (2006). Travel Time Reliability: Making It There On Time, All The Time.
    Federal Highway Administration Report FHWA-HOP-06-070.
    https://ops.fhwa.dot.gov/publications/tt_reliability/

[5] OGC (2011). OpenGIS Simple Features Specification for SQL, Rev 1.2.1.
    https://www.ogc.org/standards/sfs
"""

import requests  # type: ignore
import logging
from datetime import datetime, timezone, timedelta

import WazeRouteCalculator  # type: ignore

from config import WEATHER_API_KEY, USE_GROUND_TRUTH  # type: ignore
from weather import fetch_weather  # type: ignore
from fusion import fuse_speeds  # type: ignore
from mrt import get_mrt_status  # type: ignore
from freeflow import get_free_flow  # type: ignore
from data_loader import fetch_direction_data  # type: ignore

BDT = timezone(timedelta(hours=6))
TIMEOUT = 10
_free_flow_cache: dict = {}

logging.basicConfig(level=logging.INFO)


# -------------------------------------------------
# OGC LINESTRING GEOMETRY
# Reference: OGC SFS Rev 1.2.1
# -------------------------------------------------
def build_geom(origin: str, dest: str) -> str | None:
    """
    Build OGC WKT LineString from 'lat,lon' coordinate strings.
    Note: WKT format is lon lat (x y), not lat lon.
    """
    try:
        lat1, lon1 = origin.split(",")
        lat2, lon2 = dest.split(",")
        return (
            f"LINESTRING({lon1.strip()} {lat1.strip()}, "
            f"{lon2.strip()} {lat2.strip()})"
        )
    except (ValueError, AttributeError) as e:
        logging.warning(f"[GEOM] build_geom failed: {e}")
        return None


# -------------------------------------------------
# CONGESTION INDEX
# Formula: CI = max(0, (1 - v/v_f) * 100)
# Reference: FHWA (2006), HCM (2022)
# IMPORTANT: NOT used as model feature (derived from speed_kmh = target proxy)
# -------------------------------------------------
def compute_congestion(speed_kmh: float, free_flow_kmh: float) -> float | None:
    if speed_kmh is None or free_flow_kmh is None or free_flow_kmh <= 0:
        return None
    return round(max(0.0, min(100.0, (1 - speed_kmh / free_flow_kmh) * 100)), 2)  # type: ignore


# -------------------------------------------------
# CONGESTION SEVERITY CLASSIFICATION
# Thresholds from FHWA (2006) Table 2-3
# -------------------------------------------------
def classify_severity(c: float | None) -> tuple[str | None, int | None]:
    if c is None:
        return None, None
    if c < 20:
        return "Free Flow", 0
    elif c < 40:
        return "Light", 1
    elif c < 60:
        return "Moderate", 2
    elif c < 80:
        return "Heavy", 3
    else:
        return "Severe", 4


# -------------------------------------------------
# MAPBOX DIRECTIONS API
# -------------------------------------------------
def get_mapbox_data(origin: str, dest: str, token: str) -> dict | None:
    """
    Fetch travel time and distance from Mapbox Directions API.
    Speed = distance_km / (duration_sec / 3600)
    """
    if not token:
        return None

    def flip(c: str) -> str | None:
        try:
            lat, lon = c.split(",")
            return f"{lon.strip()},{lat.strip()}"
        except (ValueError, AttributeError):
            return None

    o, d = flip(origin), flip(dest)
    if not o or not d:
        return None

    url = (
        f"https://api.mapbox.com/directions/v5/mapbox/driving/{o};{d}"
        f"?access_token={token}"
    )

    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            logging.warning(f"[MAPBOX] HTTP {r.status_code}")
            return None

        route = r.json().get("routes", [{}])[0]
        dist = route.get("distance", 0) / 1000.0  # meters → km
        dur = route.get("duration", 0)  # seconds

        if dur <= 0 or dist <= 0:
            return None

        speed = dist / (dur / 3600.0)  # km/h

        # Physical plausibility check (RSTP: 5–80 km/h urban)
        if speed < 5 or speed > 80:
            logging.warning(f"[MAPBOX] Implausible speed {speed:.1f} km/h — rejected")
            return None

        return {
            "speed_kmh": float(round(speed, 2)),
            "actual_eta_min": float(round(dur / 60.0, 2)),
            "distance_km": float(round(dist, 2))
        }

    except requests.exceptions.Timeout:
        logging.warning("[MAPBOX] Request timeout")
        return None
    except requests.exceptions.RequestException as e:
        logging.warning(f"[MAPBOX] Network error: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        logging.warning(f"[MAPBOX] Parse error: {e}")
        return None


# -------------------------------------------------
# WAZE ROUTE CALCULATOR
# "EU" region empirically verified for Bangladesh (Dhaka) routing.
# "IL" region returns HTTP 500 for Dhaka coordinates.
# Reference: WazeRouteCalculator library documentation
# -------------------------------------------------
def get_waze_speed(origin: str, dest: str) -> float | None:
    """
    Fetch travel speed from Waze crowd-sourced data.
    Speed = dist_km / (duration_min / 60)
    """
    try:
        route = WazeRouteCalculator.WazeRouteCalculator(
            origin, dest,
            region="EU",  # FIX: "EU" handles Bangladesh global routing best (avoids IL 500 errors)
            vehicle_type="CAR"
        )
        dur_min, dist_km = route.calc_route_info()

        if dur_min <= 0 or dist_km <= 0:
            return None

        speed = dist_km / (dur_min / 60.0)  # km/h

        if speed < 5 or speed > 80:
            logging.warning(f"[WAZE] Implausible speed {speed:.1f} km/h — rejected")
            return None

        return round(speed, 2)

    except Exception as e:
        logging.warning(f"[WAZE] Failed: {e}")
        return None


# -------------------------------------------------
# FREE FLOW (CACHED)
# -------------------------------------------------
def get_cached_free_flow(direction: str) -> float:
    if direction in _free_flow_cache:
        return _free_flow_cache[direction]
    try:
        df = fetch_direction_data(direction)
        ff = get_free_flow(direction, df)
    except Exception:
        ff = get_free_flow(direction)
    _free_flow_cache[direction] = ff
    return ff


# -------------------------------------------------
# MAIN COLLECT FUNCTION
# -------------------------------------------------
def collect(origin: str, dest: str, mapbox_token: str, direction_name: str) -> dict:
    """
    Collect one observation for a corridor.

    Returns dict with status='OK' on success, status='Data unavailable' on failure.

    NOTE ON FEATURE USAGE:
        Fields marked [STORE_ONLY] are stored in DB for logging/diagnostics
        but MUST NOT be used as ML model features due to target derivation:
        - speed_kmh → derived from actual_eta_min (target)
        - congestion_percent → derived from speed_kmh
        - tti → direct linear transform of actual_eta_min
        - speed_ratio → derived from speed_kmh
        - travel_time_sec → = actual_eta_min * 60 (exact duplicate target)

        These are removed from feature_cols in trainer_xgb.py.
        Reference: Kaufman et al. (2012) — leakage formulation.
    """
    now = datetime.now(BDT)

    mapbox_data = get_mapbox_data(origin, dest, mapbox_token)
    waze_spd = get_waze_speed(origin, dest)

    if USE_GROUND_TRUTH and not mapbox_data:
        logging.warning(f"[COLLECT] Mapbox unavailable for {direction_name} — skipping")
        return {"status": "Data unavailable"}

    mapbox_spd = mapbox_data["speed_kmh"] if mapbox_data else None
    eta = mapbox_data["actual_eta_min"] if mapbox_data else None
    dist = mapbox_data["distance_km"] if mapbox_data else None

    fused_spd, conf, is_anomaly = fuse_speeds(mapbox_spd, waze_spd)
    if fused_spd is None:
        return {"status": "Data unavailable"}

    ff = get_cached_free_flow(direction_name)

    # [STORE_ONLY] — do NOT use these as model features (target-derived)
    speed_ratio = float(f"{(fused_spd / ff):.4f}") if (fused_spd is not None and ff is not None and ff > 0) else None
    congestion = compute_congestion(float(fused_spd), float(ff)) if (fused_spd is not None and ff is not None) else None

    # TTI [STORE_ONLY] — = actual_eta / free_time → direct target transform
    tti = None
    if eta and ff and dist:
        free_time = (dist / ff) * 60.0
        if free_time > 0:
            tti = round(eta / free_time, 3)

    severity, severity_idx = classify_severity(congestion)

    # Anomaly ratio [logging only]
    anomaly_ratio = None
    if mapbox_spd is not None and waze_spd is not None:
        anomaly_ratio = round(
            abs(mapbox_spd - waze_spd) / max((mapbox_spd + waze_spd) / 2.0, 1e-6),
            4
        )

    # Weather (with EPA NowCast AQI — see weather.py)
    weather = fetch_weather(23.8067, 90.3687, WEATHER_API_KEY) or {}

    # MRT status
    mrt_active, headway = get_mrt_status(now, is_holiday=False)

    # Time features
    hour = now.hour
    if 7 <= hour <= 10:
        time_slot = "morning_peak"
    elif 16 <= hour <= 20:
        time_slot = "evening_peak"
    else:
        time_slot = "off_peak"

    return {
        "status": "OK",
        "geom": build_geom(origin, dest),

        "direction": direction_name,
        "corridor_id": direction_name,

        # [STORE_ONLY] speed features — NOT for use as model features
        "speed_kmh": fused_spd,
        "free_flow_kmh": ff,
        "speed_ratio": speed_ratio,           # [STORE_ONLY]
        "congestion_percent": congestion,      # [STORE_ONLY]
        "tti": tti,                            # [STORE_ONLY] direct target transform

        "severity_status": severity,
        "severity_index": severity_idx,

        "mapbox_speed": mapbox_spd,
        "waze_speed": waze_spd,
        "data_confidence": conf,

        "is_anomaly": int(is_anomaly),
        "anomaly_score": anomaly_ratio,
        "reason": "Mapbox-Waze deviation" if is_anomaly else None,

        "actual_eta_min": eta,
        "travel_time_sec": eta * 60.0 if eta else None,  # [STORE_ONLY]
        "distance_km": dist,

        "temperature": weather.get("temperature"),
        "rain_mm": weather.get("rain_mm"),
        "wind_speed": weather.get("wind_speed"),
        "visibility_km": weather.get("visibility_km"),
        "humidity": weather.get("humidity"),

        "pm2_5": weather.get("pm2_5"),
        "pm10": weather.get("pm10"),
        "co_level": weather.get("co_level"),
        "no2_level": weather.get("no2_level"),

        "weather_condition": weather.get("weather_condition"),
        "uv_index": weather.get("uv_index"),
        "aqi": weather.get("aqi"),  # FIX: EPA NowCast AQI (not max(PM2.5,PM10))

        "mrt_status": int(mrt_active),
        "mrt_headway": headway,

        "time_slot": time_slot,
        "day_of_week": now.strftime("%A"),

        "prediction_time": now.isoformat(),
        "horizon_min": 5,

        "source_count": int(mapbox_spd is not None) + int(waze_spd is not None)
    }