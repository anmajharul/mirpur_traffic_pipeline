"""
data_collector.py — Q1 DEFENSIBLE DATA COLLECTION MODULE
==========================================================
Purpose:
- Multi-source probe data ingestion (Mapbox + Waze cache)
- FHWA congestion index and severity classification
- TTI computation (Travel Time Index)
- OGC LineString geometry encoding
- Anomaly-aware fusion
- EPA NowCast AQI (NOT max(PM2.5, PM10))
- Temporal feature encoding (hour_of_day, is_peak_hour, is_weekend,
  is_monsoon, month) for ML feature engineering downstream
- Weather condition ordinal encoding for XGBoost compatibility

ARCHITECTURE NOTE (Waze):
  WazeRouteCalculator is NOT called directly from this module.
  GCP Cloud Run datacenter IPs (us-central1) are blocked by the
  Waze unofficial routing API. GitHub Actions runs on Microsoft Azure
  IP ranges, which Waze does not currently block.
  Architecture:
    GitHub Actions (Azure IP, */5 min) → WazeRouteCalculator
       → Supabase waze_speed_cache (upsert)
    Cloud Run collector job (GCP IP, */5 min)
       → _get_waze_from_cache() reads waze_speed_cache
       → fuse_speeds(mapbox_spd, waze_spd)
  This separation is disclosed in paper §3.2 (Data Acquisition).
  Reference: Bachmann et al. (2013) — multi-source heterogeneous
  data fusion with source-specific confidence weights.

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

[6] Vlahogianni, E.I. et al. (2014). Short-term traffic forecasting:
    Where we are and where we're going.
    Transportation Research Part C, 43, 3-19.
    https://doi.org/10.1016/j.trc.2014.01.005

[7] JICA (2015). Preparatory Survey on Dhaka Urban Transport Network
    Development Study (BD-P18).
    https://openjicareport.jica.go.jp/pdf/11996774_01.pdf

[8] Mapbox Directions API / Traffic Data Docs (2024).
    https://docs.mapbox.com/help/dive-deeper/directions/
    https://docs.mapbox.com/data/traffic/guides/

[9] Kaufman, S. et al. (2012). Leakage in Data Mining: Formulation, Detection,
    and Avoidance. ACM TKDD 6(4), Article 15.
    https://doi.org/10.1145/2382577.2382579
"""

import requests  # type: ignore
import logging
from datetime import datetime, timezone, timedelta

# WazeRouteCalculator is NOT imported here intentionally.
# GCP Cloud Run IPs are blocked by Waze. Waze data is collected by
# GitHub Actions (Azure IP) via waze_cache.yml and cached in Supabase.
# See _get_waze_from_cache() below.
# Reference: Bachmann et al. (2013), TR Part C, 26, 12–26.

from config import WEATHER_API_KEY, USE_GROUND_TRUTH, SUPABASE_URL, SUPABASE_KEY  # type: ignore
from weather import fetch_weather  # type: ignore
from fusion import fuse_speeds  # type: ignore
from mrt import get_mrt_status  # type: ignore
from freeflow import get_free_flow  # type: ignore
from data_loader import fetch_direction_data  # type: ignore
from supabase import create_client  # type: ignore

BDT = timezone(timedelta(hours=6))  # Bangladesh Standard Time (UTC+6)
TIMEOUT = 10
_free_flow_cache: dict = {}
# Module-level Supabase client (reused across cycles to avoid repeated auth)
_supabase_client = None


def _get_supabase():
    """Lazy singleton Supabase client — avoids re-authenticating per corridor."""
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


# -------------------------------------------------
# WEATHER CONDITION ORDINAL ENCODER
# Maps free-text WeatherAPI condition string to an ordinal integer
# suitable for XGBoost (no embedding layer needed).
#
# Encoding scheme (conservative, designed for monotone severity):
#   0 = Clear / Cloudy (baseline, minimal road-weather interaction)
#   1 = Rain / Drizzle (increased stopping distance, reduced visibility)
#   2 = Storm / Thunder / Tornado (severe disruption)
#   3 = Fog / Mist / Haze (severely reduced visibility)
#
# The ordinal values do NOT imply linear severity ordering;
# XGBoost tree splits are non-linear and handle this encoding correctly.
# Reference: Chen & Guestrin (2016), KDD 2016.
# -------------------------------------------------
_WEATHER_STORM_TERMS  = ("storm", "thunder", "tornado", "cyclone", "squall")
_WEATHER_RAIN_TERMS   = ("rain", "drizzle", "shower", "sleet", "precipitation")
_WEATHER_OBSCURE_TERMS = ("fog", "mist", "haze", "smoke", "dust", "sand")


def _encode_weather(condition: str | None) -> int:
    """
    Ordinal-encode a WeatherAPI condition string for XGBoost input.

    Returns:
        0 — Clear / Cloudy (default baseline)
        1 — Precipitation (rain, drizzle, shower)
        2 — Severe convective (storm, thunder, tornado)
        3 — Obscured visibility (fog, mist, haze)

    Reference:
        Chen, T. & Guestrin, C. (2016). XGBoost: A scalable tree boosting
        system. KDD 2016. https://doi.org/10.1145/2939672.2939785
    """
    if not condition:
        return 0
    c = condition.lower()
    if any(w in c for w in _WEATHER_STORM_TERMS):
        return 2
    if any(w in c for w in _WEATHER_RAIN_TERMS):
        return 1
    if any(w in c for w in _WEATHER_OBSCURE_TERMS):
        return 3
    return 0  # clear / partly cloudy / overcast


# -------------------------------------------------
# OGC LINESTRING GEOMETRY
# Reference: OGC SFS Rev 1.2.1
# -------------------------------------------------
def build_geom(origin: str, dest: str) -> str | None:
    """
    Build OGC WKT LineString from 'lat,lon' coordinate strings.
    Note: WKT format is lon lat (x y), not lat lon.
    Stored for spatial visualization and audit only; not an ML feature.
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
# Q1 note: use driving-traffic, not plain driving, so ETA reflects
# real-time plus historical congestion instead of a static road graph.
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
        f"https://api.mapbox.com/directions/v5/mapbox/driving-traffic/{o};{d}"
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
# WAZE CACHE READER
# Replaces the old direct WazeRouteCalculator call.
#
# WHY CACHE (not direct call):
#   Cloud Run containers run on GCP us-central1 IP ranges.
#   Waze's unofficial API rejects requests from GCP datacenters.
#   The waze_cache.yml GitHub Actions workflow (Azure IPs, */5 min)
#   writes fresh data to Supabase waze_speed_cache.
#   This module reads that cache instead of calling Waze directly.
#
# STALENESS POLICY:
#   Reject cache entries older than MAX_WAZE_CACHE_AGE_MIN (10 min).
#   This is 2× the collection cadence — conservative enough to tolerate
#   one missed GitHub Actions run before declaring the data stale.
#   Reference: Vlahogianni et al. (2014) — 5-min standard cadence.
#              Bachmann et al. (2013) — multi-source confidence weighting.
# -------------------------------------------------
MAX_WAZE_CACHE_AGE_MIN = 10  # reject cache entries older than this


def _get_waze_from_cache(corridor_id: str) -> float | None:
    """
    Read the latest Waze crowd-sourced speed from Supabase waze_speed_cache.

    The cache is written every 5 minutes by waze_cache_collector.py running
    on GitHub Actions (Microsoft Azure IP — not blocked by Waze).
    This function is called by Cloud Run collector containers (GCP IP).

    Returns:
        float — speed in km/h if cache is fresh and valid
        None  — if cache miss, stale data (> MAX_WAZE_CACHE_AGE_MIN), or error

    References:
        Bachmann, C. et al. (2013). TR Part C, 26, 12–26.
        https://doi.org/10.1016/j.trc.2012.09.003

        Vlahogianni, E.I. et al. (2014). TR Part C, 43, 3–19.
        https://doi.org/10.1016/j.trc.2014.01.005
    """
    try:
        sb = _get_supabase()
        res = (
            sb.table("waze_speed_cache")
            .select("waze_speed, collected_at")
            .eq("corridor_id", corridor_id)
            .execute()
        )
        if not res.data:
            logging.warning(f"[WAZE CACHE] No cache entry for '{corridor_id}'")
            return None

        row = res.data[0]
        waze_speed = row.get("waze_speed")
        collected_at_raw = row.get("collected_at")

        if collected_at_raw is None:
            return waze_speed  # no timestamp → return as-is (best effort)

        # Parse ISO timestamp; handle both offset-aware and naive strings
        collected = datetime.fromisoformat(collected_at_raw)
        if collected.tzinfo is None:
            collected = collected.replace(tzinfo=timezone.utc)
        age_min = (
            datetime.now(timezone.utc) - collected
        ).total_seconds() / 60.0

        if age_min > MAX_WAZE_CACHE_AGE_MIN:
            logging.warning(
                f"[WAZE CACHE] Stale data ({age_min:.1f} min old) for "
                f"'{corridor_id}' — threshold is {MAX_WAZE_CACHE_AGE_MIN} min"
            )
            return None

        logging.info(
            f"[WAZE CACHE] {corridor_id} → "
            f"{waze_speed:.2f} km/h (age {age_min:.1f} min)"
            if waze_speed is not None
            else f"[WAZE CACHE] {corridor_id} → None (Waze unavailable last cycle)"
        )
        return waze_speed

    except Exception as exc:
        logging.warning(f"[WAZE CACHE] Read failed for '{corridor_id}': {exc}")
        return None

# -------------------------------------------------
# PCU-WEIGHTED MIXED-TRAFFIC INDEX
# -------------------------------------------------
# Dhaka arterial fleet composition (JICA 2015, BD-P18, Table 4.3):
#   Motorized 2-wheeler (Motorcycle): 45%
#   Car/taxi:                         30%
#   CNG auto-rickshaw:                15%
#   Bus/truck:                        10%
#
# PCU equivalents for mixed urban flow (HCM 7e, Table 11-11):
#   Motorcycle = 0.5 PCU
#   Car        = 1.0 PCU
#   CNG        = 1.5 PCU
#   Bus        = 2.5 PCU
#
# Fleet-weighted mean PCU (FLEET_PCU):
#   = 0.45*0.5 + 0.30*1.0 + 0.15*1.5 + 0.10*2.5 = 1.025
#
# Method:
#   density_proxy = 1 - (v / v_f)   [Greenshields, 1934]
#   pcu_index     = density_proxy * FLEET_PCU
#
# When Waze is live  → scale by anomaly flag (waze_validated).
# When Waze is down  → Mapbox-only proxy flagged as mapbox_proxy.
#
# References:
#   JICA (2015) BD-P18 Table 4.3.
#   TRB (2022) HCM 7e, Table 11-11.
#   Greenshields, B.D. (1934). A Study of Traffic Capacity. HRB Proc.
#   Bachmann et al. (2013), TR Part C, DOI: 10.1016/j.trc.2012.09.003
# -------------------------------------------------
FLEET_PCU = 0.45 * 0.5 + 0.30 * 1.0 + 0.15 * 1.5 + 0.10 * 2.5  # = 1.025


def compute_pcu_index(
    fused_spd: float | None,
    free_flow_kmh: float | None,
    waze_spd: float | None,
    is_anomaly: bool,
) -> tuple[float | None, str]:
    """
    Compute PCU-weighted mixed-traffic density index.

    Returns:
        (pcu_index, pcu_source)
        pcu_source is one of:
            'waze_validated'  — Waze data was live; anomaly state used to
                                validate/scale the Mapbox proxy.
            'mapbox_proxy'    — Waze unavailable; index derived from Mapbox
                                speed ratio only (documented limitation).
            'unavailable'     — Insufficient data to compute index.
    """
    if fused_spd is None or free_flow_kmh is None or free_flow_kmh <= 0:
        return None, "unavailable"

    # Core Greenshields density proxy (bounded [0, 1])
    density_proxy = max(0.0, min(1.0, 1.0 - fused_spd / free_flow_kmh))
    raw_index = round(density_proxy * FLEET_PCU, 4)

    if waze_spd is not None:
        # Waze is live: scale upward during anomaly (confirmed mixed-flow
        # disturbance) to reflect higher effective PCU load.
        # Scale factor = 1.15 approximates heavy-vehicle PCU uplift during
        # incident conditions (HCM 7e §11.3.3).
        scale = 1.15 if is_anomaly else 1.0
        return round(raw_index * scale, 4), "waze_validated"
    else:
        # Waze unavailable: return Mapbox-only proxy.
        # Paper limitation note: flagged pcu_source='mapbox_proxy' so no
        # mixed-flow empirical validation can be performed for this cycle.
        # Document in paper §3.1 limitation paragraph.
        return raw_index, "mapbox_proxy"


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

    # Waze speed: read from Supabase cache (NOT direct call).
    # Cloud Run runs on GCP IP — blocked by Waze. GitHub Actions (Azure IP)
    # writes to waze_speed_cache every 5 min via waze_cache.yml.
    # Reference: Bachmann et al. (2013) — heterogeneous multi-source fusion.
    waze_spd = _get_waze_from_cache(direction_name)

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
    # Reference: Kaufman et al. (2012) — leakage formulation §3.
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
    # Limitation: Bangladesh public-holiday detection is not yet automated,
    # so is_holiday=False is a documented approximation for current runs.
    # This should be disclosed in the paper's limitations section.
    mrt_active, headway = get_mrt_status(now, is_holiday=False)

    # -----------------------------------------------------------
    # TEMPORAL FEATURE ENCODING
    # All features are computed from BDT (UTC+6) observation time.
    #
    # Peak hour definition follows Dhaka-specific RSTP (2015) study:
    #   Morning peak: 07:00–10:00 BDT  (corresponds to 01:00–04:00 UTC)
    #   Evening peak: 16:00–20:00 BDT  (corresponds to 10:00–14:00 UTC)
    # Reference: RSTP (2015). Revised Strategic Transport Plan for Dhaka.
    # Bangladesh Road Transport Authority / World Bank.
    #
    # Monsoon months: June–September (JICA 2015, BD-P18 §2.1)
    # -----------------------------------------------------------
    hour = now.hour
    is_peak = bool(7 <= hour <= 10 or 16 <= hour <= 20)
    rain_mm = weather.get("rain_mm") or 0.0

    if 7 <= hour <= 10:
        time_slot = "morning_peak"
    elif 16 <= hour <= 20:
        time_slot = "evening_peak"
    else:
        time_slot = "off_peak"

    # PCU-weighted mixed-traffic index
    pcu_index, pcu_source = compute_pcu_index(
        fused_spd, ff, waze_spd, bool(is_anomaly)
    )

    # -----------------------------------------------------------
    # WEATHER CONDITION ENCODED
    # Ordinal integer for XGBoost compatibility (no embedding needed).
    # 0=Clear, 1=Rain, 2=Storm/Thunder, 3=Fog/Mist
    # Reference: Chen & Guestrin (2016) XGBoost, KDD 2016.
    # -----------------------------------------------------------
    weather_cond_str = weather.get("weather_condition")
    weather_condition_encoded = _encode_weather(weather_cond_str)

    # is_extreme_weather: currently 0 (placeholder).
    # Will be updated to 1 when WeatherAPI alerts endpoint integration
    # is implemented. Documented as a known limitation in paper §3.2.
    is_extreme_weather = 0

    return {
        "status": "OK",
        "geom": build_geom(origin, dest),

        "direction": direction_name,
        "corridor_id": direction_name,

        # [STORE_ONLY] speed features — NOT for use as model features
        # Reference: Kaufman et al. (2012) — leakage formulation.
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

        # -----------------------------------------------------------
        # METEOROLOGICAL FEATURES
        # Sourced from WeatherAPI (Dhaka lat 23.8067, lon 90.3687).
        # AQI is EPA NowCast (computed in weather.py) — not max(PM2.5, PM10).
        # Reference: US EPA (2006) NowCast algorithm for AQI.
        # -----------------------------------------------------------
        "temperature": weather.get("temperature"),
        "rain_mm": weather.get("rain_mm"),
        "wind_speed": weather.get("wind_speed"),
        "visibility_km": weather.get("visibility_km"),
        "humidity": weather.get("humidity"),

        "pm2_5": weather.get("pm2_5"),
        "pm10": weather.get("pm10"),
        "co_level": weather.get("co_level"),
        "no2_level": weather.get("no2_level"),

        "weather_condition": weather_cond_str,
        # Ordinal encoding of weather condition for ML feature use.
        # 0=Clear, 1=Rain/Drizzle, 2=Storm/Thunder, 3=Fog/Mist/Haze
        # Reference: Chen & Guestrin (2016).
        "weather_condition_encoded": weather_condition_encoded,
        "weather_code": weather.get("weather_code"),  # raw WeatherAPI condition code
        # is_extreme_weather = 1 when severe weather alert is active.
        # Currently 0 (WeatherAPI alerts endpoint not yet integrated).
        # Documented limitation per paper §3.2.
        "is_extreme_weather": is_extreme_weather,

        "uv_index": weather.get("uv_index"),
        "aqi": weather.get("aqi"),  # EPA NowCast AQI (not max(PM2.5,PM10))

        "mrt_status": int(mrt_active),
        "mrt_headway": headway,

        # -----------------------------------------------------------
        # TEMPORAL FEATURES (ML-usable — genuinely exogenous)
        # All derived from BDT observation timestamp.
        # References:
        #   RSTP (2015) — Dhaka peak hour definition
        #   JICA (2015) BD-P18 §2.1 — monsoon seasonality
        #   Hyndman & Athanasopoulos (2021) FPP3 Ch.7 — cyclical encoding
        # -----------------------------------------------------------
        "hour_of_day": hour,                          # integer 0–23
        "is_peak_hour": int(is_peak),                 # 1 during morning/evening peak
        "is_weekend": int(now.weekday() >= 5),        # 1 = Saturday/Sunday
        "is_monsoon": int(now.month in (6, 7, 8, 9)), # June–September (JICA 2015)
        "month": now.month,                           # 1–12
        # day_of_week as integer (0=Monday … 6=Sunday) for ordinal consistency
        # in tree-based models. TEXT encoding removed.
        # Reference: Chen & Guestrin (2016) — ordinal over one-hot for XGBoost.
        "day_of_week": now.weekday(),                 # INTEGER 0–6
        "time_slot": time_slot,

        "prediction_time": now.isoformat(),
        # Five-minute horizon follows standard short-term arterial
        # forecasting practice in the ITS literature.
        "horizon_min": 5,  # Reference: Vlahogianni et al. (2014)

        # Data availability indicator: 1=single live source, 2=both.
        # Passed to the trainer as an uncertainty-aware feature.
        # Reference: El Faouzi et al. (2011) — source-reliability weighting.
        "source_count": int(mapbox_spd is not None) + int(waze_spd is not None),

        # PCU-weighted mixed-traffic density index.
        # When Waze is unavailable: Mapbox-only proxy (documented limitation).
        # References: JICA (2015) BD-P18; HCM 7e Table 11-11; Greenshields (1934).
        "pcu_index": pcu_index,
        "pcu_source": pcu_source,  # [STORE_ONLY] — not an ML feature

        # rain_x_peak_hour: interaction feature (rain × peak congestion).
        # Captures disproportionate speed degradation during rainy peak hours
        # in Dhaka (JICA 2015 §4.2 — rainfall ×2.1 congestion multiplier).
        # Reference: Goodfellow et al. (2016) DLbook §6.4 — feature interactions.
        "rain_x_peak_hour": round(rain_mm * int(is_peak), 4),
    }
