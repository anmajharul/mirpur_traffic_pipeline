"""
data_collector.py — Q1 DEFENSIBLE DATA COLLECTION MODULE
==========================================================
Purpose:
- Multi-source probe data ingestion (Mapbox real-time + OSRM static baseline)
- FHWA congestion index and severity classification
- TTI computation (Travel Time Index)
- OGC LineString geometry encoding
- Temporal z-score anomaly detection
- Dynamic PCU scaling (non-lane-based heterogeneous traffic)
- EPA NowCast AQI (NOT max(PM2.5, PM10))
- Temporal feature encoding for ML feature engineering
- Weather condition ordinal encoding for XGBoost compatibility

ARCHITECTURE NOTE (Waze REMOVED):
  Waze has been fully removed from the data pipeline.
  Reason: Waze and Mapbox are correlated routing engines sharing
  overlapping GPS probe data. Their fusion violates sensor independence
  assumptions (El Faouzi et al. 2011, \u00a74). The independence assumption
  required for Kalman or inverse-variance fusion cannot be satisfied.
  Replacement: OSRM (Open Source Routing Machine) provides a genuinely
  different signal — static OSM-based routing with NO real-time traffic.
  The divergence between Mapbox (real-time) and OSRM (historical baseline)
  is a principled anomaly indicator: high divergence = unusual conditions.
  Reference: Luxen & Vetter (2011). ACM SIGSPATIAL 2011.

OSRM DUAL USE:
  1. Anomaly indicator: osrm_divergence = (osrm_spd - mapbox_spd) / osrm_spd
     Positive → current travel slower than historical (congestion)
     Negative → current travel faster than historical (unusual)
  2. Paper baseline: OSRM ETA stored as osrm_eta_min for Table 3 comparison.

REFERENCES:
[1] Luxen, D. & Vetter, C. (2011). Real-time routing with OpenStreetMap data.
    Proceedings of the 19th ACM SIGSPATIAL, pp. 513-516.
    https://doi.org/10.1145/2093973.2094062
    [Basis: OSRM static routing; osrm_divergence feature and paper baseline]

[2] El Faouzi, N.E. et al. (2011). Data fusion in road traffic engineering.
    Information Fusion, 12(1), 4-10.
    https://doi.org/10.1016/j.inffus.2010.06.001
    [Basis: Waze removed — independence assumption violated for routing engines]

[3] TRB (2022). Highway Capacity Manual, 7th Edition.
    Transportation Research Board. ISBN 978-0-309-08766-8.

[4] FHWA (2012). Travel Time Reliability: Making It There On Time, All The Time.
    Federal Highway Administration Report FHWA-HOP-06-070.
    https://ops.fhwa.dot.gov/publications/tt_reliability/
    [Basis: TTI = current_time / free_flow_time; CI = TTI - 1]

[5] OGC (2011). OpenGIS Simple Features Specification for SQL, Rev 1.2.1.
    https://www.ogc.org/standards/sfs
    [Basis: WKT LINESTRING geometry encoding for spatial storage]

[6] Vlahogianni, E.I. et al. (2014). Short-term traffic forecasting:
    Where we are and where we're going.
    Transportation Research Part C, 43, 3-19.
    https://doi.org/10.1016/j.trc.2014.01.005
    [Basis: 5-min collection cadence; 30-min (N=6) anomaly window]

[7] JICA (2015). Preparatory Survey on Dhaka Urban Transport Network
    Development Study (BD-P18).
    https://openjicareport.jica.go.jp/pdf/11996774_01.pdf
    [Basis: Dhaka peak hour definition; fleet PCU composition Table 4.3]

[8] Mapbox Directions API / Traffic Data Docs (2024).
    https://docs.mapbox.com/help/dive-deeper/directions/
    [Basis: driving-traffic profile for real-time ETA retrieval]

[9] Kaufman, S. et al. (2012). Leakage in Data Mining: Formulation, Detection,
    and Avoidance. ACM TKDD 6(4), Article 15.
    https://doi.org/10.1145/2382577.2382579
    [Basis: speed_kmh and tti marked STORE_ONLY; not used as ML features]

[10] Ahmed, M.S. & Cook, A.R. (1979). Analysis of freeway traffic time-series
     data by using Box-Jenkins techniques. Transportation Research Record,
     722, 1-9.
     [Basis: 2-sigma z-score criterion for temporal anomaly detection]
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
from mrt import get_mrt_status  # type: ignore
from freeflow import get_free_flow  # type: ignore
from data_loader import fetch_direction_data  # type: ignore
from evaluation import get_osrm_speed, get_osrm_eta  # type: ignore
from fusion import detect_temporal_anomaly  # type: ignore
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
# OSRM STATIC SPEED FETCHER
# Replaces Waze cache reader.
#
# WHY OSRM (not Waze):
#   Waze is a routing engine sharing GPS probe data with Mapbox.
#   Their fusion violates sensor independence (El Faouzi et al. 2011, \u00a74).
#   OSRM is a Static routing engine using OSM road network data with
#   NO real-time traffic. The divergence between Mapbox (real-time) and
#   OSRM (historical) is a principled anomaly feature.
#
# OSRM_DIVERGENCE FORMULA:
#   osrm_divergence = (osrm_speed - mapbox_speed) / osrm_speed
#   Positive → current travel slower than historical (congestion event)
#   Negative → current travel faster than historical (unusual event)
#
# STALENESS NOTE:
#   OSRM gives consistent static speeds — no staleness concern.
#   osrm_eta_min is stored per-cycle for paper Table 3 baseline comparison.
#
# References:
#   Luxen & Vetter (2011). ACM SIGSPATIAL 2011, pp. 513-516.
#   https://doi.org/10.1145/2093973.2094062
#   El Faouzi et al. (2011). Information Fusion, 12(1), 4-10.
#   https://doi.org/10.1016/j.inffus.2010.06.001
# -------------------------------------------------

# -------------------------------------------------
# PCU-WEIGHTED MIXED-TRAFFIC INDEX
# -------------------------------------------------
# Dhaka arterial fleet composition (JICA 2015, BD-P18, Table 4.3):
#   Motorized 2-wheeler (Motorcycle): 45%  -> 0.5 PCU each
#   Car/taxi:                         30%  -> 1.0 PCU each
#   CNG auto-rickshaw:                15%  -> 1.5 PCU each
#   Bus/truck:                        10%  -> 2.5 PCU each
#
# Fleet-weighted mean PCU (= FLEET_PCU computed in fusion.py):
#   = 0.45*0.5 + 0.30*1.0 + 0.15*1.5 + 0.10*2.5 = 1.025
# Reference: JICA (2015). RSTP Dhaka, Table 4.3.
#            https://openjicareport.jica.go.jp/pdf/12235575.pdf
#
# PCU SCALING METHOD (DYNAMIC — NOT FIXED 1.15x):
#   WHY NOT FIXED:
#     HCM 7e §11.3.3 describes capacity reduction in LANE-BASED traffic.
#     Dhaka is non-lane-based. PCU ≠ capacity. Mapping capacity drop
#     to a PCU multiplier has no theoretical basis in mixed heterogeneous flow.
#     Reference: Chandra & Sikdar (2000). Road & Transport Research, 9(3).
#   CORRECT FORMULA:
#     density_proxy = max(0, min(1, 1 - v / v_f))
#     CI = max(0, TTI - 1)  [congestion intensity; FHWA 2012]
#     PCU_d = density_proxy × FLEET_PCU × (1 + α × CI)  [α = 0.15, calibrated]
#   This is monotonically increasing with congestion intensity.
#   Reference: Chandra & Sikdar (2000); CSIR-CRRI (2017) Indo-HCM.
# -------------------------------------------------
FLEET_PCU = 0.45 * 0.5 + 0.30 * 1.0 + 0.15 * 1.5 + 0.10 * 2.5  # = 1.025
PCU_ALPHA = 0.15  # calibrated via grid search on validation set


def compute_pcu_index(
    fused_spd: float | None,
    free_flow_kmh: float | None,
    tti: float | None,
) -> tuple[float | None, str]:
    """
    Compute dynamic PCU-weighted mixed-traffic density index.

    Formula:
        density_proxy = max(0, min(1, 1 - v / v_f))     [Greenshields 1934]
        CI            = max(0, TTI - 1)                  [FHWA 2012, p.14]
        PCU_d         = density_proxy × FLEET_PCU × (1 + α × CI)

    WHY DYNAMIC (not fixed 1.15x):
        The removed 1.15 multiplier was sourced from HCM §11.3.3 (capacity
        reduction under incidents for LANE-BASED flow). Dhaka's traffic is
        non-lane-based and heterogeneous. Applying a lane-capacity multiplier
        to a PCU index violates the theoretical mapping between capacity and
        vehicle equivalence units. The dynamic CI-based formula is grounded in
        empirical PCU studies for mixed urban traffic.
        Reference: Chandra & Sikdar (2000). Road & Transport Research, 9(3).

    Returns:
        (pcu_index, pcu_source)
        pcu_source: 'dynamic_ci_scaled' or 'unavailable'

    References:
        Chandra, S. & Sikdar, P.K. (2000). Factors affecting PCU in mixed
        traffic situations on urban roads. Road & Transport Research, 9(3).
        [Basis: PCU as function of congestion intensity in non-lane-based flow]

        CSIR-CRRI (2017). Indian Highway Capacity Manual (Indo-HCM).
        https://www.crri.res.in
        [Basis: non-lane-based PCU interactions and fleet composition]

        FHWA (2012). Travel Time Reliability Guide. FHWA-HOP-06-070.
        https://ops.fhwa.dot.gov/publications/tt_reliability/
        [Basis: CI = TTI - 1 as congestion intensity measure]

        JICA (2015). RSTP Dhaka, Table 4.3.
        https://openjicareport.jica.go.jp/pdf/12235575.pdf
        [Basis: Dhaka fleet composition; FLEET_PCU = 1.025]
    """
    if fused_spd is None or free_flow_kmh is None or free_flow_kmh <= 0:
        return None, "unavailable"

    if tti is None:
        tti = max(1.0, free_flow_kmh / max(fused_spd, 1e-3))

    # Bounded Greenshields density proxy
    density_proxy = max(0.0, min(1.0, 1.0 - fused_spd / free_flow_kmh))

    # Congestion intensity: TTI - 1 (0 at free-flow, >0 under congestion)
    congestion_intensity = max(0.0, tti - 1.0)

    # Dynamic PCU: monotonically increasing with congestion intensity
    pcu_index = density_proxy * FLEET_PCU * (1.0 + PCU_ALPHA * congestion_intensity)

    return float(round(pcu_index, 4)), "dynamic_ci_scaled"


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

    if USE_GROUND_TRUTH and not mapbox_data:
        logging.warning(f"[COLLECT] Mapbox unavailable for {direction_name} — skipping")
        return {"status": "Data unavailable"}

    mapbox_spd = mapbox_data["speed_kmh"] if mapbox_data else None
    eta        = mapbox_data["actual_eta_min"] if mapbox_data else None
    dist       = mapbox_data["distance_km"] if mapbox_data else None

    if mapbox_spd is None:
        return {"status": "Data unavailable"}

    # Fused speed = Mapbox only (single source, no spatial fusion)
    # Waze removed: violates sensor independence (El Faouzi et al. 2011, §4)
    fused_spd = float(mapbox_spd)
    conf      = 0.80   # algorithmic routing baseline confidence

    ff = get_cached_free_flow(direction_name)

    # ── OSRM static speed + ETA (replaces Waze) ────────────────────────────
    # OSRM provides static OSM-based routing (no real-time traffic).
    # Stored for two purposes:
    #   1. osrm_divergence: anomaly feature (Mapbox vs historical baseline)
    #   2. osrm_eta_min: paper Table 3 baseline comparison column
    # Reference: Luxen & Vetter (2011). https://doi.org/10.1145/2093973.2094062
    osrm_spd = get_osrm_speed(origin, dest)
    osrm_eta = get_osrm_eta(origin, dest)

    # osrm_divergence = (osrm — mapbox) / osrm
    # Positive → current slower than historical (congestion)
    # Negative → current faster (unusual free-flow)
    # Undefined if OSRM unavailable (stored as None, imputed during training)
    osrm_divergence = None
    if osrm_spd is not None and osrm_spd > 0:
        osrm_divergence = round((osrm_spd - mapbox_spd) / osrm_spd, 4)

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

    # ── Temporal anomaly detection ────────────────────────────────────────────
    # Uses temporal z-score (NOT spatial Mapbox-Waze ratio).
    # History: past speeds from same corridor to build rolling baseline.
    # Reference: Ahmed & Cook (1979). TRR 722, 1-9.
    # detect_temporal_anomaly() imported from fusion.py.
    from fusion import detect_temporal_anomaly  # type: ignore
    try:
        history_df = fetch_direction_data(direction_name, days_lookback=3)
        history_speeds = []
        if not history_df.empty and "speed_kmh" in history_df.columns:
            history_speeds = history_df["speed_kmh"].dropna().tail(12).tolist()
    except Exception:
        history_speeds = []

    is_anomaly, z_score = detect_temporal_anomaly(
        current_speed=float(fused_spd),
        history=history_speeds,
    )

    # PCU-weighted mixed-traffic index (dynamic CI-based formula)
    pcu_index, pcu_source = compute_pcu_index(fused_spd, ff, tti)

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
        "data_confidence": conf,
        "source_count": 1,          # Mapbox only (1 real-time source)
        "osrm_eta_min": osrm_eta,   # [PAPER TABLE 3] OSRM static baseline ETA
        "osrm_divergence": osrm_divergence,  # [ML FEATURE] Mapbox vs OSRM divergence

        "is_anomaly": is_anomaly,
        "anomaly_score": z_score,           # temporal z-score (Ahmed & Cook 1979)
        "reason": "Anomaly (> 2σ)" if is_anomaly else ("Normal Traffic" if z_score is not None else None),

        "actual_eta_min": eta,
        "travel_time_sec": eta * 60.0 if eta else None,  # [STORE_ONLY]
        "distance_km": dist,

        # -----------------------------------------------------------
        # METEOROLOGICAL FEATURES
        # Sourced from WeatherAPI (Dhaka lat 23.8067, lon 90.3687).
        # AQI is EPA NowCast (computed in weather.py) — not max(PM2.5, PM10).
        #
        # EPA NowCast AQI Reference:
        #   US EPA (2024). Technical Assistance Document for the Reporting
        #   of Daily Air Quality — the Air Quality Index (AQI).
        #   EPA-454/B-24-001.
        #   https://www.airnow.gov/publications/air-quality-index/technical-assistance-document-for-reporting-the-daily-aqi/
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

        # Data availability: 1 = Mapbox only (Waze removed)
        # Reference: El Faouzi et al. (2011) — source independence §4.
        "source_count": 1,

        # PCU-weighted mixed-traffic density index.
        # When Waze is unavailable: Mapbox-only proxy (documented limitation).
        # References: JICA (2015) BD-P18; HCM 7e Table 11-11; Greenshields (1934).
        "pcu_index": pcu_index,
        "pcu_source": pcu_source,  # [STORE_ONLY] — not an ML feature

        # rain_x_peak_hour: interaction feature (rain × peak indicator).
        # Captures compound effect of precipitation and peak-hour congestion.
        # Lagged rain features capture post-rain drainage hysteresis.
        # Reference: Agarwal, M. et al. (2022). Weather-induced traffic disruption
        #   on urban arterials: A systematic review. TR Part D, 106, 103258.
        #   https://doi.org/10.1016/j.trd.2022.103258
        #   [Basis: rainfall-congestion interaction in heterogeneous urban traffic]
        "rain_x_peak_hour": round(rain_mm * int(is_peak), 4),
    }
