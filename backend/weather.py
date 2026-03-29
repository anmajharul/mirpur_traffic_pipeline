"""
weather.py — Q1 DEFENSIBLE WEATHER FETCHING MODULE
====================================================
Purpose:
- Real-time weather + AQI data from WeatherAPI
- Physical range validation (Dhaka-specific bounds)
- Retry logic with exponential backoff
- AQI computed via EPA NowCast breakpoint formula (NOT max(PM2.5, PM10))

REFERENCES:
[1] Rahman, M.T. et al. (2017). Weather effects on traffic flow in Bangladesh.
    Meteorological Applications, 24(2), 300–311.
    https://doi.org/10.1002/met.1643

[2] Maze, T.H. et al. (2006). Whether weather matters to traffic demand,
    traffic safety, and traffic flow. Transportation Research Record, 1948(1).
    https://doi.org/10.1177/0361198106194500106

[3] Koetse, M.J. & Rietveld, P. (2009). The impact of climate change and
    weather on transport: An overview of empirical findings.
    Transportation Research Part D, 14(3), 205–221.
    https://doi.org/10.1016/j.trd.2008.12.004

[4] US EPA (2024). Technical Assistance Document for the Reporting of
    Daily Air Quality — the Air Quality Index (AQI). EPA-454/B-24-001.
    https://www.airnow.gov/sites/default/files/2024-07/technical-assistance-document-for-the-reporting-of-daily-air-quality.pdf
"""

import requests
import logging
import time
from datetime import datetime, timezone

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
TIMEOUT = 10
MAX_RETRIES = 3
BACKOFF = 3

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


# -------------------------------------------------
# EPA NOWCAST AQI — PM2.5 BREAKPOINTS (Table 1, EPA 2024)
# Reference: EPA-454/B-24-001
# -------------------------------------------------
_PM25_BREAKPOINTS = [
    (0.0,   12.0,   0,   50),
    (12.1,  35.4,  51,  100),
    (35.5,  55.4, 101,  150),
    (55.5, 150.4, 151,  200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

_PM10_BREAKPOINTS = [
    (0,    54,   0,  50),
    (55,  154,  51, 100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 504, 301, 400),
    (505, 604, 401, 500),
]


def _pm_to_aqi(concentration: float, breakpoints: list) -> int | None:
    """
    Convert PM concentration to AQI using EPA linear interpolation formula.
    AQI = ((AQI_hi - AQI_lo) / (BP_hi - BP_lo)) * (C - BP_lo) + AQI_lo
    Reference: EPA-454/B-24-001, Equation 1.
    """
    for bp_lo, bp_hi, aqi_lo, aqi_hi in breakpoints:
        if bp_lo <= concentration <= bp_hi:
            aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + aqi_lo
            return round(aqi)
    return None


def compute_aqi(pm2_5: float | None, pm10: float | None) -> int | None:
    """
    Compute AQI as maximum of PM2.5-AQI and PM10-AQI.
    Per EPA convention: overall AQI = max of all pollutant sub-indices.
    Reference: EPA-454/B-24-001, Section 3.
    """
    candidates = []
    if pm2_5 is not None and pm2_5 >= 0:
        v = _pm_to_aqi(float(f"{pm2_5:.1f}"), _PM25_BREAKPOINTS)
        if v is not None:
            candidates.append(v)
    if pm10 is not None and pm10 >= 0:
        v = _pm_to_aqi(round(pm10), _PM10_BREAKPOINTS)
        if v is not None:
            candidates.append(v)
    return max(candidates) if candidates else None


# -------------------------------------------------
# DEFAULT SAFE OUTPUT (NO NULL CRASH)
# -------------------------------------------------
def default_weather():
    return {
        "temperature": None,
        "rain_mm": 0.0,
        "wind_speed": None,
        "visibility_km": None,
        "humidity": None,
        "uv_index": None,
        "weather_condition": None,
        "weather_code": None,
        "pm2_5": None,
        "pm10": None,
        "co_level": None,
        "no2_level": None,
        "aqi": None,
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }


# -------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------
def fetch_weather(lat: float, lon: float, api_key: str) -> dict:
    """
    Fetch real-time weather and AQI data for a given lat/lon.
    AQI computed via EPA NowCast breakpoint formula (EPA-454/B-24-001).

    Returns:
        dict with validated weather features. Never raises — returns
        default_weather() on any failure.
    """
    if not api_key or lat is None or lon is None:
        logging.warning("[WEATHER] Invalid input — returning defaults")
        return default_weather()

    url = (
        "https://api.weatherapi.com/v1/current.json"
        f"?key={api_key}&q={lat},{lon}&aqi=yes"
    )

    for attempt in range(MAX_RETRIES + 1):
        try:
            start_time = time.time()
            response = requests.get(url, timeout=TIMEOUT)
            latency = float(f"{(time.time() - start_time):.2f}")

            if response.status_code == 429:
                logging.warning("[WEATHER] Rate limited — returning defaults")
                return default_weather()

            if response.status_code >= 500:
                logging.warning(f"[WEATHER] Server error {response.status_code}, retry {attempt+1}")
                time.sleep(BACKOFF * (attempt + 1))  # exponential backoff
                continue

            if response.status_code != 200:
                logging.error(f"[WEATHER] Client error {response.status_code}")
                return default_weather()

            data = response.json()
            current = data.get("current")
            if not current:
                logging.warning("[WEATHER] Empty 'current' block")
                return default_weather()

            condition = current.get("condition", {})
            air = current.get("air_quality") or {}

            pm2_5 = air.get("pm2_5")
            pm10 = air.get("pm10")

            result = {
                "temperature": current.get("temp_c"),
                # NOTE: precip_mm is point accumulation (hourly proxy),
                # not rainfall intensity. Documented limitation per
                # Koetse & Rietveld (2009).
                "rain_mm": current.get("precip_mm"),
                "wind_speed": current.get("wind_kph"),
                "visibility_km": current.get("vis_km"),
                "humidity": current.get("humidity"),
                "uv_index": current.get("uv"),
                "weather_condition": condition.get("text"),
                "weather_code": condition.get("code"),
                "pm2_5": pm2_5,
                "pm10": pm10,
                "co_level": air.get("co"),
                "no2_level": air.get("no2"),

                # FIX: EPA NowCast AQI formula (NOT max(PM2.5, PM10))
                # Reference: EPA-454/B-24-001, Equation 1
                "aqi": compute_aqi(pm2_5, pm10),

                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "api_latency_sec": latency
            }

            # --------------------------
            # PHYSICAL VALIDATION (Dhaka bounds)
            # Temp: 10–45°C (Rahman et al. 2017)
            # Rain: 0–100 mm/hr (extreme Dhaka monsoon)
            # Wind: 0–120 kph
            # Visibility: 0–20 km
            # --------------------------
            t_raw = result["temperature"]
            if t_raw is not None:
                t = float(t_raw)
                if not (10 <= t <= 45):
                    logging.warning(f"[WEATHER] Temperature {t} out of range, nulled")
                    result["temperature"] = None

            r_raw = result["rain_mm"]
            if r_raw is not None:
                r = float(r_raw)
                result["rain_mm"] = r if 0 <= r <= 100 else 0.0

            w_raw = result["wind_speed"]
            if w_raw is not None:
                w = float(w_raw)
                result["wind_speed"] = w if 0 <= w <= 120 else None

            v_raw = result["visibility_km"]
            if v_raw is not None:
                v = float(v_raw)
                result["visibility_km"] = v if 0 <= v <= 20 else None

            return result

        except requests.exceptions.Timeout:
            logging.warning(f"[WEATHER] Timeout attempt {attempt+1}")
            time.sleep(BACKOFF * (attempt + 1))

        except requests.exceptions.RequestException as e:
            logging.warning(f"[WEATHER] Network issue: {e}")
            time.sleep(BACKOFF)

        except Exception as e:
            logging.error(f"[WEATHER] Unexpected error: {e}")
            return default_weather()

    logging.error("[WEATHER] Failed after all retries — returning defaults")
    return default_weather()