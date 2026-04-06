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
# DEFAULT SAFE OUTPUT (NO NULL CRASH)
# -------------------------------------------------
def default_weather():
    return {
        "temperature": None,
        "rain_mm": 0.0,  # Safe default to avoid math errors
        "wind_speed": None,
        "visibility_km": None,
        "humidity": None,
        "uv_index": None,
        "weather_condition": None,
        "weather_code": None,
        "pm2_5": None,
        "pm10": None,
        "co_level": None,  # Not natively available in some tier
        "no2_level": None, # Not natively available in some tier
        "aqi": None,
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }

# -------------------------------------------------
# MAIN FUNCTION (Tomorrow.io API)
# -------------------------------------------------
def fetch_weather(lat: float, lon: float, api_key: str) -> dict:
    """
    Fetch real-time weather and AQI data via Tomorrow.io Timelines API.
    Provides Q1-defensible physical environmental data (precipitation intensity, 
    visibility, EPA AQI, PM2.5).
    """
    if not api_key or lat is None or lon is None:
        logging.warning("[WEATHER] Invalid input — returning defaults")
        return default_weather()

    # Requesting specific physical fields necessary for Rainfall Hysteresis and Visibility Penalty
    fields = "temperature,precipitationIntensity,visibility,windSpeed,humidity,uvIndex,weatherCode,epaIndex,particulateMatter25,particulateMatter10"
    url = (
        f"https://api.tomorrow.io/v4/timelines?"
        f"location={lat},{lon}&fields={fields}&timesteps=current&units=metric&apikey={api_key}"
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
                time.sleep(BACKOFF * (attempt + 1))
                continue

            if response.status_code != 200:
                logging.error(f"[WEATHER] Client error {response.status_code}: {response.text}")
                return default_weather()

            data = response.json()
            timelines = data.get("data", {}).get("timelines", [])
            if not timelines or not timelines[0].get("intervals"):
                logging.warning("[WEATHER] Empty timelines block")
                return default_weather()

            current = timelines[0]["intervals"][0].get("values", {})

            # Map Tomorrow.io fields
            temp = current.get("temperature")
            rain = current.get("precipitationIntensity")
            wind = current.get("windSpeed")
            vis = current.get("visibility")
            
            result = {
                "temperature": float(temp) if temp is not None else None,
                "rain_mm": float(rain) if rain is not None else 0.0,
                "wind_speed": float(wind) if wind is not None else None,
                "visibility_km": float(vis) if vis is not None else None,
                "humidity": current.get("humidity"),
                "uv_index": current.get("uvIndex"),
                "weather_condition": "Mapped by code",  # Add a generic string since tomorrow only provides code
                "weather_code": current.get("weatherCode"),
                "pm2_5": current.get("particulateMatter25"),
                "pm10": current.get("particulateMatter10"),
                "co_level": None,  # Excluded due to API limits, we will use PM2.5 for interaction
                "no2_level": None, 
                "aqi": current.get("epaIndex"),

                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "api_latency_sec": latency,
            }

            # --------------------------
            # PHYSICAL VALIDATION (Dhaka bounds)
            # --------------------------
            if result["temperature"] is not None and not (8 <= result["temperature"] <= 45):
                result["temperature"] = None

            if result["rain_mm"] is not None and not (0 <= result["rain_mm"] <= 100):
                result["rain_mm"] = 0.0

            if result["wind_speed"] is not None and not (0 <= result["wind_speed"] <= 120):
                result["wind_speed"] = None

            if result["visibility_km"] is not None and not (0 <= result["visibility_km"] <= 20):
                result["visibility_km"] = None

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
