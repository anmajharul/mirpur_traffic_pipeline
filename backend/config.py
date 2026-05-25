"""
config.py — Q1 DEFENSIBLE CONFIGURATION MODULE
================================================
Study Area: Mirpur-10, Dhaka, Bangladesh
Coordinate System: WGS84 (EPSG:4326)

REFERENCES:
[1] CIA World Factbook — Bangladesh geographic bounds
    https://www.cia.gov/the-world-factbook/countries/bangladesh/
[2] BBS GIS (2022) — Bangladesh Bureau of Statistics spatial datasets
    https://www.bbs.gov.bd/
[3] OpenStreetMap (2024) — https://www.openstreetmap.org/
"""

import os
from dotenv import load_dotenv  # type: ignore

load_dotenv()


# -------------------------------------------------
# SAFE ENV LOADER
# -------------------------------------------------
def get_env_variable(key: str, required: bool = True, default=None):
    """
    Safely fetch environment variables.
    Raises EnvironmentError if a required variable is missing.
    """
    value = os.getenv(key, default)
    if required and not value:
        raise EnvironmentError(f"[CONFIG ERROR] Missing env variable: {key}")
    return value


# -------------------------------------------------
# SCIENTIFIC VALIDATION FLAGS
# -------------------------------------------------
ENABLE_FUSION = False      # Spatial fusion disabled (uses single-source Mapbox)
USE_GROUND_TRUTH = True    # Mandatory for Q1 publication


# -------------------------------------------------
# BANGLADESH WEEKEND DEFINITION
# -------------------------------------------------
# Bangladesh observes Friday & Saturday as the official weekend.
# Python weekday(): 4 = Friday, 5 = Saturday (0=Monday, 6=Sunday).
# Reference: Bangladesh Labor Act 2006, Section 103.
# Use: is_weekend = now.weekday() in WEEKEND_DAYS
WEEKEND_DAYS = {4, 5}  # Friday, Saturday

# Note: Public holidays are computed dynamically using the 'holidays'
# python package (holidays.country_holidays('BD')) in data_collector.py.


# -------------------------------------------------
# API KEYS
# Note (NS1 — Dual Weather API Architecture):
#   Tomorrow.io  → meteorological data (precipitation, visibility, temperature,
#                  humidity, UV index, weather condition code).
#   Open-Meteo   → air quality data (PM2.5, PM10, AQI, CO, NO2) sourced from
#                  Copernicus CAMS at no cost.
#   WeatherAPI was evaluated but replaced after empirical comparison against
#   BMD Agargaon ground-truth showed Tomorrow.io had lower RMSE for
#   precipitation intensity over the Mirpur-10 study corridor.
# -------------------------------------------------
SUPABASE_URL = get_env_variable("SUPABASE_URL")
SUPABASE_KEY = get_env_variable("SUPABASE_KEY")
MAPBOX_TOKEN = get_env_variable("MAPBOX_TOKEN", required=False)
TOMORROW_API_KEY = get_env_variable("TOMORROW_API_KEY", required=False)


# -------------------------------------------------
# STUDY AREA: MIRPUR-10 CORRIDORS
# -------------------------------------------------
# Coordinate System: WGS84 (EPSG:4326)
# Source: OpenStreetMap + Google Maps field verification
# Bangladesh geographic extent: Lat 20.34–26.63, Lon 88.01–92.67
# Reference: CIA World Factbook / BBS GIS
CORRIDORS = {
    "North (Mirpur-11 to 10)": {
        "origin": "23.818833,90.365443",
        "dest": "23.807247,90.368658",
        "name": "Mirpur-10 Circle"
    },
    "South (Kazipara to 10)": {
        "origin": "23.795476,90.373516",
        "dest": "23.806925,90.368497",
        "name": "Mirpur-10 Circle"
    },
    "East (Mirpur-14 to 10)": {
        "origin": "23.801368,90.380476",
        "dest": "23.807028,90.368790",
        "name": "Mirpur-10 Circle"
    },
    "West (Mirpur-1 to 10)": {
        "origin": "23.801584,90.357905",
        "dest": "23.807144,90.368412",
        "name": "Mirpur-10 Circle"
    }
}


# -------------------------------------------------
# COORDINATE VALIDATION
# -------------------------------------------------
def validate_corridors(corridors: dict):
    """
    Validate WGS84 coordinate format and Bangladesh geographic bounds.
    References: CIA World Factbook, BBS GIS datasets.
    """
    for name, coords in corridors.items():
        if not isinstance(coords, dict):
            raise ValueError(f"{name} → Invalid structure")
        for key in ["origin", "dest"]:
            value = coords.get(key)
            if value is None or "," not in value:
                raise ValueError(f"{name} → Invalid {key}: {value}")
            try:
                lat_str, lon_str = str(value).split(",")
                lat = float(lat_str.strip())
                lon = float(lon_str.strip())
            except Exception:
                raise ValueError(f"{name} → Invalid coordinate format: {value}")
            if not (20.34 <= lat <= 26.63 and 88.01 <= lon <= 92.67):
                raise ValueError(f"{name} → Outside Bangladesh bounds: {value}")


# -------------------------------------------------
# CONFIG VALIDATION
# -------------------------------------------------
def validate_config():
    if not SUPABASE_URL.startswith("http"):
        raise ValueError("Invalid SUPABASE_URL")
    if "supabase" not in SUPABASE_URL:
        raise ValueError("SUPABASE_URL not recognized")
    if len(SUPABASE_KEY) < 20:
        raise ValueError("SUPABASE_KEY looks invalid")
    if MAPBOX_TOKEN and len(MAPBOX_TOKEN) < 10:
        raise ValueError("MAPBOX_TOKEN looks suspicious")
    if TOMORROW_API_KEY and len(TOMORROW_API_KEY) < 10:
        raise ValueError("TOMORROW_API_KEY looks suspicious")


# -------------------------------------------------
# STARTUP VALIDATION
# -------------------------------------------------
validate_corridors(CORRIDORS)
validate_config()