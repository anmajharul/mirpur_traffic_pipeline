import os
import requests
from datetime import datetime
from supabase import create_client, Client

# Supabase Project URL
SUPABASE_URL = "https://rkousttmedthicfqybqe.supabase.co"

# GitHub Secrets theke Key gulo asbe
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

if not SUPABASE_KEY or not TOMTOM_API_KEY:
    print("❌ API Keys missing! GitHub Secrets e key add koro.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

MIRPUR_LAT = 23.8067
MIRPUR_LON = 90.3687

DESTINATIONS = [
    {"name": "Popular Hospital", "lat": 23.8041, "lon": 90.3667},
    {"name": "Farmgate", "lat": 23.7670, "lon": 90.3776},
    {"name": "Airport", "lat": 23.8500, "lon": 90.4000}
]

def fetch_and_save_weather():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Weather Data asche...")
    url = f"https://api.open-meteo.com/v1/forecast?latitude={MIRPUR_LAT}&longitude={MIRPUR_LON}&current_weather=true&hourly=rain&timezone=Asia/Dhaka"
    try:
        res = requests.get(url)
        data = res.json()
        if "current_weather" in data:
            weather_data = {
                "temperature": data["current_weather"]["temperature"],
                "wind_speed": data["current_weather"]["windspeed"],
                "rain_mm": data["hourly"]["rain"][0] if "hourly" in data else 0.0
            }
            supabase.table("weather_data").insert(weather_data).execute()
            print("✅ Weather Data Saved!")
    except Exception as e:
        print(f"❌ Weather Error: {e}")

def fetch_and_save_traffic():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Traffic Data asche...")
    for dest in DESTINATIONS:
        url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={dest['lat']}%2C{dest['lon']}&key={TOMTOM_API_KEY}"
        try:
            res = requests.get(url)
            if res.status_code == 200:
                flow = res.json().get("flowSegmentData", {})
                if flow:
                    current_speed = flow.get("currentSpeed", 0)
                    free_flow_speed = flow.get("freeFlowSpeed", 50)
                    
                    congestion = max(0, min(100, 100 - (current_speed / free_flow_speed) * 100))
                    travel_time_sec = int((2 / current_speed) * 3600) if current_speed > 0 else 999
                    
                    traffic_data = {
                        "destination": dest["name"],
                        "travel_time_sec": travel_time_sec,
                        "speed_kmh": current_speed,
                        "congestion_percent": round(congestion, 1)
                    }
                    supabase.table("traffic_data").insert(traffic_data).execute()
                    print(f"✅ Traffic Saved for: {dest['name']}")
        except Exception as e:
            print(f"❌ Traffic Error ({dest['name']}): {e}")

if __name__ == "__main__":
    print("="*40)
    fetch_and_save_weather()
    fetch_and_save_traffic()
    print("="*40)
    print("Task Completed!")