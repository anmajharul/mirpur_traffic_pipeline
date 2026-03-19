import requests
import os

def check_coverage():
    lat, lon = "23.8071318", "90.3686089"
    api_key = os.getenv("TOMTOM_API_KEY", "").strip() # Space থাকলে ক্লিন করবে
    
    # টেস্টিং ইউআরএল
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/18/json"
    params = {
        "point": f"{lat},{lon}",
        "key": api_key
    }
    
    print(f"📡 Requesting: {url}?point={lat},{lon}")
    
    res = requests.get(url, params=params)
    
    if res.status_code == 200:
        print("✅ Coverage Confirmed! Data received.")
        print(res.json())
    else:
        print(f"❌ Error {res.status_code}: {res.text}")

if __name__ == "__main__":
    check_coverage()