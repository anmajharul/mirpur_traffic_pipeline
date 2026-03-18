import os
import json
import requests
from supabase import create_client, Client
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS (NO HARDCODED SECRETS)
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ API Keys missing! Make sure they are set in GitHub Secrets.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🧠 FETCH DATA & TRAIN LLM
# ==========================================
def train_model():
    print("⏳ Fetching historical data from Supabase...")
    
    # 1. Get raw data from database (Changed to traffic_records)
    response = supabase.table("traffic_records").select("*").order("timestamp", desc=True).limit(2000).execute()
    data = response.data
    
    if not data:
        print("❌ No data found in Supabase (traffic_records table).")
        return

    # Process data into hourly averages
    hourly_cal = {}
    for row in data:
        # Get timestamp field
        time_field = row.get('timestamp') or row.get('created_at')
        if not time_field:
            continue
            
        try:
            # Extract hour safely
            time_obj = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
            hour = time_obj.hour
            
            if hour not in hourly_cal:
                hourly_cal[hour] = {"sum": 0, "count": 0}
            
            hourly_cal[hour]["sum"] += row['speed_kmh']
            hourly_cal[hour]["count"] += 1
        except Exception as e:
            print(f"Warning: Could not parse time for row: {e}")
            continue

    if not hourly_cal:
        print("❌ Failed to process hourly data.")
        return

    hist_str = ""
    for h in sorted(hourly_cal.keys()):
        avg_speed = round(hourly_cal[h]["sum"] / hourly_cal[h]["count"], 2)
        count = hourly_cal[h]["count"]
        hist_str += f"Hour {h}: {avg_speed} km/h (based on {count} pts)\n"

    print("🧠 Data aggregated. Sending to Groq Llama for analysis...")

    # 2. Call Groq API
    sys_instruction = "You are a strict data-science ML model. You MUST only output a single, raw JSON array of exactly 24 numbers representing average optimal traffic speeds (in km/h) for hours 0 to 23 at Mirpur-10, Dhaka. Smooth out the historical data provided. DO NOT output any markdown, no comments. Just the array."
    prompt = f"Raw Historical Data:\n{hist_str}\nCompute optimal 24h speed curve array."

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "llama-3.3-70b-versatile",
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": sys_instruction},
            {"role": "user", "content": prompt}
        ]
    }

    res = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
    
    if res.status_code != 200:
        print(f"❌ Groq API failed: {res.text}")
        return
        
    groq_output = res.json()['choices'][0]['message']['content']
    
    try:
        # Extract JSON array robustly
        import re
        arr_match = re.search(r'\[(.*?)\]', groq_output, re.DOTALL)
        if arr_match:
            learned_weights = json.loads(f"[{arr_match.group(1)}]")
        else:
            learned_weights = json.loads(groq_output.strip())
            
        if len(learned_weights) == 24:
            print(f"✅ Successfully trained! Generated curve: {learned_weights[:3]}...")
            
            # 3. Save the result back to Supabase
            payload_db = {
                "id": 1, 
                "weights": learned_weights,
            }
            # Make sure you have the 'ml_weights' table in your database
            supabase.table("ml_weights").upsert(payload_db).execute()
            print("🚀 Weights saved to Supabase! React app can now read these directly.")
            
        else:
            print(f"❌ Invalid array length from Groq. Expected 24, got {len(learned_weights)}.")
            
    except Exception as e:
        print(f"❌ Failed to parse JSON or push to DB: {e}\nRaw Output: {groq_output}")

if __name__ == "__main__":
    train_model()