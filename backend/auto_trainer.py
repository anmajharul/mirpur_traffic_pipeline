import os
import json
import requests
from supabase import create_client, Client

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://fojusiwszuetnibdgbze.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "sb_publishable_mUMIUt23GmSVnbHk_AzB8w_xzRQvjvv")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "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")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🧠 FETCH DATA & TRAIN LLM
# ==========================================
def train_model():
    print("⏳ Fetching historical data from Supabase...")
    
    # 1. Get raw data from database
    response = supabase.table("traffic_records").select("*").order("timestamp", desc=True).limit(2000).execute()
    data = response.data
    
    if not data:
        print("❌ No data found in Supabase. Run data_collector.py first.")
        return

    # Process data into hourly averages
    hourly_cal = {}
    for row in data:
        # Assuming timestamp is ISO format
        hour = int(row['timestamp'].split('T')[1].split(':')[0])
        if hour not in hourly_cal:
            hourly_cal[hour] = {"sum": 0, "count": 0}
        hourly_cal[hour]["sum"] += row['speed_kmh']
        hourly_cal[hour]["count"] += 1

    hist_str = ""
    for h in sorted(hourly_cal.keys()):
        avg_speed = round(hourly_cal[h]["sum"] / hourly_cal[h]["count"], 2)
        count = hourly_cal[h]["count"]
        hist_str += f"Hour {h}: {avg_speed} km/h (based on {count} pts)\n"

    print("🧠 Data aggregated. Sending to Groq Llama-3.3 for analysis...")

    # 2. Call Groq API
    sys_instruction = "You are a strict data-science ML model. You MUST only output a single, raw JSON array of exactly 24 numbers representing average optimal traffic speeds (in km/h) for hours 0 to 23 at Mirpur-10, Dhaka. Smooth out the historical data provided. DO NOT output any markdown, no comments. Just the array, e.g. [45.1, 46.2, ...]"
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
        # Extract JSON array
        import re
        arr_match = re.search(r'\[(.*?)\]', groq_output, re.DOTALL)
        if arr_match:
            learned_weights = json.loads(f"[{arr_match.group(1)}]")
        else:
            learned_weights = json.loads(groq_output.strip())
            
        if len(learned_weights) == 24:
            print(f"✅ Successfully trained! Generated curve: {learned_weights[:3]}...")
            
            # 3. Save the result back to Supabase (e.g., in a separate table 'ml_weights')
            # For this to work, you must create an `ml_weights` table in Supabase 
            # with columns: id (int), weights (jsonb), updated_at (timestamp).
            
            payload_db = {
                "id": 1, 
                "weights": learned_weights,
                "updated_at": "now()"
            }
            # Upsert into table (insert or update)
            supabase.table("ml_weights").upsert(payload_db).execute()
            print("🚀 Weights saved to Supabase! React app can now read these directly.")
            
        else:
            print("❌ Invalid array length from Groq.")
            
    except Exception as e:
        print(f"❌ Failed to parse JSON or push to DB: {e}\nRaw Output: {groq_output}")

if __name__ == "__main__":
    train_model()
