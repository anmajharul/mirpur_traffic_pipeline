import os
import json
import requests
import time
from supabase import create_client, Client
from datetime import datetime
import re

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ API Keys missing! Make sure they are set in GitHub Secrets.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🧠 FETCH DATA & TRAIN LLM FOR EACH DIRECTION
# ==========================================
def train_model():
    print("⏳ Fetching historical data from Supabase...")
    
    response = supabase.table("traffic_records").select("*").order("timestamp", desc=True).limit(5000).execute()
    data = response.data
    
    if not data:
        print("❌ No data found in Supabase (traffic_records table).")
        return

    directions_data = {}
    for row in data:
        direction = row.get('direction')
        time_field = row.get('timestamp')
        if not direction or not time_field: continue
            
        try:
            time_obj = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
            hour = time_obj.hour
            
            if direction not in directions_data: directions_data[direction] = {}
            if hour not in directions_data[direction]: directions_data[direction][hour] = {"sum": 0, "count": 0}
            
            directions_data[direction][hour]["sum"] += row['speed_kmh']
            directions_data[direction][hour]["count"] += 1
        except Exception:
            continue

    if not directions_data:
        print("❌ Failed to process directional hourly data.")
        return

    for direction, hourly_cal in directions_data.items():
        hist_str = ""
        for h in sorted(hourly_cal.keys()):
            avg_speed = round(hourly_cal[h]["sum"] / hourly_cal[h]["count"], 2)
            hist_str += f"Hour {h}: {avg_speed} km/h\n"
            
        print(f"\n🧠 Training AI for: {direction}...")

        sys_instruction = f"You are an ML model analyzing traffic for {direction} at Mirpur-10, Dhaka. Output ONLY a raw JSON array of exactly 24 numbers. These numbers represent the optimal predicted speed (km/h) for hours 0 to 23 based on the historical data. Smooth the curve. NO MARKDOWN, NO TEXT."
        prompt = f"Historical Data for {direction}:\n{hist_str}\nGenerate the 24-element array."

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

        # 🚨 ADVANCED RETRY LOGIC FOR RATE LIMITS
        success = False
        groq_output = ""
        
        for attempt in range(3): # সর্বোচ্চ ৩ বার চেষ্টা করবে
            try:
                res = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=40)
                
                if res.status_code == 200:
                    groq_output = res.json()['choices'][0]['message']['content']
                    success = True
                    break # সফল হলে লুপ থেকে বের হয়ে যাবে
                elif res.status_code == 429: # 429 মানে Rate Limit
                    print(f"⚠️ Groq Rate Limit hit for {direction} (Attempt {attempt+1}/3). Waiting 30 seconds...")
                    time.sleep(30) # লিমিট খেলে ৩০ সেকেন্ড অপেক্ষা করে আবার ট্রাই করবে
                else:
                    print(f"❌ Groq API Error {res.status_code} for {direction}: {res.text}")
                    break # অন্য কোনো এরর হলে স্কিপ করবে
                    
            except Exception as e:
                print(f"❌ Network error for {direction}: {e}")
                time.sleep(10)

        if not success:
            print(f"⏭️ Skipping {direction} after 3 failed attempts.")
            continue # ৩ বার ফেইল করলে পরের ডিরেকশনে যাবে
        
        # ডাটা প্রসেসিং ও সুপাবেস ইনসার্ট
        try:
            arr_match = re.search(r'\[(.*?)\]', groq_output, re.DOTALL)
            if arr_match:
                learned_weights = json.loads(f"[{arr_match.group(1)}]")
            else:
                learned_weights = json.loads(groq_output.strip())
                
            if len(learned_weights) == 24:
                payload_db = {"direction": direction, "weights": learned_weights}
                supabase.table("ml_weights").upsert(payload_db).execute()
                print(f"✅ Weights saved for {direction}!")
            else:
                print(f"❌ Invalid array length for {direction}. Expected 24, got {len(learned_weights)}.")
                
        except Exception as e:
            print(f"❌ Failed to parse JSON for {direction}. Raw output: {groq_output}")

        # সাধারণ ব্রেক (পরের ডিরেকশনে যাওয়ার আগে)
        print("⏳ Waiting 20 seconds before next AI request to respect Free Tier limits...")
        time.sleep(20)

if __name__ == "__main__":
    train_model()