import os
import json
import requests
import time
import re
from supabase import create_client, Client
from datetime import datetime

# ==========================================
# ⚙️ CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ API Keys missing! Check GitHub Secrets.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🧠 FETCH DATA & TRAIN LLM
# ==========================================
def train_model():
    print(f"⏳ [{datetime.now().strftime('%H:%M:%S')}] Fetching historical records...")
    
    try:
        # ৫০০০ রেকর্ড রিসার্চের জন্য যথেষ্ট ভালো স্যাম্পল সাইজ
        response = supabase.table("traffic_records").select("direction, speed_kmh, timestamp").order("timestamp", desc=True).limit(5000).execute()
        data = response.data
    except Exception as e:
        print(f"❌ Supabase Fetch Error: {e}")
        return

    if not data:
        print("❌ No data found in 'traffic_records'.")
        return

    # ১. ডেটা গ্রুপিং (Direction -> Hour -> Avg Speed)
    directions_data = {}
    for row in data:
        direction = row.get('direction')
        time_field = row.get('timestamp')
        speed = row.get('speed_kmh')
        
        if not all([direction, time_field, speed]): continue
            
        try:
            # Timestamp থেকে ঘণ্টা বের করা (UTC handling)
            time_obj = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
            hour = time_obj.hour
            
            if direction not in directions_data: directions_data[direction] = {}
            if hour not in directions_data[direction]: directions_data[direction][hour] = {"sum": 0, "count": 0}
            
            directions_data[direction][hour]["sum"] += speed
            directions_data[direction][hour]["count"] += 1
        except:
            continue

    # ২. প্রতিটি ডিরেকশনের জন্য আলাদাভাবে AI ট্রেনিং
    for direction, hourly_cal in directions_data.items():
        hist_str = ""
        for h in sorted(hourly_cal.keys()):
            avg_speed = round(hourly_cal[h]["sum"] / hourly_cal[h]["count"], 2)
            hist_str += f"Hour {h}: {avg_speed} km/h\n"
            
        print(f"\n🤖 Training Llama-3.3 for: {direction}...")

        # মেগা প্রম্পট: মডেলকে বাধ্য করা যেন সে শুধু JSON দেয়
        sys_instruction = (
            f"You are a Traffic Flow ML model for {direction} at Mirpur-10, Dhaka. "
            "Task: Predict 24-hour speed profile based on historical data. "
            "CRITICAL: Output ONLY a raw JSON array of 24 numbers (Hour 0 to 23). "
            "No explanations, no markdown code blocks, just [n1, n2, ... n24]."
        )
        
        prompt = f"Historical Speed Data:\n{hist_str}\n\nReturn exactly 24 elements JSON array."

        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.2, # কিছুটা ক্রিয়েটিভিটি এলাউ করা হয়েছে স্মুথনেস এর জন্য
            "messages": [
                {"role": "system", "content": sys_instruction},
                {"role": "user", "content": prompt}
            ]
        }

        # ৩. স্মার্ট রিট্রাই লজিক (Handling Rate Limits)
        groq_output = ""
        for attempt in range(3):
            try:
                res = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=45)
                if res.status_code == 200:
                    groq_output = res.json()['choices'][0]['message']['content']
                    break
                elif res.status_code == 429:
                    print(f"⚠️ Rate limit hit! Waiting 40s (Attempt {attempt+1}/3)...")
                    time.sleep(40)
                else:
                    print(f"❌ API Error {res.status_code}: {res.text}")
                    break
            except Exception as e:
                print(f"❌ Connection Error: {e}")
                time.sleep(10)

        if not groq_output:
            continue

        # ৪. ক্লিনআপ ও সুপাবেসে সেভ (Regex to extract array)
        try:
            # এআই যদি বাড়তি কথা লিখে ফেলে, এই রেজেক্স শুধু ব্র্যাকেটের ভেতরের অংশটুকু নিবে
            arr_match = re.search(r'\[\s*(-?\d+(\.\d+)?\s*,\s*)*-?\d+(\.\d+)?\s*\]', groq_output)
            if arr_match:
                learned_weights = json.loads(arr_match.group(0))
            else:
                # ফলব্যাক: ট্রাই টু পার্স সরাসরি
                learned_weights = json.loads(groq_output.strip())
                
            if len(learned_weights) == 24:
                payload_db = {"direction": direction, "weights": learned_weights}
                # Upsert এর জন্য 'direction' কলামটি অবশ্যই Primary বা Unique হতে হবে
                supabase.table("ml_weights").upsert(payload_db).execute()
                print(f"✅ Training Complete for {direction}!")
            else:
                print(f"❌ Error: Predicted {len(learned_weights)} hours. Need exactly 24.")
                
        except Exception as e:
            print(f"❌ JSON Parsing failed for {direction}: {e}")

        # ৫. ফ্রি-টিয়ার সেফটি গ্যাপ (Groq Rate limit protection)
        print("⏳ Cooling down for 25 seconds...")
        time.sleep(25)

if __name__ == "__main__":
    train_model()