import os
import json
import requests
import time
import re
from supabase import create_client, Client
from datetime import datetime, timezone

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
# 🧠 FETCH & TRAIN LOGIC
# ==========================================
def train_model():
    print(f"⏳ [{datetime.now().strftime('%H:%M:%S')}] Starting AI Training Cycle...")
    
    try:
        # লেটেস্ট ৫০০০ রেকর্ড রিসার্চের জন্য নিচ্ছি (Inner Speed & Severity সহ)
        response = supabase.table("traffic_records") \
            .select("direction, inner_speed, severity_index, timestamp") \
            .order("timestamp", desc=True) \
            .limit(5000) \
            .execute()
        data = response.data
    except Exception as e:
        print(f"❌ Supabase Fetch Error: {e}")
        return

    if not data or len(data) < 100:
        print("❌ Not enough data to train. Need at least 100 records.")
        return

    # ১. ডাটা প্রসেসিং (Direction -> Hour -> Statistics)
    # আমরা এখন স্পিড এবং জ্যামের ইনডেক্স—দুটোই ট্র্যাক করবো
    directions_stats = {}
    for row in data:
        direction = row.get('direction')
        time_field = row.get('timestamp')
        i_speed = row.get('inner_speed')
        s_idx = row.get('severity_index')
        
        if not all([direction, time_field]) or i_speed is None: continue
            
        try:
            time_obj = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
            hour = time_obj.hour
            
            if direction not in directions_stats: directions_stats[direction] = {}
            if hour not in directions_stats[direction]: 
                directions_stats[direction][hour] = {"speed_sum": 0, "severity_sum": 0, "count": 0}
            
            directions_stats[direction][hour]["speed_sum"] += i_speed
            directions_stats[direction][hour]["severity_sum"] += (s_idx if s_idx is not None else 0)
            directions_stats[direction][hour]["count"] += 1
        except: continue

    # ২. প্রতিটি ডিরেকশনের জন্য AI ট্রেনিং শুরু
    for direction, hourly_map in directions_stats.items():
        # রিসার্চের জন্য ডাটা স্ট্রিং তৈরি
        history_summary = ""
        for h in sorted(hourly_map.keys()):
            avg_s = round(hourly_map[h]["speed_sum"] / hourly_map[h]["count"], 2)
            avg_sev = round(hourly_map[h]["severity_sum"] / hourly_map[h]["count"], 2)
            history_summary += f"H{h}: {avg_s}km/h (Sev:{avg_sev})\n"
            
        print(f"\n🤖 Training Llama-3.3 for: {direction} (Using Inner-Node Data)...")

        # সিস্টেম ইন্সট্রাকশন (মডেলকে বাধ্য করা যেন সে শুধু JSON দেয়)
        sys_instruction = (
            f"You are a Traffic Prediction Expert for Mirpur-10, Dhaka. Target: {direction}. "
            "Task: Based on historical Inner-Node speeds and Severity Index (0-3), "
            "predict a 24-hour optimized speed baseline (Hour 0-23). "
            "CRITICAL: Output ONLY a valid JSON array of 24 numbers. No text before/after. "
            "Example: [30.5, 32.1, ...]"
        )
        
        prompt = f"Historical Intersection Data:\n{history_summary}\n\nReturn Exactly 24 elements JSON array."

        payload = {
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.25, # কিছুটা ফ্লেক্সিবল রাখা হয়েছে স্মুথ গ্রাফের জন্য
            "messages": [
                {"role": "system", "content": sys_instruction},
                {"role": "user", "content": prompt}
            ]
        }

        # ৩. স্মার্ট কল উইথ রেট-লিমিট হ্যান্ডলিং
        groq_raw = ""
        for attempt in range(3):
            try:
                res = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                    json=payload, timeout=45
                )
                if res.status_code == 200:
                    groq_raw = res.json()['choices'][0]['message']['content']
                    break
                elif res.status_code == 429:
                    print(f"⚠️ Rate limit! Waiting 35s (Attempt {attempt+1})...")
                    time.sleep(35)
                else:
                    print(f"❌ Error {res.status_code}: {res.text}")
                    break
            except Exception as e:
                print(f"❌ Request Failed: {e}")
                time.sleep(10)

        if not groq_raw: continue

        # ৪. ক্লিনআপ ও সুপাবেসে সেভ
        try:
            # Regex ব্যবহার করে শুধু ব্র্যাকেটের ভেতরের অংশটুকু নেওয়া (অতিরিক্ত টেক্সট বাদ দিতে)
            match = re.search(r'\[\s*(-?\d+(\.\d+)?\s*,\s*)*-?\d+(\.\d+)?\s*\]', groq_raw)
            if match:
                learned_weights = json.loads(match.group(0))
                
                if len(learned_weights) == 24:
                    # 'ml_weights' টেবিলে আপসার্ট করা
                    db_payload = {
                        "direction": direction, 
                        "weights": learned_weights,
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }
                    supabase.table("ml_weights").upsert(db_payload).execute()
                    print(f"✅ AI Weights updated for {direction}!")
                else:
                    print(f"❌ Received {len(learned_weights)} elements, expected 24.")
            else:
                print(f"❌ AI Response format error for {direction}")
                
        except Exception as e:
            print(f"❌ Parsing failed for {direction}: {e}")

        # ৫. কুলডাউন (Groq এর ফ্রি টিয়ারে রিকোয়েস্টের গ্যাপ জরুরি)
        print("⏳ Cooling down for 20 seconds...")
        time.sleep(20)

if __name__ == "__main__":
    train_model()