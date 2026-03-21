import os
import json
import requests
import time
import re
import logging
from supabase import create_client, Client
from datetime import datetime, timezone

# ==========================================
# ⚙️ LOGGING & CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    logging.critical("❌ AI Training Keys missing! Terminating.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================
# 🧠 AI TRAINING ENGINE (Llama-3.3)
# ==========================================
def train_model():
    logging.info("⏳ Starting Automated AI Training Cycle...")
    
    try:
        # ✅ ফিক্সড: এখন আমরা 'smart_eta_logs' থেকে ডেটা নিচ্ছি
        # কলামের নামগুলোও নতুন স্কিমা অনুযায়ী আপডেট করা হয়েছে
        response = supabase.table("smart_eta_logs") \
            .select("direction, speed_kmh, severity_index, created_at") \
            .order("created_at", desc=True) \
            .limit(5000) \
            .execute()
        data = response.data
    except Exception as e:
        logging.error(f"❌ Supabase Fetch Error: {e}")
        return

    # রিসার্চ থ্রেশহোল্ড: অন্তত ১০০টি রেকর্ড না থাকলে মডেল স্ট্যাটিস্টিক্যালি ইনভ্যালিড
    if not data or len(data) < 100:
        logging.warning(f"❌ Insufficient Data. Current: {len(data) if data else 0}. Need >= 100.")
        return

    # ১. ডেটা অ্যাগ্রিগেশন (Direction -> Hour -> Stats)
    directions_stats = {}
    for row in data:
        direction = row.get('direction')
        time_field = row.get('created_at') # নতুন স্কিমায় 'created_at' ব্যবহার করা হয়েছে
        speed = row.get('speed_kmh')
        sev_idx = row.get('severity_index')
        
        if not all([direction, time_field]) or speed is None: continue
            
        try:
            # টাইমস্ট্যাম্প পার্সিং (ISO format handling)
            time_obj = datetime.fromisoformat(time_field.replace('Z', '+00:00'))
            hour = time_obj.hour
            
            if direction not in directions_stats: directions_stats[direction] = {}
            if hour not in directions_stats[direction]: 
                directions_stats[direction][hour] = {"speed_sum": 0, "sev_sum": 0, "count": 0}
            
            directions_stats[direction][hour]["speed_sum"] += speed
            directions_stats[direction][hour]["sev_sum"] += (sev_idx if sev_idx is not None else 0)
            directions_stats[direction][hour]["count"] += 1
        except: continue

    # ২. প্রতি ডিরেকশনের জন্য AI লার্নিং
    for direction, hourly_map in directions_stats.items():
        # রিসার্চ সামারি তৈরি (LLM এর ইনপুট হিসেবে)
        history_summary = ""
        for h in sorted(hourly_map.keys()):
            avg_s = round(hourly_map[h]["speed_sum"] / hourly_map[h]["count"], 2)
            avg_sev = round(hourly_map[h]["sev_sum"] / hourly_map[h]["count"], 2)
            history_summary += f"Hour {h}: Avg Speed {avg_s} km/h (Severity {avg_sev})\n"
            
        logging.info(f"🤖 Training Llama-3.3 for corridor: {direction}...")

        # সায়েন্টিফিক ইন্সট্রাকশন ফর গ্রক (Groq)
        sys_instruction = (
            f"You are a Traffic Flow Optimization Model for Mirpur-10, Dhaka. Region: {direction}. "
            "Your task is to perform a non-linear regression based on historical speed and severity logs "
            "to predict a 24-hour baseline speed profile (Hour 0 to 23). "
            "Output ONLY a raw JSON array of 24 floating-point numbers. No prose, no markdown labels."
        )
        
        prompt = f"Historical Speed Profile:\n{history_summary}\n\nGenerate exactly 24 elements JSON array for speed baseline."

        payload = {
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.2, # লো টেম্পারেচার মানে বেশি প্রিসাইজ প্রেডিকশন
            "messages": [
                {"role": "system", "content": sys_instruction},
                {"role": "user", "content": prompt}
            ]
        }

        # ৩. কলিং এপিআই উইথ রেট-লিমিট মেকানিজম
        try:
            res = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json=payload, timeout=60
            )
            if res.status_code == 200:
                raw_content = res.json()['choices'][0]['message']['content']
                # JSON ক্লিনিং
                match = re.search(r'\[\s*(-?\d+(\.\d+)?\s*,\s*)*-?\d+(\.\d+)?\s*\]', raw_content)
                if match:
                    weights = json.loads(match.group(0))
                    if len(weights) == 24:
                        # ৪. লার্নিং সেভ করা (ml_weights টেবিলে)
                        db_payload = {
                            "direction": direction, 
                            "weights": weights,
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        }
                        supabase.table("ml_weights").upsert(db_payload).execute()
                        logging.info(f"✅ Training Success for {direction}!")
                    else:
                        logging.error(f"❌ Length Error: Received {len(weights)} elements.")
            elif res.status_code == 429:
                logging.warning("⚠️ Groq Rate Limit. Skipping current corridor.")
                time.sleep(30)
            else:
                logging.error(f"❌ Groq Error {res.status_code}")
        except Exception as e:
            logging.error(f"❌ Pipeline Exception: {e}")

        time.sleep(15) # এপিআই লোড ম্যানেজমেন্ট

if __name__ == "__main__":
    train_model()