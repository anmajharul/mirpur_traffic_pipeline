import os
import random
import logging
from dotenv import load_dotenv
from supabase import create_client

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def inject_synthetic_rain():
    # Load environment variables
    _BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(_BASE, '.env'), override=True)
    
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        logging.error("Missing Supabase credentials in .env")
        return
        
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    logging.info("Fetching recent traffic logs to inject synthetic rain...")
    # Fetch 1500 recent rows to sample from
    response = supabase.table("smart_eta_logs").select("id, speed_kmh, congestion_percent, aqi").order("created_at", desc=True).limit(1500).execute()
    
    data = response.data
    if not data:
        logging.error("No data found in smart_eta_logs.")
        return
        
    # Randomly select ~20% of the rows to simulate a rainy week
    sample_size = min(len(data), 300)
    rainy_rows = random.sample(data, sample_size)
    
    updates = []
    
    for row in rainy_rows:
        # Simulate rainfall between 2.0mm (Light) to 35.0mm (Heavy Monsoon)
        rain_mm = round(random.uniform(2.0, 35.0), 2)
        
        # Original values
        orig_speed = float(row.get('speed_kmh', 30.0) or 30.0)
        orig_cong = float(row.get('congestion_percent', 50.0) or 50.0)
        orig_aqi = float(row.get('aqi', 150.0) or 150.0)
        
        # Apply synthetic physics:
        # 1. Rain slows down traffic (heavier rain = slower speed)
        speed_penalty = 1.0 - min(0.6, (rain_mm / 60.0))  # Max 60% speed drop
        new_speed = round(orig_speed * speed_penalty, 2)
        
        # 2. Rain increases congestion
        cong_bump = 1.0 + (rain_mm / 40.0) # Up to roughly double congestion
        new_cong = round(min(100.0, orig_cong * cong_bump), 2)
        
        # 3. Rain washes out pollution (improves AQI)
        aqi_drop = 1.0 - min(0.8, (rain_mm / 30.0)) # Heavy rain clears the air drastically
        new_aqi = round(max(15.0, orig_aqi * aqi_drop), 2)
        
        # Ensure values don't break database schemas
        new_speed = max(2.0, new_speed)
        
        # Queue the update
        updates.append({
            "id": row['id'],
            "rain_mm": rain_mm,
            "speed_kmh": new_speed,
            "congestion_percent": new_cong,
            "aqi": new_aqi
        })
        
    logging.info(f"Injecting {len(updates)} synthetic rainy events into Supabase...")
    
    # Update row by row to prevent missing column constraints
    success_count = 0
    for row in updates:
        try:
            update_payload = {
                "rain_mm": row["rain_mm"],
                "speed_kmh": row["speed_kmh"],
                "congestion_percent": row["congestion_percent"],
                "aqi": row["aqi"]
            }
            supabase.table("smart_eta_logs").update(update_payload).eq("id", row["id"]).execute()
            success_count += 1
        except Exception as e:
            logging.error(f"Failed to update row {row['id']}: {e}")
            
    logging.info(f"Synthetic Data Injection Complete! Successfully updated {success_count} rows.")
    logging.info("Next steps: Run `python backend/train_weather_ml.py` and `python backend/trainer_xgb.py` to see the new curves and SHAP values.")

if __name__ == "__main__":
    inject_synthetic_rain()
