import os
import json
import requests
from datetime import datetime
import re

# ==========================================
# CONFIGURATION & API KEYS
# ==========================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("ERROR: API Keys missing! Make sure they are set in GitHub Secrets.")
    exit(1)

SUPABASE_REST_URL = f"{SUPABASE_URL}/rest/v1"
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


def supabase_select(table: str, params: dict) -> list:
    url = f"{SUPABASE_REST_URL}/{table}"
    res = requests.get(url, headers=SUPABASE_HEADERS, params=params, timeout=20)
    if not res.ok:
        raise RuntimeError(f"Supabase select failed ({res.status_code}): {res.text}")
    return res.json()


def supabase_upsert(table: str, payload: dict, on_conflict: str | None = None) -> None:
    url = f"{SUPABASE_REST_URL}/{table}"
    headers = {
        **SUPABASE_HEADERS,
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    params = {"on_conflict": on_conflict} if on_conflict else None
    res = requests.post(url, headers=headers, params=params, json=payload, timeout=20)
    if not res.ok:
        raise RuntimeError(f"Supabase upsert failed ({res.status_code}): {res.text}")


# ==========================================
# FETCH DATA & TRAIN LLM FOR EACH DIRECTION
# ==========================================

def train_model():
    print("Fetching historical data from Supabase...")

    try:
        data = supabase_select(
            "traffic_records",
            {
                "select": "*",
                "order": "created_at.desc",
                "limit": "5000",
            },
        )
    except Exception as e:
        print(f"ERROR: Failed to fetch data from Supabase: {e}")
        return

    if not data:
        print("ERROR: No data found in Supabase (traffic_records table).")
        return

    # Separate data by direction
    directions_data = {}
    for row in data:
        direction = row.get("direction")
        if not direction:
            continue

        time_field = row.get("created_at")
        if not time_field:
            continue

        try:
            time_obj = datetime.fromisoformat(time_field.replace("Z", "+00:00"))
            hour = time_obj.hour

            if direction not in directions_data:
                directions_data[direction] = {}
            if hour not in directions_data[direction]:
                directions_data[direction][hour] = {"sum": 0, "count": 0}

            directions_data[direction][hour]["sum"] += row["speed_kmh"]
            directions_data[direction][hour]["count"] += 1
        except Exception:
            continue

    if not directions_data:
        print("ERROR: Failed to process directional hourly data.")
        return

    # Process each direction separately
    for direction, hourly_cal in directions_data.items():
        hist_str = ""
        for h in sorted(hourly_cal.keys()):
            avg_speed = round(hourly_cal[h]["sum"] / hourly_cal[h]["count"], 2)
            hist_str += f"Hour {h}: {avg_speed} km/h\n"

        print(f"Training AI for: {direction}...")

        # Call Groq API
        sys_instruction = (
            "You are an ML model analyzing traffic for "
            f"{direction} at Mirpur-10, Dhaka. "
            "Output ONLY a raw JSON array of exactly 24 numbers. "
            "These numbers represent the optimal predicted speed (km/h) "
            "for hours 0 to 23 based on the historical data. "
            "Smooth the curve. NO MARKDOWN, NO TEXT."
        )
        prompt = f"Historical Data for {direction}:\n{hist_str}\nGenerate the 24-element array."

        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": sys_instruction},
                {"role": "user", "content": prompt},
            ],
        }

        res = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60,
        )

        if res.status_code != 200:
            print(f"ERROR: Groq API failed for {direction}: {res.text}")
            continue

        groq_output = res.json()["choices"][0]["message"]["content"]

        try:
            arr_match = re.search(r"\[(.*?)\]", groq_output, re.DOTALL)
            if arr_match:
                learned_weights = json.loads(f"[{arr_match.group(1)}]")
            else:
                learned_weights = json.loads(groq_output.strip())

            if len(learned_weights) == 24:
                payload_db = {
                    "direction": direction,
                    "weights": learned_weights,
                }
                try:
                    supabase_upsert("ml_weights", payload_db, on_conflict="direction")
                except Exception as e:
                    print(f"ERROR: Failed to save weights for {direction}: {e}")
                    continue

                print(
                    f"Saved weights for {direction}! Curve starts: {learned_weights[:3]}"
                )
            else:
                print(
                    "ERROR: Invalid array length for "
                    f"{direction}. Expected 24, got {len(learned_weights)}."
                )

        except Exception as e:
            print(f"ERROR: Failed to parse JSON for {direction}: {e}")


if __name__ == "__main__":
    train_model()
