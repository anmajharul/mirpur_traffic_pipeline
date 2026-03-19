# Mirpur Traffic AI

This project focuses on Mirpur‑10 and collects, stores, and learns traffic, weather, and air‑quality data to generate AI‑based 24‑hour speed profiles. Data is saved to Supabase, backed up to CSV, and an automated training job runs using Groq Llama‑3.3.

## What It Does

- Collects traffic speed from 4 directions toward Mirpur‑10 using HERE API
- Adds weather + AQI data using WeatherAPI
- Inserts records into Supabase `traffic_records`
- Maintains a local/Git CSV backup at `backend/traffic_data_backup.csv`
- Trains 24‑hour speed profiles per direction using Groq Llama‑3.3
- Includes an alternate simple pipeline (TomTom + Open‑Meteo) in `mirpur_data_collector`
- Automates hourly data collection and 6‑hour training via GitHub Actions

## Architecture (Short)

1. `backend/data_collector.py`  
2. HERE + WeatherAPI → record creation  
3. Supabase `traffic_records` table  
4. CSV backup (committed to GitHub)  
5. `backend/auto_trainer.py` → Groq Llama‑3.3 → `ml_weights` table

## Folder Structure

- `backend/`
- `backend/data_collector.py` (main data collector)
- `backend/auto_trainer.py` (LLM training)
- `backend/requirements.txt`
- `backend/traffic_data_backup.csv`
- `mirpur_data_collector/`
- `mirpur_data_collector/data_pipeline.py` (alternate pipeline)
- `mirpur_data_collector/requirements.txt`
- `.github/workflows/` (automation)

## Requirements

- Python 3.10+  
- Supabase project  
- API Keys: HERE, WeatherAPI, Groq (and TomTom if you run the alternate pipeline)

## Environment Variables

| Variable | Used In | Description |
|---|---|---|
| `SUPABASE_URL` | `backend/data_collector.py`, `backend/auto_trainer.py` | Supabase Project URL |
| `SUPABASE_KEY` | all Python scripts | Supabase Service Role/Anon Key |
| `HERE_API_KEY` | `backend/data_collector.py` | HERE Routing API |
| `WEATHER_API_KEY` | `backend/data_collector.py` | WeatherAPI |
| `GROQ_API_KEY` | `backend/auto_trainer.py` | Groq LLM API |
| `TOMTOM_API_KEY` | `mirpur_data_collector/data_pipeline.py` | TomTom Traffic API |

## Local Setup (Backend Collector)

```powershell
cd e:\mirpur_traffic_ai\backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run it:

```powershell
python data_collector.py
```

## Local Setup (Auto Trainer)

```powershell
cd e:\mirpur_traffic_ai\backend
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python auto_trainer.py
```

## Alternate Data Pipeline (TomTom + Open‑Meteo)

```powershell
cd e:\mirpur_traffic_ai\mirpur_data_collector
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python data_pipeline.py
```

## GitHub Actions Automation

- `data_collector.yml`  
  Runs hourly, executes `backend/data_collector.py` and commits CSV backup.
- `ml_trainer.yml`  
  Runs every 6 hours, executes `backend/auto_trainer.py`.

You must set these secrets in GitHub:

- `SUPABASE_URL`
- `SUPABASE_KEY`
- `HERE_API_KEY`
- `WEATHER_API_KEY`
- `GROQ_API_KEY`

## Recommended Supabase Schema

### `traffic_records`

Both scripts write into this table, so treat some columns as optional.

- `timestamp` (text or timestamptz)
- `direction` (text)
- `destination` (text)
- `speed_kmh` (float)
- `congestion_percent` (float)
- `travel_time_sec` (int, optional)
- `temperature` (float, optional)
- `rain_mm` (float, optional)
- `wind_speed` (float, optional)
- `wind_dir` (text, optional)
- `visibility_km` (float, optional)
- `uv_index` (float, optional)
- `weather_condition` (text, optional)
- `aqi` (int, optional)
- `pm2_5`, `pm10`, `co_level`, `no2_level` (float, optional)
- `sunrise`, `sunset`, `moon_phase` (text, optional)

### `ml_weights`

- `direction` (text, unique)
- `weights` (json or jsonb) — 24‑hour speed profile

### `weather_data` (if you run `data_pipeline.py`)

- `temperature` (float)
- `wind_speed` (float)
- `rain_mm` (float)
- `timestamp` (default now, optional)

## Configuration

Update `CENTER` and `LOCATIONS` in `backend/data_collector.py` to collect data for a different area or routes.

## Notes

- CSV backup file: `backend/traffic_data_backup.csv`
- `auto_trainer.py` expects a raw 24‑element JSON array from Groq. If output includes extra text, parsing will fail.


# Author

Majharul Islam  
Civil Engineering Student  
Bangladesh University of Business and Technology (BUBT)

Research Focus:
Transportation Engineering  
Travel Behavior Analysis  
Discrete Choice Modeling

[![Portfolio](https://img.shields.io/badge/Website-anmajharul.bd-blue?style=for-the-badge&logo=googlechrome)](https://anmajharul.bd) 

© Majharul Islam – Research Portfolio
