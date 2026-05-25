import os
from pathlib import Path
from supabase import create_client
import logging

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def download_models():
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY are required")

    supabase = create_client(supabase_url, supabase_key)
    bucket_name = "ml-models"
    
    models_to_download = [
        ("latest/model_ml_weight.json", "model_ml_weight.json"),
        ("latest/model_mlp_weight.pt", "model_mlp_weight.pt")
    ]
    
    for remote_path, local_path in models_to_download:
        try:
            res = supabase.storage.from_(bucket_name).download(remote_path)
            with open(local_path, "wb") as f:
                f.write(res)
            logging.info(f"Successfully downloaded {remote_path} to {local_path}")
        except Exception as e:
            logging.warning(f"Could not download {remote_path}: {e}")

if __name__ == "__main__":
    download_models()
