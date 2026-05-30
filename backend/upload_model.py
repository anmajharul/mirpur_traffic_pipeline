"""
upload_model.py - Supabase Storage uploader for weekly XGBoost artifacts
=======================================================================
Purpose:
- Upload the latest trained XGBoost JSON artifact to Supabase Storage
- Keep a stable latest/ path for inference services
- Keep a timestamped archive copy for rollback and audit

REFERENCES:
[1] Supabase Python Storage upload docs
    https://supabase.com/docs/reference/python/storage-from-upload
[2] Chen, T., & Guestrin, C. (2016). XGBoost: A scalable tree boosting system.
    KDD '16. DOI: https://doi.org/10.1145/2939672.2939785
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from supabase import create_client
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DEFAULT_BUCKET = "ml-models"
DEFAULT_LOCAL_ARTIFACT = "model_ml_weight.json"
DEFAULT_REMOTE_LATEST = "latest/model_ml_weight.json"


def upload_model_artifact() -> None:
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY are required")

    bucket_name = os.environ.get("SUPABASE_MODEL_BUCKET") or DEFAULT_BUCKET
    local_path = Path(os.environ.get("MODEL_ARTIFACT_PATH") or DEFAULT_LOCAL_ARTIFACT)
    latest_path = os.environ.get("MODEL_REMOTE_LATEST_PATH") or DEFAULT_REMOTE_LATEST

    if not local_path.exists():
        print(f"Model artifact not found: {local_path}. Skipping upload.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = f"archives/model_ml_weight_{timestamp}.json"

    supabase = create_client(supabase_url, supabase_key)
    storage = supabase.storage.from_(bucket_name)

    with local_path.open("rb") as artifact_file:
        storage.upload(
            path=latest_path,
            file=artifact_file,
            file_options={"cache-control": "3600", "upsert": "true"},
        )

    with local_path.open("rb") as archive_file:
        storage.upload(
            path=archive_path,
            file=archive_file,
            file_options={"cache-control": "31536000", "upsert": "false"},
        )

    print(
        f"Uploaded {local_path.name} to bucket '{bucket_name}' "
        f"as '{latest_path}' and '{archive_path}'"
    )


if __name__ == "__main__":
    upload_model_artifact()
