# ============================================================
# Dockerfile — Mirpur-10 Traffic Dashboard Inference API
# Cloud Run Service: 512Mi RAM, 1 vCPU, 1 Min-Instance (Cold Start Fix)
# ============================================================

FROM python:3.11-slim

# Metadata
LABEL maintainer="mirpur-traffic-ai"
LABEL service="inference-api"

# Prevents Python from buffering stdout/stderr (critical for Cloud Run logs)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install OS-level dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies FIRST (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY backend/ ./backend/
COPY web_app.py .

# Expose standard FastAPI port
EXPOSE 8000

# Start Uvicorn, mapping to Cloud Run's required port 8000
CMD ["uvicorn", "web_app:app", "--host", "0.0.0.0", "--port", "8000"]
