-- ============================================================
-- weather_ml_insights — ML Polynomial Regression Predictions
-- ============================================================
-- Stores pre-computed rain-impact curves trained by train_weather_ml.py
-- using Ridge Polynomial Regression on historical smart_eta_logs data.
--
-- Purpose: Frontend Rain tab → "ML Learned Curve" toggle reads from here.
-- Populated by: backend/train_weather_ml.py (run manually or on schedule)
-- Schema version: 1.0 (2026-04-07)
--
-- References:
--   EEA (2023). EMEP/EEA Guidebook — wet scavenging, AQI reduction
--   HCM 7th Ed. Ch.15 — rain speed penalty (10–17% range)
--   scikit-learn Ridge Regression (sklearn.linear_model.Ridge, alpha=1.0)
-- ============================================================

CREATE TABLE IF NOT EXISTS public.weather_ml_insights (
    id                          BIGSERIAL PRIMARY KEY,

    -- Rain intensity bin (mm/hr) — standard WMO buckets
    rain_bucket_mm              NUMERIC(6,2)  NOT NULL,

    -- ML predicted percentage speed penalty vs dry baseline
    -- (Polynomial Degree 2 Ridge Regression: speed_kmh ~ rain_mm)
    predicted_speed_drop_pct    NUMERIC(6,2)  NOT NULL DEFAULT 0,

    -- ML predicted AQI improvement pct (wet scavenging effect)
    -- (Linear Ridge: aqi ~ rain_mm — linear is safer for sparse heavy-rain data)
    predicted_aqi_drop_pct      NUMERIC(6,2)  NOT NULL DEFAULT 0,

    -- ML predicted congestion increase pct vs baseline congestion at 0mm
    -- (Polynomial Degree 2 Ridge Regression: congestion_percent ~ rain_mm)
    predicted_congestion_bump_pct NUMERIC(6,2) NOT NULL DEFAULT 0,

    -- Model confidence: sample_size / 10000 (naive normalization)
    confidence_score            NUMERIC(5,4)  NOT NULL DEFAULT 0,

    -- Number of smart_eta_logs rows used in training
    sample_size                 INTEGER       NOT NULL DEFAULT 0,

    -- Timestamp when this training run was executed (UTC)
    trained_at                  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Index for ordered reads by rain bucket (used by frontend chart)
CREATE INDEX IF NOT EXISTS idx_wml_rain_bucket ON public.weather_ml_insights (rain_bucket_mm ASC);

-- Allow anonymous frontend reads (matches RLS on smart_eta_logs)
ALTER TABLE public.weather_ml_insights ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow anonymous reads on weather_ml_insights"
    ON public.weather_ml_insights
    FOR SELECT TO anon USING (true);

-- Only service role can insert (train_weather_ml.py uses SUPABASE_KEY = service role)
CREATE POLICY "Allow service role inserts on weather_ml_insights"
    ON public.weather_ml_insights
    FOR INSERT TO service_role WITH CHECK (true);

CREATE POLICY "Allow service role deletes on weather_ml_insights"
    ON public.weather_ml_insights
    FOR DELETE TO service_role USING (true);
