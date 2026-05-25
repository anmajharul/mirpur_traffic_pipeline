-- Q1 DEFENSIBLE ML FORECAST TABLE
-- Stores 24-hour ahead XGBoost predictions for diurnal traffic flows.
-- Requires unique constraint on target_time + direction to prevent duplicates.

CREATE TABLE IF NOT EXISTS public.ml_forecasts (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    target_time_utc TIMESTAMPTZ NOT NULL,
    target_hour INT NOT NULL,
    direction TEXT NOT NULL,
    predicted_speed_kmh NUMERIC NOT NULL,
    predicted_congestion_percent NUMERIC NOT NULL,
    tcn_predicted_speed_kmh NUMERIC,
    tcn_predicted_congestion_percent NUMERIC,
    mlp_predicted_speed_kmh NUMERIC,
    mlp_predicted_congestion_percent NUMERIC,
    UNIQUE(target_time_utc, direction)
);

-- Add index for fast querying by the frontend (fetching 'tomorrow' forecasts)
CREATE INDEX IF NOT EXISTS idx_ml_forecasts_time ON public.ml_forecasts(target_time_utc);
