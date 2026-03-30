-- =========================================================================
-- Q1 DEFENSIBLE SQL SCHEMA: smart_eta_logs
-- =========================================================================
-- Purpose: 
-- Stores real-time, multi-source fused traffic data for Mirpur-10 corridors.
-- Serves as the primary empirical dataset for the XGBoost training pipeline.
--
-- Q1 Publication Modifications:
-- 1. `mrt_status` and `is_anomaly` strictly defined as integer (0/1) for 
--    cross-platform ML pipeline compatibility, avoiding boolean cast errors.
-- 2. Added `pcu_index` and `pcu_source` to quantify the Mixed-Traffic state.
--    (References: Greenshields 1934, HCM 7e §11, JICA 2015 BD-P18).
-- 3. Features derived linearly from the target (e.g., speed, tti, congestion)
--    are securely isolated here and programmatically stripped before training 
--    in `data_loader.py` to prevent data leakage (Kaufman et al. 2012).
-- =========================================================================

CREATE TABLE IF NOT EXISTS public.smart_eta_logs (
    id bigserial NOT NULL,
    created_at timestamp with time zone NULL DEFAULT now(),
    direction text NOT NULL,
    mapbox_speed double precision NULL,
    waze_speed double precision NULL,
    congestion_percent double precision NULL,
    severity_status text NULL,
    severity_index integer NULL,
    data_confidence double precision NULL,
    rain_mm double precision NULL,
    temperature double precision NULL,
    wind_speed double precision NULL,
    visibility_km double precision NULL,
    uv_index double precision NULL,
    weather_condition text NULL,
    -- Air Quality Index using EPA NowCast formula
    aqi double precision NULL, 
    pm2_5 double precision NULL,
    pm10 double precision NULL,
    co_level double precision NULL,
    no2_level double precision NULL,
    time_slot text NULL,
    -- Operations indicators (int 0/1)
    mrt_status integer NULL DEFAULT 0,
    mrt_headway integer NULL DEFAULT 0,
    is_anomaly integer NULL DEFAULT 0,
    anomaly_score numeric NULL,
    reason text NULL,
    humidity numeric NULL,
    -- Strict ML Target variable
    actual_eta_min numeric NULL,
    day_of_week text NULL,
    travel_time_sec double precision NULL,
    distance_km double precision NULL,
    -- Data robustness metric
    source_count integer NULL,
    speed_kmh double precision NULL,
    free_flow_kmh double precision NULL,
    speed_ratio double precision NULL,
    prediction_time timestamp without time zone NULL,
    horizon_min integer NULL,
    corridor_id text NULL,
    tti double precision NULL,
    -- OGC standard geospatial geometry
    geom geometry NULL,
    -- Mixed-flow Traffic Quantification
    pcu_index double precision NULL,
    pcu_source text NULL,
    
    CONSTRAINT smart_eta_logs_pkey PRIMARY KEY (id)
) TABLESPACE pg_default;

-- Optimised B-Tree indices to support fast time-series analytical queries
CREATE INDEX IF NOT EXISTS idx_smart_meta 
    ON public.smart_eta_logs USING btree (direction, created_at) TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_eta_direction_time 
    ON public.smart_eta_logs USING btree (direction, created_at DESC) TABLESPACE pg_default;
