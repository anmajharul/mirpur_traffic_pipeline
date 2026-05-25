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

    -- =========================================================================
    -- FATAL-1 FIX: ML Feature columns previously missing from schema
    -- data_collector.py collects all of these; Supabase was silently dropping
    -- them. All are in FEATURE_COLS in trainer_xgb.py — NULL in DB = NULL in
    -- training = model learns nothing from these features.
    -- =========================================================================

    -- ── OSRM routing divergence (spatial baseline feature) ──────────────────
    -- osrm_divergence = (osrm_speed - mapbox_speed) / osrm_speed
    -- Positive → current slower than historical (congestion signal)
    -- Reference: Luxen & Vetter (2011). ACM SIGSPATIAL.
    osrm_divergence double precision NULL,
    osrm_eta_min double precision NULL,       -- OSRM baseline ETA for Paper Table 3

    -- ── Weather condition encoding ───────────────────────────────────────────
    -- Ordinal: 0=Clear, 1=Cloudy/Fog, 2=Rain, 3=Storm
    weather_condition_encoded integer NULL,
    weather_code integer NULL,                -- raw Tomorrow.io weatherCode integer

    -- ── Binary temporal/operational flags ────────────────────────────────────
    -- All stored as integer 0/1 for ML pipeline compatibility
    -- Reference: schema_smart_eta_logs header note §Q1-2
    is_holiday integer NULL DEFAULT 0,        -- 1 on BD public holidays & weekends
    is_peak_hour integer NULL DEFAULT 0,      -- 1 during 07-10 or 16-20 BDT (RSTP)
    is_weekend integer NULL DEFAULT 0,        -- 1 on Friday(4) or Saturday(5)
    is_monsoon integer NULL DEFAULT 0,        -- 1 during June-September (WMO)
    is_extreme_weather integer NULL DEFAULT 0, -- 1 when rain_mm > 10 (WMO Heavy)

    -- ── Temporal integer features ─────────────────────────────────────────────
    hour_of_day integer NULL,                 -- 0-23 BDT integer hour
    month integer NULL,                       -- 1-12

    -- ── Novel Q1 rain/weather derived features ────────────────────────────────
    -- Reference: Agarwal et al. (2022) TR Part D, 106, 103258.
    rain_accumulation_3h double precision NULL,   -- rolling 3h rainfall sum (mm)
    rain_x_peak_hour double precision NULL,       -- rain_mm × is_peak_hour interaction

    -- Reference: Ivanović et al. (2022) Sustainability 14(9), 4985.
    visibility_penalty double precision NULL,     -- 0-0.1 penalty factor

    -- WMO rainfall intensity category (0=None,1=Light,2=Mod,3=Heavy,4=Violent)
    -- Reference: WMO (2018) CIMO Vol.I §6.7.1
    wmo_rain_category integer NULL DEFAULT 0,

    -- Reference: Zhang & Batterman (2013) Science of Total Environment.
    emission_congestion_cross double precision NULL,  -- |osrm_divergence| × PM2.5

    CONSTRAINT smart_eta_logs_pkey PRIMARY KEY (id)
) TABLESPACE pg_default;

-- Optimised B-Tree indices to support fast time-series analytical queries
CREATE INDEX IF NOT EXISTS idx_smart_meta 
    ON public.smart_eta_logs USING btree (direction, created_at) TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_eta_direction_time 
    ON public.smart_eta_logs USING btree (direction, created_at DESC) TABLESPACE pg_default;
