-- =========================================================================
-- Q1 DEFENSIBLE SQL SCHEMA: model_metrics
-- =========================================================================
-- Purpose:
-- Stores the time-series cross-validation performance of the daily XGBoost
-- ETA retraining jobs running in GitHub Actions.
--
-- Q1 Publication Modifications:
-- This table natively requires logging three primary evaluation metrics
-- (MAE, RMSE, MAPE) coupled with 95% Bootstrap Confidence Intervals.
-- Reporting all three is an active standard for Q1 Transportation research 
-- (see Bergmeir & Benitez 2012, Efron & Tibshirani 1993).
-- Note: 'ml_weights' table was deprecated to migrate XGBoost artifacts 
-- to physical JSON blobs in Supabase Storage.
-- =========================================================================

CREATE TABLE IF NOT EXISTS public.model_metrics (
    id bigserial NOT NULL,
    -- Time representation mapped to daily retraining cycle
    timestamp timestamp with time zone NULL DEFAULT now(),
    model_type text NOT NULL,
    -- Data size & capacity checks
    n_samples integer NULL,
    n_features integer NULL,
    
    -- Absolute Error (Performance Measure)
    cv_mean_mae double precision NULL,
    cv_std_mae double precision NULL,
    
    -- Root Mean Square Error (Outlier / Extreme Jam Sensitivity)
    cv_mean_rmse double precision NULL,
    cv_std_rmse double precision NULL,
    
    -- Mean Absolute Percentage Error (Percentage Representation)
    cv_mean_mape double precision NULL,
    cv_std_mape double precision NULL,
    
    -- 95% Confidence Intervals (Statistical Robustness)
    cv_ci95_lower double precision NULL,
    cv_ci95_upper double precision NULL,
    
    -- Fold information (Required: >= 5 to prevent bias per our configs)
    cv_n_folds integer NULL,
    
    -- Track features directly injected into the XGB model
    features_used text NULL,
    
    CONSTRAINT model_metrics_pkey PRIMARY KEY (id)
) TABLESPACE pg_default;

-- Optimised index for fetching the latest metric per run
CREATE INDEX IF NOT EXISTS idx_model_metrics_time 
    ON public.model_metrics USING btree ("timestamp" DESC) TABLESPACE pg_default;
