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

    -- ── Experiment identity ──────────────────────────────────────────────
    -- Versioned UTC timestamp slug (e.g. 20260514T070000Z) for audit trail.
    -- Reference: Sculley et al. (2015) NeurIPS — hidden technical debt in ML.
    model_version text NULL,

    -- ── Dataset summary ──────────────────────────────────────────────────
    n_samples integer NULL,
    n_features integer NULL,
    train_rows integer NULL,         -- rows used for training (80% of n_samples)
    test_rows integer NULL,          -- rows used for hold-out evaluation (20%)
    split_ratio double precision NULL DEFAULT 0.8,

    -- ── Incremental Learning Checkpoint ──────────────────────────────────────
    -- UTC timestamp of the latest data row used in this training run.
    -- On next run, data_loader loads only rows AFTER (data_cutoff_time - overlap).
    -- Reference: Losing et al. (2018). Incremental on-line learning.
    --   Neurocomputing 275, 1261-1274. https://doi.org/10.1016/j.neucom.2017.06.084
    data_cutoff_time timestamp with time zone NULL,

    -- ── Walk-forward CV metrics (primary) ────────────────────────────────
    -- 5-fold walk-forward per Bergmeir & Benítez (2012).
    -- Reference: https://doi.org/10.1016/j.ins.2011.12.028
    cv_mean_mae double precision NULL,
    cv_std_mae double precision NULL,
    cv_mean_rmse double precision NULL,
    cv_std_rmse double precision NULL,
    cv_mean_mape double precision NULL,
    cv_std_mape double precision NULL,
    cv_mean_smape double precision NULL,
    cv_std_smape double precision NULL,
    cv_ci95_lower double precision NULL,
    cv_ci95_upper double precision NULL,
    cv_n_folds integer NULL,

    -- ── Hold-out Test Set metrics ─────────────────────────────────────────
    -- Strict 80/20 temporal split; no shuffling (Bergmeir & Benítez 2012).
    model_mae double precision NULL,
    model_rmse double precision NULL,
    model_mape double precision NULL,
    model_smape double precision NULL,
    model_r2 double precision DEFAULT 0.0,   -- Explained variance (NO4)
    mae_ci_lower double precision NULL,
    mae_ci_upper double precision NULL,
    rmse_ci_lower double precision NULL,
    rmse_ci_upper double precision NULL,

    -- ── Baseline & improvement (Paper Table 3) ───────────────────────────
    -- Baseline 1: Historical average (per-corridor mean, train set only).
    -- Reference: Hyndman & Koehler (2006). DOI: 10.1016/j.ijforecast.2006.03.001
    baseline_mae double precision NULL,
    baseline_rmse double precision NULL,
    baseline_mape double precision NULL,
    baseline_smape double precision NULL,
    improvement_mae_pct double precision NULL,
    improvement_rmse_pct double precision NULL,

    -- ── Error distribution ────────────────────────────────────────────────
    error_mean double precision NULL,
    error_std double precision NULL,

    -- ── Corridor-level breakdown ──────────────────────────────────────────
    -- JSON dict: {"North (Mirpur-11 to 10)": 1.23, "South ...": 2.34, ...}
    corridor_mae jsonb NULL,

    -- ── Metadata ─────────────────────────────────────────────────────────
    features_used text NULL,
    notes text NULL,

    -- ── Advanced Q1 Metrics (TCN-TFT Multi-Horizon & Probabilistic) ───────
    -- Reference: Lim et al. (2021) TFT DOI: 10.1016/j.ijforecast.2021.03.040
    -- Reference: Gneiting & Raftery (2007) DOI: 10.1198/016214506000001437
    probabilistic_metrics jsonb DEFAULT '{}'::jsonb, -- e.g. {"PICP": 0.85, "MPIW": 12.5, "q_loss": ...}
    multi_horizon_metrics jsonb DEFAULT '{}'::jsonb, -- e.g. {"mae_step_1": 4.2, "mae_step_6": 8.9}

    -- ── Model-Agnostic Hyperparameter Storage (Hybrid JSONB) ──────────────
    -- Each model type stores its own hyperparameters as a JSON object.
    -- This avoids NULL pollution from model-specific columns in a shared table.
    --
    -- XGBoost example:
    --   {"n_estimators": 200, "max_depth": 6, "learning_rate": 0.05,
    --    "subsample": 0.8, "colsample_bytree": 0.8}
    --
    -- MLP example:
    --   {"hidden_sizes": [256, 128, 64], "dropout_rate": 0.2,
    --    "learning_rate": 0.001, "batch_size": 64, "max_epochs": 200, "patience": 20}
    --
    -- TCN-TFT example:
    --   {"seq_len": 12, "hidden_size": 32, "num_heads": 4,
    --    "kernel_size": 2, "dropout": 0.2, "patience": 5, "max_epochs": 50}
    --
    -- Query example (filter by XGBoost max_depth):
    --   SELECT * FROM model_metrics WHERE model_specific_params->>'max_depth' = '6';
    model_specific_params jsonb DEFAULT '{}'::jsonb,

    -- ── Legacy XGBoost-specific columns (kept for backward compatibility) ──
    -- New inserts should use model_specific_params JSONB instead.
    -- Reference: Chen & Guestrin (2016) KDD 2016.
    --   https://doi.org/10.1145/2939672.2939785
    n_estimators integer NULL,
    max_depth integer NULL,
    learning_rate double precision NULL,
    subsample double precision NULL,
    colsample_bytree double precision NULL,
    artifact_path text NULL,   -- Supabase Storage path for model artifact

    CONSTRAINT model_metrics_pkey PRIMARY KEY (id)
) TABLESPACE pg_default;

-- Optimised index for fetching the latest metric per run
CREATE INDEX IF NOT EXISTS idx_model_metrics_time
    ON public.model_metrics USING btree ("timestamp" DESC) TABLESPACE pg_default;

-- Index for efficient per-model-type queries (Paper Table 3 generation)
CREATE INDEX IF NOT EXISTS idx_model_metrics_type
    ON public.model_metrics USING btree (model_type) TABLESPACE pg_default;

-- GIN index for fast JSONB hyperparameter queries
-- e.g. WHERE model_specific_params->>'max_depth' = '6'
CREATE INDEX IF NOT EXISTS idx_model_metrics_params
    ON public.model_metrics USING gin (model_specific_params) TABLESPACE pg_default;

-- Migration: Add model_r2 if it was missing in an older deployment
ALTER TABLE public.model_metrics
ADD COLUMN IF NOT EXISTS model_r2 double precision DEFAULT 0.0;

-- ১. আগের রেকর্ডগুলোতে NULL থাকলে সেগুলো পালটে ডেফল্ট ভ্যালু বসাবে
UPDATE public.model_metrics SET model_specific_params = '{}'::jsonb WHERE model_specific_params IS NULL;
UPDATE public.model_metrics SET probabilistic_metrics = '{}'::jsonb WHERE probabilistic_metrics IS NULL;
UPDATE public.model_metrics SET multi_horizon_metrics = '{}'::jsonb WHERE multi_horizon_metrics IS NULL;
UPDATE public.model_metrics SET model_r2 = 0.0 WHERE model_r2 IS NULL;

-- ২. ভবিষ্যতে কোনো নতুন রেকর্ড ইনসার্ট হলে তা যেন নিজে থেকেই NULL না হয়ে এই ভ্যালুগুলো পায়
ALTER TABLE public.model_metrics ALTER COLUMN model_specific_params SET DEFAULT '{}'::jsonb;
ALTER TABLE public.model_metrics ALTER COLUMN probabilistic_metrics SET DEFAULT '{}'::jsonb;
ALTER TABLE public.model_metrics ALTER COLUMN multi_horizon_metrics SET DEFAULT '{}'::jsonb;
ALTER TABLE public.model_metrics ALTER COLUMN model_r2 SET DEFAULT 0.0;

-- ৩. অন্যান্য মেট্রিক্স এবং লিগ্যাসি কলামগুলোর NULL ভ্যালু রিমুভ করা
UPDATE public.model_metrics SET cv_mean_rmse = 0.0 WHERE cv_mean_rmse IS NULL;
UPDATE public.model_metrics SET cv_std_rmse = 0.0 WHERE cv_std_rmse IS NULL;
UPDATE public.model_metrics SET cv_mean_mape = 0.0 WHERE cv_mean_mape IS NULL;
UPDATE public.model_metrics SET cv_std_mape = 0.0 WHERE cv_std_mape IS NULL;
UPDATE public.model_metrics SET cv_mean_smape = 0.0 WHERE cv_mean_smape IS NULL;
UPDATE public.model_metrics SET cv_std_smape = 0.0 WHERE cv_std_smape IS NULL;

UPDATE public.model_metrics SET cv_ci95_lower = 0.0 WHERE cv_ci95_lower IS NULL;
UPDATE public.model_metrics SET cv_ci95_upper = 0.0 WHERE cv_ci95_upper IS NULL;
UPDATE public.model_metrics SET cv_n_folds = 0 WHERE cv_n_folds IS NULL;

UPDATE public.model_metrics SET n_estimators = -1 WHERE n_estimators IS NULL;
UPDATE public.model_metrics SET max_depth = -1 WHERE max_depth IS NULL;
UPDATE public.model_metrics SET learning_rate = -1.0 WHERE learning_rate IS NULL;
UPDATE public.model_metrics SET subsample = -1.0 WHERE subsample IS NULL;
UPDATE public.model_metrics SET colsample_bytree = -1.0 WHERE colsample_bytree IS NULL;

-- ৪. ভবিষ্যতের জন্য ডেফল্ট ভ্যালু সেট করা
ALTER TABLE public.model_metrics ALTER COLUMN cv_mean_rmse SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_std_rmse SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_mean_mape SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_std_mape SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_mean_smape SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_std_smape SET DEFAULT 0.0;

ALTER TABLE public.model_metrics ALTER COLUMN cv_ci95_lower SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_ci95_upper SET DEFAULT 0.0;
ALTER TABLE public.model_metrics ALTER COLUMN cv_n_folds SET DEFAULT 0;

ALTER TABLE public.model_metrics ALTER COLUMN n_estimators SET DEFAULT -1;
ALTER TABLE public.model_metrics ALTER COLUMN max_depth SET DEFAULT -1;
ALTER TABLE public.model_metrics ALTER COLUMN learning_rate SET DEFAULT -1.0;
ALTER TABLE public.model_metrics ALTER COLUMN subsample SET DEFAULT -1.0;
ALTER TABLE public.model_metrics ALTER COLUMN colsample_bytree SET DEFAULT -1.0;
