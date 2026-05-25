-- =========================================================================
-- Q1 DEFENSIBLE SQL SCHEMA: shap_feature_importance
-- =========================================================================
-- Purpose:
-- Stores the Global Feature Importance (SHAP values) for the trained XGBoost model.
-- This ensures transparency and explainability required by Q1 Transportation journals.
-- =========================================================================

-- 1. Create shap_feature_importance table
CREATE TABLE IF NOT EXISTS public.shap_feature_importance (
    id bigserial PRIMARY KEY,
    timestamp timestamptz NOT NULL DEFAULT now(),
    feature_name text NOT NULL,
    mean_abs_shap numeric NOT NULL,
    model_type text NOT NULL,
    created_at timestamptz DEFAULT now()
);

-- 2. Enable RLS and set policies
ALTER TABLE public.shap_feature_importance ENABLE ROW LEVEL SECURITY;

-- Allow read access for everyone
CREATE POLICY "Allow read access for all" ON public.shap_feature_importance 
FOR SELECT USING (true);

-- Allow insert/update for anonymous users (for testing/local development)
CREATE POLICY "Allow all operations for anon" ON public.shap_feature_importance 
FOR ALL USING (true) WITH CHECK (true);

-- Ensure model_metrics also has RLS policy enabled for insertions if needed
CREATE POLICY "Allow all operations for anon" ON public.model_metrics 
FOR ALL USING (true) WITH CHECK (true);
