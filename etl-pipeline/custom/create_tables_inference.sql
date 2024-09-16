-- Docs: https://docs.mage.ai/guides/sql-blocks
CREATE TABLE IF NOT EXISTS inference_runs (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS inference_results (
    id SERIAL PRIMARY KEY,
    inference_run INTEGER REFERENCES inference_runs(id),
    station INTEGER REFERENCES stations(id),
    forecasts_6h JSONB NOT NULL DEFAULT '{}'::jsonb,
    forecasts_12h JSONB NOT NULL DEFAULT '{}'::jsonb,
    aqi_input JSONB NOT NULL DEFAULT '{}'::jsonb

);
