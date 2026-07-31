-- Non-destructive indexes for maintenance near-term tick and status queries.
-- Apply manually in Supabase SQL editor; not executed automatically by the app.

CREATE INDEX IF NOT EXISTS current_sessions_park_iso_date_idx
  ON current_sessions (park, iso_date);

CREATE INDEX IF NOT EXISTS current_sessions_park_iso_date_start_ts_idx
  ON current_sessions (park, iso_date, start_ts);

CREATE INDEX IF NOT EXISTS threshold_scan_jobs_mode_status_created_idx
  ON threshold_scan_jobs (mode, status, created_at DESC);

CREATE INDEX IF NOT EXISTS threshold_scan_jobs_mode_status_completed_idx
  ON threshold_scan_jobs (mode, status, completed_at DESC);

CREATE INDEX IF NOT EXISTS threshold_scan_jobs_active_status_idx
  ON threshold_scan_jobs (status, created_at DESC)
  WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS threshold_scan_jobs_apply_source_job_id_idx
  ON threshold_scan_jobs ((results_json->>'sourceJobId'))
  WHERE mode = 'threshold_week_apply_prepared';
