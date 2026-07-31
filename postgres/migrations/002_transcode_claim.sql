-- KEDA creates one Job per unit of work but never hands it the message; the
-- pod claims its own work by winning an atomic UPDATE here, and a heartbeat
-- on the claimed row is the lease the sweeper CronJob reads to recover work
-- abandoned by a pod that died after committing its Kafka offset but before
-- finishing. See docs/aws/14-keda-scaledjobs.md.

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS transcode_status TEXT NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS transcode_heartbeat TIMESTAMPTZ;

ALTER TABLE videos
  ADD COLUMN IF NOT EXISTS transcode_heartbeat TIMESTAMPTZ;

-- ingest-webhook runs COUNT(*) WHERE status='live' on every publish attempt
-- (the MAX_CONCURRENT_LIVE_STREAMS admission check). Without this it's a
-- sequential scan over a table that grows one row per broadcast forever.
-- With it, a sub-millisecond index-only scan.
CREATE INDEX IF NOT EXISTS streams_live_idx ON streams (status) WHERE status = 'live';
