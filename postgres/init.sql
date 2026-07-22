-- Minimal Phase 1 schema. Auth is stream-keys-only (see SPEC.md) — no
-- password/login system, so "streamers" is the only account concept.

CREATE TABLE streamers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name TEXT NOT NULL,
    stream_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per live publish/unpublish cycle.
CREATE TABLE streams (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    streamer_id UUID NOT NULL REFERENCES streamers(id),
    -- The MediaMTX path (== the stream key today — see
    -- research_validated_architecture memory for why that's a design smell
    -- worth revisiting). Not exposed via the public API; kept for internal
    -- lookups (e.g. finding the right hls/live/{path}/ prefix in MinIO).
    path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'live' CHECK (status IN ('live', 'ended')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ
);

-- Uploaded VOD files, separate from live streams.
CREATE TABLE videos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    streamer_id UUID NOT NULL REFERENCES streamers(id),
    title TEXT NOT NULL,
    raw_object_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'uploaded' CHECK (status IN ('uploaded', 'transcoding', 'ready', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw view-event log (one row per "watch" click for now — there's no
-- player yet to send finer-grained heartbeat/progress events). Aggregate
-- counts are derived with GROUP BY rather than kept in a running counter
-- column, so the raw log stays the source of truth.
CREATE TABLE view_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_type TEXT NOT NULL CHECK (content_type IN ('stream', 'video')),
    content_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
