-- Ark initial schema: 9 core tables (clean, normalized, low-memory SQLite)

-- 1. System settings & encrypted credentials (115 cookie, telegram bot token, passwords, etc.)
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    encrypted INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);

-- 2. API tokens (hashed with SHA-256, supports admin & readonly roles)
CREATE TABLE IF NOT EXISTS api_tokens (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL CHECK (role IN ('admin', 'readonly')),
    token_hash TEXT NOT NULL UNIQUE,
    prefix TEXT NOT NULL,
    expires_at INTEGER,
    last_used_at INTEGER,
    created_at INTEGER NOT NULL
);

-- 3. Monitored sources (115 cloud directories, local/NAS watch directories, Bark path prefix)
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('cloud', 'local')),
    enabled INTEGER NOT NULL DEFAULT 1,
    config_json TEXT NOT NULL DEFAULT '{}',
    bark_prefix TEXT NOT NULL DEFAULT '',
    cursor TEXT NOT NULL DEFAULT '',
    next_run_at INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sources_kind_enabled ON sources(kind, enabled);

-- 4. Processing & cleanup rules (global, per-source, or seed-download cleanup)
CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('global', 'source', 'seed')),
    source_id TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rules_scope_source ON rules(scope, source_id);

-- 5. Unified end-to-end pipelines (offline -> discover -> cloud extract -> local fallback -> cleanup)
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('offline', 'cloud_archive', 'local_archive')),
    identity TEXT NOT NULL UNIQUE,
    parent_id TEXT NOT NULL DEFAULT '',
    batch_id TEXT NOT NULL DEFAULT '',
    source_id TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    original_link TEXT NOT NULL DEFAULT '',
    info_hash TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    stage TEXT NOT NULL,
    wait_reason TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    target_date TEXT NOT NULL DEFAULT '',
    target_cid TEXT NOT NULL DEFAULT '',
    auto_extract INTEGER NOT NULL DEFAULT 1,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_run_at INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    locator_json TEXT NOT NULL DEFAULT '{}',
    policy_json TEXT NOT NULL DEFAULT '{}',
    output_json TEXT NOT NULL DEFAULT '{}',
    progress_json TEXT NOT NULL DEFAULT '{}',
    remote_id TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pipelines_state_next ON pipelines(state, next_run_at);
CREATE INDEX IF NOT EXISTS idx_pipelines_batch ON pipelines(batch_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_hash ON pipelines(info_hash);

-- 6. Import batches & idempotency tracking (plus batch_items join table)
CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    token_id TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    request_hash TEXT NOT NULL DEFAULT '',
    target_date TEXT NOT NULL DEFAULT '',
    auto_extract INTEGER NOT NULL DEFAULT 1,
    queued_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    invalid_count INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_batches_token_idem
    ON batches(token_id, idempotency_key)
    WHERE idempotency_key <> '';

CREATE TABLE IF NOT EXISTS batch_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    duplicate INTEGER NOT NULL DEFAULT 0,
    UNIQUE(batch_id, pipeline_id)
);

-- 7. Pipeline timeline events & integrated notification outbox
CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    stage TEXT NOT NULL,
    message TEXT NOT NULL,
    notify_status TEXT NOT NULL DEFAULT 'none' CHECK (notify_status IN ('none', 'pending', 'sent', 'failed')),
    notify_attempts INTEGER NOT NULL DEFAULT 0,
    notify_next_at INTEGER NOT NULL DEFAULT 0,
    notify_error TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_pipeline ON task_events(pipeline_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_notify ON task_events(notify_status, notify_next_at);

-- 8. Remote write operation journal (prevents duplicate submissions or unsafe deletions)
CREATE TABLE IF NOT EXISTS operation_journal (
    op_key TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('prepared', 'completed', 'uncertain', 'failed')),
    evidence_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_op_journal_pipeline ON operation_journal(pipeline_id);

-- 9. Sanitized Bark trigger messages & 90s coalescing window records (keep latest 50)
CREATE TABLE IF NOT EXISTS bark_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    group_name TEXT NOT NULL DEFAULT '',
    matched_source_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'received',
    window_id TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bark_created ON bark_messages(created_at DESC);
