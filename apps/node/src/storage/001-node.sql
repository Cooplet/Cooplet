-- Node identity and EventBus watermarks
CREATE TABLE node_meta (
    id                        INTEGER PRIMARY KEY CHECK (id = 1),
    node_id                   TEXT    NOT NULL,
    display_name              TEXT    NOT NULL,
    hub_url                   TEXT,
    hub_fingerprint           TEXT,
    hub_enrolled_at_ms        INTEGER,
    events_pruned_through_seq INTEGER NOT NULL DEFAULT 0,
    events_head_seq           INTEGER NOT NULL DEFAULT 0,
    created_at_ms             INTEGER NOT NULL,
    updated_at_ms             INTEGER NOT NULL
);

-- Local participants
CREATE TABLE participants (
    participant_id       TEXT    PRIMARY KEY,
    kind                 TEXT    NOT NULL CHECK (kind IN ('agent', 'human', 'system')),
    name                 TEXT    NOT NULL,
    client_kind          TEXT,
    store_namespace      TEXT,
    native_session_id    TEXT,
    workspace_dir        TEXT,
    template_id          TEXT,
    ephemeral_for_run_id TEXT,
    process_ownership    TEXT    NOT NULL DEFAULT 'managed' CHECK (process_ownership IN ('managed', 'attached')),
    state                TEXT    NOT NULL CHECK (state IN (
                             'starting','idle','working','awaiting_approval',
                             'stopping','stopped','unavailable','unknown'
                         )),
    capabilities_json    TEXT    NOT NULL DEFAULT '{}',
    adapter_binding_json TEXT    NOT NULL DEFAULT '{}',
    runtime_pid          INTEGER,
    runtime_pgid         INTEGER,
    runtime_start_token  TEXT,
    generation           INTEGER NOT NULL DEFAULT 1,
    created_at_ms        INTEGER NOT NULL,
    updated_at_ms        INTEGER NOT NULL,
    stopped_at_ms        INTEGER
);
CREATE UNIQUE INDEX idx_participants_active_name
    ON participants(name) WHERE state NOT IN ('stopped');
CREATE UNIQUE INDEX idx_participants_native_identity
    ON participants(client_kind, store_namespace, native_session_id)
    WHERE native_session_id IS NOT NULL;
CREATE INDEX idx_participants_ephemeral
    ON participants(ephemeral_for_run_id) WHERE ephemeral_for_run_id IS NOT NULL;
CREATE INDEX idx_participants_stopped
    ON participants(stopped_at_ms) WHERE stopped_at_ms IS NOT NULL;

-- Local access credentials
CREATE TABLE participant_credentials (
    token_id            TEXT    PRIMARY KEY,
    participant_id      TEXT    REFERENCES participants(participant_id) ON DELETE CASCADE,
    principal_kind      TEXT    NOT NULL CHECK (principal_kind IN ('participant','operator','adapter','workflow_run')),
    actor_id            TEXT    NOT NULL,
    scopes_json         TEXT    NOT NULL,
    verifier_hmac       TEXT    NOT NULL UNIQUE,
    workflow_run_id     TEXT,
    expires_at_ms       INTEGER,
    revoked_at_ms       INTEGER,
    created_at_ms       INTEGER NOT NULL
);
CREATE INDEX idx_part_creds_participant ON participant_credentials(participant_id);
CREATE INDEX idx_part_creds_workflow_run
    ON participant_credentials(workflow_run_id) WHERE workflow_run_id IS NOT NULL;
CREATE INDEX idx_part_creds_expires
    ON participant_credentials(expires_at_ms) WHERE expires_at_ms IS NOT NULL;
CREATE INDEX idx_part_creds_revoked
    ON participant_credentials(revoked_at_ms) WHERE revoked_at_ms IS NOT NULL;

-- Idempotency operations and receipts
CREATE TABLE operations (
    operation_id        TEXT    PRIMARY KEY,
    actor_id            TEXT    NOT NULL,
    operation_key       TEXT    NOT NULL,
    kind                TEXT    NOT NULL,
    request_sha256      TEXT    NOT NULL,
    state               TEXT    NOT NULL CHECK (state IN ('accepted','in_progress','succeeded','failed','unknown')),
    resource_id         TEXT,
    result_json         TEXT,
    error_json          TEXT,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    expires_at_ms       INTEGER NOT NULL,
    UNIQUE (actor_id, operation_key)
);
CREATE INDEX idx_operations_recovery ON operations(state) WHERE state IN ('accepted', 'in_progress');
CREATE INDEX idx_operations_expires ON operations(expires_at_ms);
CREATE INDEX idx_operations_unknown_approval
    ON operations(request_sha256)
    WHERE kind = 'approval.respond' AND state = 'unknown';

-- Messages, questions, and answers
CREATE TABLE messages (
    message_id          TEXT    PRIMARY KEY,
    sender_id           TEXT    NOT NULL,
    recipient_id        TEXT    NOT NULL,
    in_reply_to         TEXT    REFERENCES messages(message_id) ON DELETE SET NULL,
    body                TEXT    NOT NULL,
    attachments_json    TEXT    NOT NULL DEFAULT '[]',
    question_json       TEXT,
    answer_json         TEXT,
    delivery_mode       TEXT    NOT NULL CHECK (delivery_mode IN ('direct_now','queue_until_idle')),
    delivery_state      TEXT    NOT NULL CHECK (delivery_state IN (
                            'accepted','queued','delivered','busy',
                            'awaiting_approval','expired','unavailable','unknown'
                        )),
    delivery_detail     TEXT,
    operation_id        TEXT    REFERENCES operations(operation_id) ON DELETE SET NULL,
    hub_message_id      TEXT,
    created_at_ms       INTEGER NOT NULL,
    delivered_at_ms     INTEGER,
    expires_at_ms       INTEGER NOT NULL,
    CHECK (NOT (question_json IS NOT NULL AND answer_json IS NOT NULL))
);
CREATE UNIQUE INDEX idx_messages_single_answer
    ON messages(in_reply_to)
    WHERE answer_json IS NOT NULL AND in_reply_to IS NOT NULL;
CREATE UNIQUE INDEX idx_messages_hub_dedup
    ON messages(hub_message_id) WHERE hub_message_id IS NOT NULL;
CREATE INDEX idx_messages_recipient_time ON messages(recipient_id, created_at_ms DESC);
CREATE INDEX idx_messages_sender_id ON messages(sender_id, message_id);
CREATE INDEX idx_messages_recipient_id ON messages(recipient_id, message_id);
CREATE INDEX idx_messages_recipient_attachments
    ON messages(recipient_id, message_id DESC) WHERE attachments_json != '[]';
CREATE INDEX idx_messages_sender_attachments
    ON messages(sender_id, message_id DESC) WHERE attachments_json != '[]';
CREATE INDEX idx_messages_operation_fk ON messages(operation_id);
CREATE INDEX idx_messages_reply_fk ON messages(in_reply_to);
CREATE INDEX idx_messages_expires ON messages(expires_at_ms);

-- Local intake queue
CREATE TABLE intake_queue (
    queue_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    participant_id      TEXT    NOT NULL REFERENCES participants(participant_id) ON DELETE CASCADE,
    message_id          TEXT    NOT NULL UNIQUE REFERENCES messages(message_id) ON DELETE CASCADE,
    payload_envelope    TEXT    NOT NULL,
    attachments_json    TEXT    NOT NULL DEFAULT '[]',
    state               TEXT    NOT NULL CHECK (state IN (
                            'queued','dispatched','delivered','expired','cancelled','unknown'
                        )),
    enqueued_at_ms      INTEGER NOT NULL,
    deadline_at_ms      INTEGER NOT NULL,
    dispatched_at_ms    INTEGER,
    resolved_at_ms      INTEGER
);
CREATE INDEX idx_intake_queue_participant_fk ON intake_queue(participant_id, state);
CREATE INDEX idx_intake_queue_ready ON intake_queue(participant_id, queue_id) WHERE state = 'queued';
CREATE INDEX idx_intake_queue_dispatched ON intake_queue(state) WHERE state = 'dispatched';
CREATE INDEX idx_intake_queue_deadline ON intake_queue(deadline_at_ms) WHERE state = 'queued';
CREATE INDEX idx_intake_queue_resolved ON intake_queue(resolved_at_ms) WHERE resolved_at_ms IS NOT NULL;

-- Crash-safe fences for Hub thread injection
CREATE TABLE thread_inject_fences (
    thread_id              TEXT    NOT NULL,
    participant_id         TEXT    NOT NULL REFERENCES participants(participant_id) ON DELETE CASCADE,
    highest_dispatched_seq INTEGER NOT NULL,
    state                  TEXT    NOT NULL CHECK (state IN ('injecting','confirmed','unknown')),
    updated_at_ms          INTEGER NOT NULL,
    expires_at_ms          INTEGER NOT NULL,
    PRIMARY KEY (thread_id, participant_id)
);
CREATE INDEX idx_thread_fences_participant_fk ON thread_inject_fences(participant_id);
CREATE INDEX idx_thread_fences_unresolved ON thread_inject_fences(state)
    WHERE state IN ('injecting', 'unknown');
CREATE INDEX idx_thread_fences_expires ON thread_inject_fences(expires_at_ms)
    WHERE state = 'confirmed';

-- Disk-backed blobs
CREATE TABLE blobs (
    blob_id             TEXT    PRIMARY KEY,
    sha256              TEXT    NOT NULL,
    size_bytes          INTEGER NOT NULL CHECK (size_bytes >= 0),
    mime_type           TEXT    NOT NULL,
    original_name       TEXT    NOT NULL,
    safe_disk_name      TEXT    NOT NULL,
    disk_rel_path       TEXT    NOT NULL,
    owner_actor_id      TEXT    NOT NULL,
    source_hub_blob_id  TEXT,
    attached_at_ms      INTEGER,
    created_at_ms       INTEGER NOT NULL,
    expires_at_ms       INTEGER NOT NULL,
    deleted_at_ms       INTEGER
);
CREATE UNIQUE INDEX idx_blobs_hub_cache
    ON blobs(source_hub_blob_id) WHERE source_hub_blob_id IS NOT NULL AND deleted_at_ms IS NULL;
CREATE INDEX idx_blobs_expires ON blobs(expires_at_ms) WHERE deleted_at_ms IS NULL;
CREATE INDEX idx_blobs_gc_pending ON blobs(deleted_at_ms, blob_id) WHERE deleted_at_ms IS NOT NULL;

-- Workflow runs
CREATE TABLE workflow_runs (
    run_id              TEXT    PRIMARY KEY,
    workflow_name       TEXT    NOT NULL,
    entry_path          TEXT    NOT NULL,
    workspace_dir       TEXT    NOT NULL,
    initiator_actor_id  TEXT    NOT NULL,
    params_json         TEXT    NOT NULL DEFAULT '{}',
    state               TEXT    NOT NULL CHECK (state IN (
                            'running','completed','failed','cancelled','interrupted'
                        )),
    result_json         TEXT,
    error_json          TEXT,
    runner_pid          INTEGER,
    runner_pgid         INTEGER,
    runner_start_token  TEXT,
    operation_id        TEXT    REFERENCES operations(operation_id) ON DELETE SET NULL,
    started_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    finished_at_ms      INTEGER,
    expires_at_ms       INTEGER NOT NULL
);
CREATE INDEX idx_workflow_runs_operation_fk ON workflow_runs(operation_id);
CREATE INDEX idx_workflow_runs_recovery
    ON workflow_runs(state) WHERE state = 'running' OR (state = 'interrupted' AND finished_at_ms IS NULL);
CREATE INDEX idx_workflow_runs_expires
    ON workflow_runs(expires_at_ms) WHERE finished_at_ms IS NOT NULL;

-- Workflow step checkpoints
CREATE TABLE workflow_steps (
    run_id              TEXT    NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    step_key            TEXT    NOT NULL,
    status              TEXT    NOT NULL CHECK (status IN ('completed','failed')),
    result_json         TEXT    NOT NULL,
    created_at_ms       INTEGER NOT NULL,
    PRIMARY KEY (run_id, step_key)
);

-- Transactional event outbox
CREATE TABLE events (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            TEXT    NOT NULL UNIQUE,
    kind                TEXT    NOT NULL,
    aggregate_type      TEXT    NOT NULL,
    aggregate_id        TEXT    NOT NULL,
    actor_id            TEXT,
    payload_json        TEXT    NOT NULL,
    created_at_ms       INTEGER NOT NULL
);
CREATE INDEX idx_events_created ON events(created_at_ms);
CREATE INDEX idx_events_kind_seq ON events(kind, seq);
