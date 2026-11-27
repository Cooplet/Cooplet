-- Initial Hub schema. Connection pragmas and migration transactions belong to the storage engine.
CREATE TABLE hub_meta (
    id                        INTEGER PRIMARY KEY CHECK (id = 1),
    hub_id                    TEXT    NOT NULL,
    display_name              TEXT    NOT NULL,
    master_key_fingerprint    TEXT    NOT NULL,
    events_pruned_through_seq INTEGER NOT NULL DEFAULT 0,
    events_head_seq           INTEGER NOT NULL DEFAULT 0,
    created_at_ms             INTEGER NOT NULL,
    updated_at_ms             INTEGER NOT NULL
);

CREATE TABLE principals (
    principal_id        TEXT    PRIMARY KEY,
    kind                TEXT    NOT NULL CHECK (kind IN ('human_admin','human_member','service')),
    username            TEXT    NOT NULL UNIQUE,
    display_name        TEXT    NOT NULL,
    participant_id      TEXT    UNIQUE,
    scopes_json         TEXT    NOT NULL DEFAULT '[]',
    allowed_nodes_json  TEXT,
    disabled_at_ms      INTEGER,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL
);

CREATE TABLE auth_tokens (
    token_id            TEXT    PRIMARY KEY,
    principal_id        TEXT    NOT NULL REFERENCES principals(principal_id) ON DELETE CASCADE,
    token_kind          TEXT    NOT NULL CHECK (token_kind IN ('bearer_api','enrollment_token','service_token')),
    is_admin            INTEGER NOT NULL DEFAULT 0 CHECK (is_admin IN (0, 1)),
    verifier_hmac       TEXT    NOT NULL UNIQUE,
    label               TEXT    NOT NULL DEFAULT '',
    scopes_json         TEXT    NOT NULL DEFAULT '[]',
    allowed_nodes_json  TEXT,
    consumed_at_ms      INTEGER,
    expires_at_ms       INTEGER NOT NULL,
    revoked_at_ms       INTEGER,
    created_at_ms       INTEGER NOT NULL,
    CHECK (is_admin = 0 OR token_kind = 'bearer_api')
);
CREATE INDEX idx_auth_tokens_principal ON auth_tokens(principal_id);
CREATE INDEX idx_auth_tokens_principal_page ON auth_tokens(principal_id, token_id)
    WHERE token_kind != 'enrollment_token';
CREATE INDEX idx_auth_tokens_enrollment_page ON auth_tokens(token_id)
    WHERE token_kind = 'enrollment_token';
CREATE INDEX idx_auth_tokens_gc ON auth_tokens(expires_at_ms);
CREATE INDEX idx_auth_tokens_revoked ON auth_tokens(revoked_at_ms) WHERE revoked_at_ms IS NOT NULL;
CREATE INDEX idx_auth_tokens_consumed ON auth_tokens(consumed_at_ms) WHERE consumed_at_ms IS NOT NULL;

CREATE TABLE nodes (
    node_id             TEXT    PRIMARY KEY,
    display_name        TEXT    NOT NULL,
    owner_principal_id  TEXT    NOT NULL REFERENCES principals(principal_id) ON DELETE RESTRICT,
    credential_hmac     TEXT    NOT NULL UNIQUE,
    generation          INTEGER NOT NULL DEFAULT 1,
    session_epoch       INTEGER NOT NULL DEFAULT 0,
    status              TEXT    NOT NULL CHECK (status IN ('online','offline','revoked')),
    capabilities_json   TEXT    NOT NULL DEFAULT '{}',
    roster_catalog_json TEXT,
    last_seen_at_ms     INTEGER,
    enrolled_at_ms      INTEGER NOT NULL,
    revoked_at_ms       INTEGER
);

CREATE TABLE node_participants (
    node_id             TEXT    NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    participant_id      TEXT    NOT NULL,
    kind                TEXT    NOT NULL CHECK (kind IN ('agent','human','system')),
    name                TEXT    NOT NULL,
    client_kind         TEXT,
    workspace_dir       TEXT,
    state               TEXT    NOT NULL CHECK (state IN (
                            'starting','idle','working','awaiting_approval',
                            'stopping','stopped','unavailable','unknown'
                        )),
    capabilities_json   TEXT    NOT NULL DEFAULT '{}',
    approval_json       TEXT,
    synced_epoch        INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    PRIMARY KEY (node_id, participant_id)
);
CREATE INDEX idx_node_participants_part ON node_participants(participant_id);

CREATE TABLE operations (
    operation_id        TEXT    PRIMARY KEY,
    actor_id            TEXT    NOT NULL,
    operation_key       TEXT    NOT NULL,
    kind                TEXT    NOT NULL,
    request_sha256      TEXT    NOT NULL,
    target_node_id      TEXT    REFERENCES nodes(node_id) ON DELETE SET NULL,
    state               TEXT    NOT NULL CHECK (state IN ('accepted','in_progress','succeeded','failed','unknown')),
    resource_id         TEXT,
    result_json         TEXT,
    error_json          TEXT,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    expires_at_ms       INTEGER NOT NULL,
    UNIQUE (actor_id, operation_key)
);
CREATE INDEX idx_hub_ops_recovery ON operations(state) WHERE state IN ('accepted','in_progress');
CREATE INDEX idx_hub_ops_node_unknown ON operations(target_node_id, updated_at_ms, operation_id)
    WHERE target_node_id IS NOT NULL AND state IN ('accepted','in_progress','unknown');
CREATE INDEX idx_hub_ops_expires ON operations(expires_at_ms);

CREATE TABLE hub_messages (
    message_id            TEXT    PRIMARY KEY,
    sender_actor_id       TEXT    NOT NULL,
    sender_node_id        TEXT    REFERENCES nodes(node_id) ON DELETE SET NULL,
    target_node_id        TEXT    REFERENCES nodes(node_id) ON DELETE SET NULL,
    target_participant_id TEXT    NOT NULL,
    in_reply_to           TEXT    REFERENCES hub_messages(message_id) ON DELETE SET NULL,
    body                  TEXT    NOT NULL,
    attachments_json      TEXT    NOT NULL DEFAULT '[]',
    question_json         TEXT,
    answer_json           TEXT,
    delivery_mode         TEXT    NOT NULL CHECK (delivery_mode IN ('direct_now','queue_until_idle')),
    delivery_state        TEXT    NOT NULL CHECK (delivery_state IN (
                              'accepted','queued','dispatched','delivered','busy',
                              'awaiting_approval','expired','unavailable','unknown'
                          )),
    delivery_detail       TEXT,
    operation_id          TEXT    REFERENCES operations(operation_id) ON DELETE SET NULL,
    created_at_ms         INTEGER NOT NULL,
    delivered_at_ms       INTEGER,
    expires_at_ms         INTEGER NOT NULL,
    CHECK (NOT (question_json IS NOT NULL AND answer_json IS NOT NULL))
);
CREATE UNIQUE INDEX idx_hub_messages_single_answer
    ON hub_messages(in_reply_to)
    WHERE answer_json IS NOT NULL AND in_reply_to IS NOT NULL;
CREATE INDEX idx_hub_messages_target ON hub_messages(target_node_id, target_participant_id, created_at_ms DESC);
CREATE INDEX idx_hub_messages_expires ON hub_messages(expires_at_ms);
CREATE INDEX idx_hub_messages_operation_fk ON hub_messages(operation_id);
CREATE INDEX idx_hub_messages_reply_fk ON hub_messages(in_reply_to);
CREATE INDEX idx_hub_messages_recovery ON hub_messages(delivery_state) WHERE delivery_state IN ('accepted','dispatched');

CREATE TABLE threads (
    thread_id           TEXT    PRIMARY KEY,
    title               TEXT    NOT NULL,
    topic               TEXT,
    created_by_actor_id TEXT    NOT NULL,
    status              TEXT    NOT NULL CHECK (status IN ('open','closed')),
    head_seq            INTEGER NOT NULL DEFAULT 0,
    floor_seq           INTEGER NOT NULL DEFAULT 0,
    pin_version_clock   INTEGER NOT NULL DEFAULT 0 CHECK (pin_version_clock >= 0),
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    closed_at_ms        INTEGER,
    expires_at_ms       INTEGER
);
CREATE INDEX idx_threads_status ON threads(status, updated_at_ms DESC);
CREATE INDEX idx_threads_expires ON threads(expires_at_ms) WHERE expires_at_ms IS NOT NULL;

CREATE TABLE thread_members (
    thread_id           TEXT    NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
    member_key          TEXT    NOT NULL,
    participant_id      TEXT    NOT NULL,
    node_id             TEXT    REFERENCES nodes(node_id) ON DELETE SET NULL,
    handle              TEXT    NOT NULL,
    role                TEXT    NOT NULL CHECK (role IN ('owner','member','observer')),
    wake_policy         TEXT    NOT NULL CHECK (wake_policy IN ('mentions_only','all','muted')),
    delivery_mode       TEXT    NOT NULL DEFAULT 'queue_until_idle'
                        CHECK (delivery_mode IN ('direct_now','queue_until_idle')),
    state               TEXT    NOT NULL DEFAULT 'active'
                        CHECK (state IN ('active','paused_unknown','left')),
    delivery_generation INTEGER NOT NULL DEFAULT 0 CHECK (delivery_generation >= 0),
    confirmed_seq       INTEGER NOT NULL DEFAULT 0,
    -- The head at explicit resume, extended by relevant direct wakes deferred into recovery.
    resume_through_seq  INTEGER,
    pending_delivery_id TEXT,
    pending_from_seq    INTEGER,
    pending_to_seq      INTEGER,
    joined_at_ms        INTEGER NOT NULL,
    left_at_ms          INTEGER,
    CHECK ((pending_delivery_id IS NULL AND pending_from_seq IS NULL AND pending_to_seq IS NULL)
        OR (pending_delivery_id IS NOT NULL AND pending_from_seq IS NOT NULL AND pending_to_seq IS NOT NULL
            AND pending_from_seq >= 1 AND pending_to_seq >= pending_from_seq)),
    CHECK (resume_through_seq IS NULL OR (resume_through_seq > confirmed_seq AND delivery_mode = 'direct_now' AND state = 'active')),
    PRIMARY KEY (thread_id, member_key),
    UNIQUE (thread_id, handle)
);
CREATE INDEX idx_thread_members_pending ON thread_members(node_id, participant_id, thread_id)
    WHERE state = 'active' AND node_id IS NOT NULL;
CREATE INDEX idx_thread_members_member_key ON thread_members(member_key, thread_id);
CREATE INDEX idx_thread_members_pending_blobs ON thread_members(node_id, thread_id)
    WHERE state = 'active' AND role != 'observer' AND wake_policy != 'muted' AND pending_delivery_id IS NOT NULL;

CREATE TABLE thread_posts (
    thread_id           TEXT    NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
    seq                 INTEGER NOT NULL,
    post_id             TEXT    NOT NULL UNIQUE,
    author_actor_id     TEXT    NOT NULL,
    author_kind         TEXT    NOT NULL CHECK (author_kind IN ('human','agent','service')),
    author_handle       TEXT    NOT NULL,
    in_reply_to_post_id TEXT,
    body                TEXT    NOT NULL,
    mentions_json       TEXT    NOT NULL DEFAULT '[]',
    attachments_json    TEXT    NOT NULL DEFAULT '[]',
    question_json       TEXT,
    answer_json         TEXT,
    operation_id        TEXT    REFERENCES operations(operation_id) ON DELETE SET NULL,
    created_at_ms       INTEGER NOT NULL,
    expires_at_ms       INTEGER NOT NULL,
    PRIMARY KEY (thread_id, seq),
    CHECK (NOT (question_json IS NOT NULL AND answer_json IS NOT NULL))
);
CREATE UNIQUE INDEX idx_thread_posts_single_answer
    ON thread_posts(in_reply_to_post_id)
    WHERE answer_json IS NOT NULL AND in_reply_to_post_id IS NOT NULL;
CREATE INDEX idx_thread_posts_expires ON thread_posts(expires_at_ms);
CREATE INDEX idx_thread_posts_expiry_seq ON thread_posts(thread_id, expires_at_ms, seq);
CREATE INDEX idx_thread_posts_operation_fk ON thread_posts(operation_id);

CREATE TABLE thread_pins (
    thread_id           TEXT    NOT NULL REFERENCES threads(thread_id) ON DELETE CASCADE,
    pin_key             TEXT    NOT NULL,
    post_id             TEXT,
    value_json          TEXT    NOT NULL,
    version             INTEGER NOT NULL DEFAULT 1,
    updated_by_actor_id TEXT    NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    PRIMARY KEY (thread_id, pin_key)
);

CREATE TABLE blobs (
    blob_id             TEXT    PRIMARY KEY,
    sha256              TEXT    NOT NULL,
    size_bytes          INTEGER NOT NULL CHECK (size_bytes >= 0),
    mime_type           TEXT    NOT NULL,
    original_name       TEXT    NOT NULL,
    safe_disk_name      TEXT    NOT NULL,
    disk_rel_path       TEXT    NOT NULL,
    owner_actor_id      TEXT    NOT NULL,
    scope_node_id       TEXT    REFERENCES nodes(node_id) ON DELETE SET NULL,
    scope_peer_node_id  TEXT,
    scope_thread_id     TEXT    REFERENCES threads(thread_id) ON DELETE SET NULL,
    attached_at_ms      INTEGER,
    created_at_ms       INTEGER NOT NULL,
    expires_at_ms       INTEGER NOT NULL,
    deleted_at_ms       INTEGER
);
CREATE INDEX idx_hub_blobs_expires ON blobs(expires_at_ms) WHERE deleted_at_ms IS NULL;
CREATE INDEX idx_hub_blobs_gc_pending ON blobs(deleted_at_ms, blob_id) WHERE deleted_at_ms IS NOT NULL;
CREATE INDEX idx_hub_blobs_thread_fk ON blobs(scope_thread_id);

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
CREATE INDEX idx_hub_events_created ON events(created_at_ms);
CREATE INDEX idx_hub_events_kind_seq ON events(kind, seq);
