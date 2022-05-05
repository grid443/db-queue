CREATE TABLE message
(
    id         UUID PRIMARY KEY,
    queue_name VARCHAR NOT NULL,
    state      VARCHAR NOT NULL,
    body       JSONB   NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX ix_message_queue_name_state ON message (queue_name, state);
CREATE INDEX ix_message_created_at ON message (created_at);

