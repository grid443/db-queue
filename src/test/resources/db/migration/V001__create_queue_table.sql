CREATE TABLE queue
(
    id      UUID PRIMARY KEY,
    name    VARCHAR NOT NULL,
    message JSONB   NOT NULL
);

INSERT INTO queue (id, name, message) VALUES ('466b4029-6eff-4698-b854-01bf9cdfd091', 'test', '{"name": "value"}'::JSONB);
