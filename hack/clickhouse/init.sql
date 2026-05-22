-- Create a test database with sample tables for backup testing

CREATE DATABASE IF NOT EXISTS testdb;

CREATE TABLE IF NOT EXISTS testdb.events
(
    id UInt64,
    event_type String,
    payload String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (event_type, created_at);

CREATE TABLE IF NOT EXISTS testdb.users
(
    id UInt64,
    name String,
    email String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY id;

-- Insert sample data
INSERT INTO testdb.events (id, event_type, payload) VALUES
    (1, 'click', '{"page": "/home"}'),
    (2, 'click', '{"page": "/about"}'),
    (3, 'view', '{"page": "/home"}'),
    (4, 'signup', '{"plan": "free"}'),
    (5, 'click', '{"page": "/pricing"}');

INSERT INTO testdb.users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com');
