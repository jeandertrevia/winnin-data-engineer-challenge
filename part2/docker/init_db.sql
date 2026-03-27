CREATE DATABASE creators_db;

\connect creators_db;

CREATE SCHEMA IF NOT EXISTS raw;

-- Seed: lista de creators para monitorar
CREATE TABLE IF NOT EXISTS raw.creators_seed (
    id        SERIAL PRIMARY KEY,
    handle    VARCHAR(100) NOT NULL UNIQUE,
    name      VARCHAR(255),
    active    BOOLEAN DEFAULT TRUE,
    added_at  TIMESTAMP DEFAULT NOW()
);

INSERT INTO raw.creators_seed (handle, name) VALUES
    ('@felipeneto',    'Felipe Neto'),
    ('@KondZilla',     'KondZilla'),
    ('@luccasneto',    'Luccas Neto'),
    ('@portadosfundos','Porta dos Fundos'),
    ('@PewDiePie',     'PewDiePie')
ON CONFLICT (handle) DO NOTHING;

-- Dados brutos dos creators resolvidos via API
CREATE TABLE IF NOT EXISTS raw.creators (
    id          SERIAL PRIMARY KEY,
    handle      VARCHAR(100) NOT NULL UNIQUE,
    channel_id  VARCHAR(100) NOT NULL UNIQUE,
    title       VARCHAR(255),
    loaded_at   TIMESTAMP DEFAULT NOW()
);

-- Dados brutos dos posts/videos
CREATE TABLE IF NOT EXISTS raw.posts (
    id           SERIAL PRIMARY KEY,
    video_id     VARCHAR(100) NOT NULL UNIQUE,
    channel_id   VARCHAR(100) NOT NULL,
    title        TEXT,
    published_at TIMESTAMP,
    views        BIGINT,
    likes        BIGINT,
    tags         TEXT[],
    loaded_at    TIMESTAMP DEFAULT NOW()
);
