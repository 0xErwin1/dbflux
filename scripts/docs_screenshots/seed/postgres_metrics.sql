-- Synthetic time-series data for the documentation screenshots, seeded into
-- the metrics database. The server must preload timescaledb. Every value is
-- derived from the row number, so two runs produce the same rows.

CREATE EXTENSION timescaledb;

CREATE TABLE server_metrics (
    time            timestamptz NOT NULL,
    host            text NOT NULL,
    cpu_percent     double precision NOT NULL,
    memory_percent  double precision NOT NULL
);

SELECT create_hypertable('server_metrics', 'time');

INSERT INTO server_metrics (time, host, cpu_percent, memory_percent)
SELECT
    timestamptz '2025-06-02 08:00:00+00' + step * interval '5 minutes',
    host,
    round((35 + host_index * 12 + 20 * sin(step / 9.0 + host_index)
        + 6 * sin(step / 2.5))::numeric, 1)::double precision,
    round((48 + host_index * 9 + 8 * sin(step / 20.0 + host_index * 2))::numeric, 1)::double precision
FROM generate_series(0, 71) AS step,
    unnest(ARRAY['api-1', 'api-2', 'worker-1']) WITH ORDINALITY AS hosts (host, host_index)
-- Interleaved hosts, so the first page of the table holds every series.
ORDER BY step, host;
