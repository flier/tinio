-- Detach every other session from a scratch database before dropping it.
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{name}'
