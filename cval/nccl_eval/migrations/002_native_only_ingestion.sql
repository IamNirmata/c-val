-- Retire copied-SQLite migration while preserving any historical PostgreSQL rows.
ALTER TABLE nccl_raw.test_run
    ADD CONSTRAINT test_run_native_only_new_rows
    CHECK (NOT legacy_source) NOT VALID;