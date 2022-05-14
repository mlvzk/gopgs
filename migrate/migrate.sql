CREATE SCHEMA IF NOT EXISTS {{SchemaName}};

CREATE TABLE IF NOT EXISTS {{SchemaName}}.done_migrations (
    version integer not null,
    statements text not null,
    PRIMARY KEY (version)
);

CREATE FUNCTION pg_temp.migrate(migration_statements {{SchemaName}}.done_migrations[]) RETURNS SETOF {{SchemaName}}.done_migrations AS $$
DECLARE
    starting_version integer;
    target_version integer;
    migration {{SchemaName}}.done_migrations;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('{{SchemaName}} migration'));

    SELECT max(version) INTO starting_version FROM {{SchemaName}}.done_migrations;
    IF starting_version IS NULL THEN
        starting_version = 0;
    END IF;

    SELECT count(*) INTO target_version FROM unnest(migration_statements);
    IF starting_version >= target_version THEN
        RETURN;
    END IF;

    FOR migration IN SELECT * FROM unnest(migration_statements) WHERE version > starting_version ORDER BY version ASC
    LOOP
        EXECUTE migration.statements;
        INSERT INTO {{SchemaName}}.done_migrations (version, statements) VALUES (migration.version, migration.statements);
        RETURN NEXT migration;
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Migration % failed: %.', migration.version, SQLERRM;
END;
$$ LANGUAGE plpgsql;
