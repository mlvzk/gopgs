CREATE TABLE pgkv.store (
    key text NOT NULL,
    value bytea NOT NULL,
    compressed boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (key)
);

CREATE FUNCTION pgkv.store_updated_at_trigger() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER store_updated_at_trigger
    BEFORE UPDATE ON pgkv.store
    FOR EACH ROW
    EXECUTE PROCEDURE pgkv.store_updated_at_trigger();

CREATE FUNCTION pgkv.store_notify_trigger() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('pgkv_store_update', NEW.key);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER store_notify_trigger
    AFTER INSERT OR UPDATE ON pgkv.store
    FOR EACH ROW
    EXECUTE PROCEDURE pgkv.store_notify_trigger();
