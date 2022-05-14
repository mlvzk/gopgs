CREATE TABLE pgqueue.dictionaries
(
    id bigserial NOT NULL,
    ref_count integer NOT NULL,
    data bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (id)
);

CREATE TABLE pgqueue.jobs
(
    id          bigserial   NOT NULL,
    queue       text        NOT NULL,
    priority    smallint    NOT NULL,
    enqueued_at timestamptz NOT NULL,
    run_at      timestamptz,
    expires_at  timestamptz,
    failed_at   timestamptz,
    args        bytea       NOT NULL,
    dict_id     bigint      REFERENCES pgqueue.dictionaries(id),
    error_count integer     NOT NULL,
    last_error  text        NOT NULL,
    retryable   boolean     NOT NULL,

    PRIMARY KEY (id)
);

CREATE FUNCTION pgqueue.update_dictionaries_ref_count() RETURNS trigger AS $$
    BEGIN
    IF (TG_OP = 'DELETE') THEN
        UPDATE pgqueue.dictionaries SET ref_count = ref_count - 1 WHERE id = OLD.dict_id;
        DELETE FROM pgqueue.dictionaries WHERE id = OLD.dict_id AND ref_count = 0;
    ELSIF (TG_OP = 'INSERT') THEN
        UPDATE pgqueue.dictionaries SET ref_count = ref_count + 1 WHERE id = NEW.dict_id;
    END IF;
    RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dictionaries_update_ref_count_insert
    AFTER INSERT ON pgqueue.jobs
    FOR EACH ROW
    EXECUTE FUNCTION pgqueue.update_dictionaries_ref_count();

CREATE TRIGGER dictionaries_update_ref_count_delete
    AFTER DELETE ON pgqueue.jobs
    FOR EACH ROW
    EXECUTE FUNCTION pgqueue.update_dictionaries_ref_count();
