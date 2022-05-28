CREATE FUNCTION pg_temp.get_or_lock(store_key text, after timestamptz) RETURNS TABLE (kv pgkv.store, found boolean, locked boolean) AS $$
BEGIN
    RETURN QUERY SELECT s, true AS found, false as locked FROM pgkv.store s WHERE key = store_key AND updated_at > after;
    IF FOUND THEN
        RETURN;
    END IF;

    IF NOT pg_try_advisory_xact_lock(hashtext('pgkv.get_or_lock ' || store_key)) THEN
        RETURN QUERY SELECT null::pgkv.store, false AS found, false as locked;
        RETURN;
    END IF;

    RETURN QUERY SELECT s, true AS found, false as locked FROM pgkv.store s WHERE key = store_key AND updated_at > after;
    IF FOUND THEN
        RETURN;
    END IF;

    RETURN QUERY SELECT null::pgkv.store, false AS found, pg_try_advisory_lock(hashtext('pgkv.get_or_lock ' || store_key)) AS locked;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_temp.unlock_get_or_lock(key text) RETURNS void AS $$
BEGIN
    IF NOT pg_advisory_unlock(hashtext('pgkv.get_or_lock ' || key)) THEN
        RAISE EXCEPTION 'Failed to unlock key %, you probably did not own a lock for it', key;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_temp.set_and_unlock(store_key text, value bytea, compressed boolean) RETURNS void AS $$
BEGIN
    -- Downgrade the session-level lock to a transaction-level lock.
    -- If instead we just had an unlock after the INSERT,
    -- the lock would be released before the INSERT is commited,
    -- so a connection that just got the lock and did a SELECT
    -- would not see the new row.
    IF NOT pg_try_advisory_xact_lock(hashtext('pgkv.get_or_lock ' || store_key)) THEN
        RAISE EXCEPTION 'Failed to lock key %, you probably did not own a lock for it', store_key;
    END IF;
    IF NOT pg_advisory_unlock(hashtext('pgkv.get_or_lock ' || store_key)) THEN
        RAISE EXCEPTION 'Failed to unlock (session-level) key %', store_key;
    END IF;

    INSERT INTO pgkv.store (key, value, compressed) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = $2, compressed = $3;
END;
$$ LANGUAGE plpgsql;
