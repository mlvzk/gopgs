DROP TRIGGER dictionaries_update_ref_count_insert ON pgqueue.jobs;
DROP TRIGGER dictionaries_update_ref_count_delete ON pgqueue.jobs;
DROP FUNCTION pgqueue.update_dictionaries_ref_count;
ALTER TABLE pgqueue.dictionaries DROP COLUMN ref_count;
