DELETE
FROM pgqueue.dictionaries
WHERE NOT EXISTS (
    SELECT 1 FROM pgqueue.jobs WHERE dict_id = pgqueue.dictionaries.id
);
