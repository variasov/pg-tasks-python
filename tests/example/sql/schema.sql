CREATE EXTENSION pg_tasks SCHEMA public;

CREATE TABLE some_task(
    LIKE tasks
        INCLUDING DEFAULTS
        INCLUDING IDENTITY
        INCLUDING INDEXES,
    payload text
);
SELECT register_task(
    schema_name := 'public',
    table_name := 'some_task',
    timeout := '00:01:00',
    max_retries := 3
);

-- worker_id '3c69e1ea-efc5-4b0c-9b30-8aeceea7e0c3'
-- SELECT * FROM acquire_some_task('3c69e1ea-efc5-4b0c-9b30-8aeceea7e0c3', 1);

-- SELECT *
-- FROM next_batch_of_some_task(
--     '3c69e1ea-efc5-4b0c-9b30-8aeceea7e0c3',
--     1, '{1}', '{}', '{}'
-- );

INSERT INTO some_task(payload)
SELECT NULL
FROM generate_series(1, 100000) i;

TRUNCATE some_task;
SELECT unregister_task('some_task');

SELECT count(*) FROM some_task
                WHERE finished_at IS NOT NULL;
