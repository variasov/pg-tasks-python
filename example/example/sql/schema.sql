CREATE EXTENSION pg_tasks;

CREATE TABLE some_task(
    id bigserial PRIMARY KEY,
    some_field text
);

SELECT register_task('some_task');
