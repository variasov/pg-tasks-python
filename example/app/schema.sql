-- Выполняется из миграций
CREATE SCHEMA tasks;

-- Это выполняется единожды на БД:
CREATE EXTENSION pg_tasks SCHEMA tasks;


CREATE TABLE tasks.do_something(
    LIKE tasks.template
        INCLUDING DEFAULTS
        INCLUDING IDENTITY
        INCLUDING INDEXES,
    payload text
);
SELECT tasks.register(
    schema_name := 'tasks',
    table_name := 'do_something',
    timeout := '00:01:00',
    max_retries := 3,
    notification_interval := '10s',
    storage_time := '24:00:00'
);


CREATE TABLE tasks.do_another(
    LIKE tasks.template
        INCLUDING DEFAULTS
        INCLUDING IDENTITY
        INCLUDING INDEXES,
    payload text
);
SELECT tasks.register(
    schema_name := 'tasks',
    table_name := 'do_another',
    timeout := '00:01:00',
    max_retries := 3,
    storage_time := '24:00:00'
);


CREATE FUNCTION tasks.schedule_do_another() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO tasks.do_another(payload)
    VALUES (OLD.payload);
    RETURN NEW;
END;
$$;

CREATE TRIGGER schedule_do_another
AFTER UPDATE ON tasks.do_something
FOR EACH ROW
WHEN (OLD.finished_at IS NULL AND NEW.finished_at IS NOT NULL)
EXECUTE FUNCTION tasks.schedule_do_another();
