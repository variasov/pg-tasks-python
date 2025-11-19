CREATE TABLE tasks_types (
    table_name text PRIMARY KEY,
    max_retries: int DEFAULT 0
);

CREATE UNLOGGED TABLE workers (
    id UUID PRIMARY KEY
);

CREATE UNLOGGED TABLE workers_tasks(
    worker_id UUID REFERENCES workers(id),
    task_type text REFERENCES tasks_types(table_name)
);

CREATE FUNCTION register_tasks(worker_id UUID, new_tasks text[]) RETURNS UUID
LANGUAGE sql AS $$
    INSERT INTO workers(id) VALUES (worker_id) ON CONFLICT DO NOTHING;
    INSERT INTO workers_tasks(worker_id, task_type)
        SELECT worker_id, task_type
        FROM workers_tasks
    ON CONFLICT DO NOTHING;
    SELECT worker_id;
$$;

CREATE FUNCTION unregister_tasks(worker_id UUID, tasks text[]) RETURNS UUID
LANGUAGE sql AS $$
    DELETE FROM workers_tasks WHERE workers_tasks.worker_id = ANY(tasks);
    SELECT worker_id;
$$;

