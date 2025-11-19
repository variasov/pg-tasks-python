from pg_tasks_worker import Worker
from psycopg import Connection

from .settings import Settings
from . import tasks


settings = Settings()
conn_factory = lambda: Connection.connect(str(settings.DB_URL))

worker = Worker(conn_factory)

some_task = tasks.SomeTask()
another_task = tasks.AnotherTask()

worker.register(
    some_task.run,
    another_task.run,
)


if __name__ == "__main__":
    worker.run()
