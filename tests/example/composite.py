import logging

from classic.actors import Supervisor
from classic.db_tools import ConnectionPool, Engine
from classic.pg.tasks import SimpleWorker

import psycopg

from .settings import Settings
from .tasks import SomeTask


logging.basicConfig(level=logging.INFO)

settings = Settings()

db_pool = ConnectionPool(
    lambda: psycopg.connect(
        str(settings.DB_URL),
        autocommit=True,
    ),
    limit=settings.DB_POOL_SIZE,
)
db_engine = Engine('', db_pool)

some_task = SomeTask(db=db_engine)

workers = [
    SimpleWorker(
        db_pool,
        'public',
        'some_task',
        some_task.run,
        interval=5,
    ) for _ in range(settings.NUMBER_OF_WORKERS)
]

supervisor = Supervisor()
for worker in workers:
    supervisor.add(worker)

supervisor.run()

# for worker in workers:
#     worker.start()
#
#
# for worker in workers:
#     worker.join()
