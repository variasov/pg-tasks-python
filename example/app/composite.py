import logging
from types import SimpleNamespace

from classic.db_tools import ConnectionPool, Engine
from classic.operations import Operation
import psycopg

from pg_tasks import Worker

from .settings import Settings
from . import tasks, use_cases


logging.basicConfig(level=logging.DEBUG)

settings = Settings()

DB = SimpleNamespace()
DB.pool = ConnectionPool(
    lambda: psycopg.connect(
        str(settings.DB_URL),
        autocommit=True,
    ),
    limit=settings.DB_POOL_SIZE,
)
DB.engine = Engine('', DB.pool)

APP = SimpleNamespace()
APP.operation = Operation(DB.engine)
APP.do_something = use_cases.DoSomething(
    db=DB.engine,
    operation_=APP.operation,
)

TASKS = SimpleNamespace()
TASKS.do_something = tasks.DoSomething(
    db=DB.engine,
    use_case=APP.do_something,
    operation_=APP.operation,
)
TASKS.do_another = tasks.DoAnother(
    db=DB.engine,
    operation_=APP.operation,
)
TASKS.worker = Worker(str(settings.DB_URL))
TASKS.worker.add(TASKS.do_something)
TASKS.worker.add(TASKS.do_another)
