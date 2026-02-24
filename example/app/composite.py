import gevent.monkey
gevent.monkey.patch_all()

import logging

from classic.db_tools import ConnectionPool, Engine
from classic.health_checks import HealthCheck
import psycopg
from gevent_runner import GreenletRunner

from .settings import Settings
from . import tasks


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

do_something = tasks.DoSomething(
    db=db_engine,
    conn_factory=db_pool.connect,
)
do_another = tasks.DoAnother(
    conn_factory=db_pool.connect,
)

worker = GreenletRunner()
worker.add(HealthCheck(), daemon=True)
worker.add(do_something.loop, do_another.loop)
