import logging
import time
import uuid
from typing import Any, Callable
import threading

from classic.actors import Actor
from classic.db_tools import ConnectionPool

from . import client


class SimpleWorker(Actor):
    _task_name: str
    _task_callable: Callable[[Any, ...], None]
    _batch_size: int
    _interval: float

    _logger: logging.Logger
    _conn_pool: ConnectionPool
    _stopped: threading.Event

    def __init__(
        self,
        pool: ConnectionPool,
        schema_name,
        task_name: str,
        task_callable: Callable,
        batch_size: int = 50,
        interval: float = 10.0,
        logger: logging.Logger = None,
    ):
        super().__init__()
        self._conn_pool = pool
        self._schema_name = schema_name
        self._task_name = task_name
        self._task_callable = task_callable
        self._batch_size = batch_size
        self._interval = interval
        self._id = str(uuid.uuid4())
        self._logger = logger or logging.getLogger('classic.pg.SimpleWorker')
        self._stopped = threading.Event()
        self._client = client.PGTasksClient()

    def stop(self):
        self._stopped.set()

    def run(self):
        while not self._stopped.is_set():
            with self._conn_pool.connect() as conn:
                tasks = self._client.acquire(
                    self._schema_name,
                    self._task_name,
                    conn,
                )

            if not tasks:
                self._logger.debug(
                    'No %s in queue, waiting %s secs',
                    self._task_name,
                    self._interval,
                )
                time.sleep(self._interval)
                continue

            self._logger.debug(
                'Received %s %s from queue',
                len(tasks),
                self._task_name,
            )
            for task in tasks:
                if self._stopped.is_set():
                    self._logger.info(
                        'Worker %s has stopped, unhandled tasks num: %s',
                        self._task_name,
                        len(tasks),
                    )
                    return

                try:
                    self._task_callable(**task)
                except Exception:
                    self._logger.exception('%s #%s failed', self._task_name)
                else:
                    self._logger.info(
                        '%s #%s executed',
                        self._task_name,
                        task['id'],
                    )
