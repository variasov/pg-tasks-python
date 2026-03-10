import logging
import threading
from typing import Optional

import psycopg
from psycopg_pool import ConnectionPool

from frozendict import frozendict

from .tasks import Task


class WorkerThread(threading.Thread):
    _task: Task
    _pool: ConnectionPool
    _logger: logging.Logger
    _notification_received: threading.Event
    _need_to_stop: threading.Event

    def __init__(
        self,
        task: Task,
        conn_pool: ConnectionPool,
        logger: logging.Logger,
    ) -> None:
        super().__init__()
        self._task = task
        self._conn_pool = conn_pool
        self._logger = logger
        self._need_to_stop = threading.Event()
        self._notification_received = threading.Event()

    def awake(self) -> None:
        self._notification_received.set()

    def run(self):
        while not self._need_to_stop.is_set():
            conn = None
            try:
                conn = self._conn_pool.getconn()
                tasks = self._task.acquire(conn, self._task.batch_size)
            finally:
                if conn is not None:
                    self._conn_pool.putconn(conn)

            if not tasks:
                self._logger.debug(
                    'No %s in queue, waiting %s secs',
                    self._task,
                    self._task.interval,
                )
                if self._notification_received.wait(self._task.interval):
                    self._notification_received.clear()
                continue

            self._logger.debug(
                'Received %s %s from queue',
                len(tasks),
                self._task,
            )
            for task in tasks:
                try:
                    self._task.run(**task)
                except Exception:
                    self._logger.exception(
                        '%s #%s failed', self._task, task['id'],
                    )

    def stop(self):
        self._need_to_stop.set()
        self._notification_received.set()


class Worker:
    """

    """

    heartbeat_interval: float  # In seconds

    _conn_pool: ConnectionPool
    _conn: Optional[psycopg.Connection]
    _registry: frozendict[str, list[WorkerThread]]
    _registry_lock: threading.RLock
    _logger: logging.Logger
    _need_to_stop: threading.Event
    _running: bool

    def __init__(
        self,
        conn_info: Optional[str] = None,
        heartbeat_interval: float = 60.0,
        pool: Optional[ConnectionPool] = None,
        logger: logging.Logger = None,
    ) -> None:
        self._logger = logger or logging.getLogger('pg_tasks.worker')

        assert pool or conn_info, 'conn_info or pool must be provided'

        if pool is not None:
            self._conn_pool = pool
            if (
                self._conn_pool.kwargs is None or
                self._conn_pool.kwargs.get('autocommit', False) is False
            ):
                self._logger.warning('Set autocommit=False in external pool!')
                self._conn_pool.kwargs |= {'autocommit': True}
        else:
            self._conn_pool = ConnectionPool(
                conn_info, kwargs={'autocommit': True},
            )

        self._conn = None

        self.heartbeat_interval = heartbeat_interval

        self._registry = frozendict()
        self._registry_lock = threading.RLock()

        self._need_to_stop = threading.Event()
        self._running = False

    def add(self, task: Task, threads_num: int = 1) -> None:
        """Add task to worker. Start threads_num worker threads"""

        with self._registry_lock:
            if task.schema_table in self._registry:
                return

            threads = [
                WorkerThread(task, self._conn_pool, self._logger)
                for _ in range(threads_num)
            ]
            self._registry |= {task.schema_table: threads}

        if self._running:
            for thread in threads:
                thread.start()

    def remove(self, task: Task) -> None:
        """Remove task, stop worker threads"""

        with self._registry_lock:
            try:
                threads = self._registry[task.schema_table]
            except KeyError:
                return

            self._registry = frozendict({
                key: value
                for key, value in self._registry.items()
                if key != task.schema_table
            })

        if self._running:
            for thread in threads:
                thread.stop()

    def run(self):
        """Start worker. Blocks forever."""

        self._running = True

        with self._registry_lock:
            for threads in self._registry.values():
                for thread in threads:
                    thread.start()

        while not self._need_to_stop.is_set():
            try:
                self._loop()
            except KeyboardInterrupt:
                self._close_conn()
                self._stop_threads()
                break

    # Alias
    __call__ = run

    def stop(self):
        """Send signal to stop."""

        self._need_to_stop.set()

    def _loop(self):
        if not self._ensure_connect():
            return

        try:
            notify = next(self._notifies)
            self._logger.debug('Notify received - %s', notify.payload)

            for thread in self._registry.get(notify.payload, []):
                thread.awake()

        except StopIteration:
            self._logger.debug('Timeout while receiving notification')

            if self._ping():
                self._renew_notifies()

        except psycopg.DatabaseError:
            self._close_conn()

    def _ensure_connect(self) -> bool:
        if self._conn is not None:
            return True

        try:
            self._conn = self._conn_pool.getconn()
            self._conn.execute('LISTEN pg_tasks')
            self._logger.info('Listener connection established')
            self._renew_notifies()

        except psycopg.DatabaseError:
            self._logger.warning('Cannot connect to database for notifications')
            self._close_conn()
            return False

        return True

    def _renew_notifies(self):
        self._notifies = self._conn.notifies(timeout=self.heartbeat_interval)
        self._logger.debug('New notifies generator created')

    def _ping(self) -> bool:
        try:
            self._conn.execute('SELECT 1')
            self._logger.debug('Listener connection ping successful')
            return True

        except psycopg.DatabaseError:
            self._logger.debug('Listener connection ping failed')
            self._close_conn()

        return False

    def _close_conn(self):
        if self._notifies is not None:
            self._notifies.close()

        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._logger.debug('Listener connection closed')

    def _stop_threads(self):
        with self._registry_lock:
            for threads in self._registry.values():
                for thread in threads:
                    thread.stop()
