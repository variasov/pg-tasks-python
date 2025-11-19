from dataclasses import dataclass
import threading
import time
from typing import Optional, Sequence
import queue
from uuid import UUID, uuid4

from psycopg import Connection

from . import queries, executor
from .types import Task, ConnFactory


Stop = object()


@dataclass
class Register:
    tasks: dict[str, Task]


@dataclass
class Unregister:
    tasks: Sequence[str]


class Worker:
    STOPPED = 0
    RUNNING = 1
    POLLING = 2
    LISTENING = 3
    STOPPING = 4

    _stopped: bool
    _thread: Optional[threading.Thread]
    _executor: executor.Executor
    _conn_factory: ConnFactory
    _conn: Optional[Connection]
    _tasks: dict[str, Task]
    _state: int
    _timeout: float
    _id: UUID
    
    def __init__(
        self,
        conn_factory: ConnFactory,
        workers_num: int = 1,
        watermark: int = 0,
        workers_queue_size: int = 0,
        timeout: float = 0.1,
    ):
        self._conn_factory = conn_factory
        self._thread = None
        self._inbox = queue.Queue()
        if workers_num > 0:
            self._executor = executor.ThreadPoolExecutor(
                max_workers=workers_num,
                watermark=watermark,
                queue_size=workers_queue_size,
            )
        else:
            self._executor = executor.SimpleExecutor()
        self._tasks = {}
        self._state = self.STOPPED
        self._timeout = timeout
        self._id = uuid4()

    def run(self):
        self._state = self.RUNNING
        while not self._state == self.STOPPING:
            self._consume_inbox()

            if self._state == self.POLLING:
                self._poll()
            elif self._state == self.LISTENING:
                self._listen()

            if self._state == self.STOPPED:
                self._process_results()

        self._state = self.STOPPED

    def _consume_inbox(self) -> None:
        started_at = time.time()

        while not self._inbox.empty():
            message = self._inbox.get(block=False)
            self._react_to_message(message)

        if self._state == self.POLLING:
            timeout = started_at - time.time()
            if timeout > self._timeout:
                return

            try:
                message = self._inbox.get(
                    block=True, timeout=timeout
                )
                self._react_to_message(message)
            except queue.Empty:
                return

    def _react_to_message(self, message):
        if message is Stop:
            self._stop()
        elif isinstance(message, Register):
            self._register(message)
        elif isinstance(message, Unregister):
            self._unregister(message)

    def register(self, **tasks: Task):
        self._inbox.put(Register(tasks))

    def _register(self, message: Register):
        self._tasks.update(message.tasks)
        self._try_to_register_tasks(*message.tasks)

    def unregister(self, *tasks: str):
        self._inbox.put(Unregister(tasks))

    def _unregister(self, message: Unregister):
        for task in message.tasks:
            del self._tasks[task]
        self._try_to_unregister_tasks(*message.tasks)

    def _connect(self) -> None:
        self._conn: Connection = self._conn_factory()
        self._conn.autocommit = True

        self._try_to_register_tasks(*self._tasks.values())

    def _try_to_register_tasks(self, *tasks: str) -> None:
        if not self._conn:
            return
        queries.register_tasks(self._conn, self._id, *tasks)

    def _try_to_unregister_tasks(self, *tasks: str) -> None:
        if not self._conn:
            return
        queries.unregister_tasks(self._conn, self._id, *tasks)

    def _poll(self):
        if self._executor.is_watermark_reached:
            return

        if tasks := queries.get_tasks_batch(
            self._conn,
            limit=self._executor.free_queue_space,
        ):
            self._executor.execute(*tasks)
        elif self._state != self.LISTENING:
            self._state = self.LISTENING

    def _listen(self):
        pass

    def _process_results(self):
        if not self._conn:
            return
        finished = self._executor.get_finished_tasks()
        queries.mark_tasks_as_finished(self._conn, finished)
        self._executor.clear_results(finished)

    def start(self):
        if not self._thread or not self._thread.is_alive():
            # TODO: Без daemon=True потоки не останавливались до завершения
            # выполнения задачи. Нужно найти аналогичное решение для
            # корректной остановки потока.
            self._thread = threading.Thread(target=self.run, daemon=True)
            self._thread.start()

    def join(self, timeout: float = None):
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout)

    def stop(self, block: bool = True, timeout: float = None) -> None:
        """
        Завершает выполнение планировщика.
        """
        if self._thread and self._thread.is_alive():
            self._inbox.put(Stop)
            if block:
                self._thread.join(timeout)

    def _stop(self):
        if self._state not in (self.STOPPED, self.STOPPING):
            self._state = self.STOPPING
