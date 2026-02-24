from datetime import datetime, timedelta
from dataclasses import dataclass, field
import logging
import re
import time
from typing import Callable, TypedDict, ClassVar

import psycopg.rows
from psycopg.sql import SQL, Identifier


class TaskFields(TypedDict, total=False):
    id: int
    created_at: datetime
    scheduled_to: datetime
    started_at: datetime | None
    finished_at: datetime | None
    execution_count: int
    comment: str | None


ConnFactory = Callable[[], psycopg.Connection]


_camel_case_re = re.compile(r'(?<!^)(?=[A-Z])')


@dataclass
class Task:
    conn_factory: ConnFactory
    logger: logging.Logger = None

    ext_schema: ClassVar[str] = 'tasks'
    schema: ClassVar[str] = 'tasks'
    table: ClassVar[str] = None
    interval: ClassVar[float] = 10.0
    batch_size: ClassVar[int] = 50

    _table: str = field(init=False)

    def __post_init__(self):
        self._table = (
            _camel_case_re.sub(
                '_', self.__class__.__name__,
            ).lower()
            if self.table is None
            else self.table
        )

        ext_schema = Identifier(self.ext_schema)

        self._acquire_query = SQL('SELECT * FROM {}.{}(%s)').format(
            ext_schema,
            Identifier(f'acquire_{self.schema}_{self._table}'),
        ).as_bytes()

        self._finish_query = SQL('SELECT * FROM {}.{}(%s, %s, %s)').format(
            ext_schema,
            Identifier(f'finish_{self.schema}_{self._table}'),
        ).as_bytes()

        self._retry_query = SQL('SELECT * FROM {}.{}(%s, %s, %s)').format(
            ext_schema,
            Identifier(f'retry_{self.schema}_{self._table}'),
        ).as_bytes()

        self._reset_query = SQL('SELECT * FROM {}.{}(%s)').format(
            ext_schema,
            Identifier(f'reset_{self.schema}_{self._table}'),
        ).as_bytes()

        self._clean_query = SQL('SELECT * FROM {}.{}(%s)').format(
            ext_schema,
            Identifier(f'clean_{self.schema}_{self._table}'),
        ).as_bytes()

        self.logger = self.logger or logging.getLogger(
            self.__class__.__qualname__
        )

    def acquire(
        self,
        batch_size: int,
        /,
        conn: psycopg.Connection = None,
    ) -> list[TaskFields]:
        if conn is None:
            with self.conn_factory() as conn:
                return self._acquire(conn, batch_size)
        else:
            return self._acquire(conn, batch_size)

    def _acquire(
        self,
        conn: psycopg.Connection,
        batch_size: int,
    ) -> list[TaskFields]:
        cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        return cursor.execute(self._acquire_query, (batch_size, )).fetchall()

    def finish(
        self,
        task_id: int,
        started_at: datetime,
        comment: str = None,
        *,
        conn: psycopg.Connection = None,
    ):
        if conn is None:
            with self.conn_factory() as conn:
                result = conn.execute(
                    self._finish_query,
                    (task_id, started_at, comment),
                )
        else:
            result = conn.execute(
                self._finish_query,
                (task_id, started_at, comment),
            )

        self.logger.info(
            'Task %s #%s has finished',
            self.__class__.__name__,
            task_id,
        )
        return result

    def retry(
        self,
        task_id: int,
        scheduled_to: datetime | timedelta = None,
        comment: str = None,
        *,
        conn: psycopg.Connection = None,
    ):
        if isinstance(scheduled_to, timedelta):
            scheduled_to = datetime.now() + scheduled_to

        if conn is None:
            with self.conn_factory() as conn:
                result = conn.execute(
                    self._retry_query,
                    (task_id, scheduled_to, comment),
                )
        else:
            result = conn.execute(
                self._retry_query,
                (task_id, scheduled_to, comment),
            )

        self.logger.info(
            'Task %s #%s scheduled to retry',
            self.__class__.__name__,
            task_id,
        )
        return result

    def reset(
        self,
        task_id: int,
        *,
        conn: psycopg.Connection = None,
    ):
        if conn is None:
            with self.conn_factory() as conn:
                conn.execute(self._reset_query, (task_id, ))
        else:
            conn.execute(self._reset_query, (task_id, ))

        self.logger.info(
            'Task %s #%s has been reset',
            self.__class__.__name__,
            task_id,
        )

    def clean(
        self,
        finished_before: datetime,
        *,
        conn: psycopg.Connection = None,
    ) -> int:
        if conn is None:
            with self.conn_factory() as conn:
                result = conn.execute(
                    self._clean_query,
                    (finished_before, ),
                ).rowcount
        else:
            result = conn.execute(
                self._clean_query,
                (finished_before, ),
            ).rowcount

        self.logger.info(
            'Task queue %s was cleaned',
            self.__class__.__name__,
        )
        return result

    def loop(self):
        while True:
            tasks = self.acquire(self.batch_size)

            if not tasks:
                self.logger.debug(
                    'No %s in queue, waiting %s secs',
                    self.__class__.__name__,
                    self.interval,
                )
                time.sleep(self.interval)
                continue

            self.logger.debug(
                'Received %s %s from queue',
                len(tasks),
                self.__class__.__name__,
            )
            for task in tasks:
                try:
                    self.run(**task)
                except Exception:
                    self.logger.exception(
                        '%s #%s failed', self.__class__.__name__, task['id'],
                    )

    def run(self, **kwargs):
        raise NotImplemented
