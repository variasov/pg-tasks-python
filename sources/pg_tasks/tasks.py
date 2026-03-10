from datetime import datetime, timedelta
from dataclasses import dataclass, field
import logging
import re
from typing import TypedDict, ClassVar

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


_camel_case_re = re.compile(r'(?<!^)(?=[A-Z])')


@dataclass
class Task:
    """Base class for tasks."""

    logger: logging.Logger = None

    ext_schema: ClassVar[str] = 'tasks'
    schema: ClassVar[str] = 'tasks'
    table: ClassVar[str] = None
    interval: ClassVar[float] = 60.0
    batch_size: ClassVar[int] = 1

    _table: str = field(init=False)

    def __post_init__(self):
        self._table = (
            _camel_case_re.sub(
                '_', self.__class__.__name__,
            ).lower()
            if self.table is None
            else self.table
        )
        self.schema_table = f'{self.schema}.{self._table}'

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

        self._cancel_query = SQL('SELECT * FROM {}.{}(%s, %s, %s)').format(
            ext_schema,
            Identifier(f'cancel_{self.schema}_{self._table}'),
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
        conn: psycopg.Connection,
        batch_size: int,
    ) -> list[TaskFields]:
        """
        Acquire batch of tasks with size 'batch_size' from DB.

        Batch must be finished before task timeout, else another workers
        can acquire not finished tasks, and result's may be doubled.
        """
        cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        return cursor.execute(self._acquire_query, (batch_size, )).fetchall()

    def finish(
        self,
        conn: psycopg.Connection,
        task_id: int,
        started_at: datetime,
        comment: str = None,
    ):
        """
        Mark task as finished.

        started_at is mandatory. Worker will mark task as finished only if
        task's created_at in db is equal to started_at from worker.
        """
        conn.execute(self._finish_query, (task_id, started_at, comment))
        self.logger.info(
            'Task %s #%s has finished',
            self.__class__.__name__,
            task_id,
        )

    def retry(
        self,
        conn: psycopg.Connection,
        task_id: int,
        scheduled_to: datetime | timedelta = None,
        comment: str = None,
    ):
        """
        Schedule task for retry.

        If scheduled_to present, new execution will start after the time
        in scheduled_to, if not, some worker can execute it immediately.
        """
        if isinstance(scheduled_to, timedelta):
            scheduled_to = datetime.now() + scheduled_to

        conn.execute(self._retry_query, (task_id, scheduled_to, comment))
        self.logger.info(
            'Task %s #%s scheduled to retry',
            self.__class__.__name__,
            task_id,
        )

    def cancel(
        self,
        conn: psycopg.Connection,
        task_id: int,
        comment: str = None,
    ):
        """Cancel task in DB. Another worker can't acquire task anymore."""
        conn.execute(self._cancel_query, (task_id, comment))
        self.logger.info(
            'Task %s #%s was canceled',
            self.__class__.__name__,
            task_id,
        )

    def reset(self, conn: psycopg.Connection, task_id: int) -> None:
        """Reset task state in DB for new execution."""
        conn.execute(self._reset_query, (task_id, ))
        self.logger.info(
            'Task %s #%s has been reset',
            self.__class__.__name__,
            task_id,
        )

    def clean(self, conn: psycopg.Connection, finished_before: datetime) -> int:
        """Purge tasks, finished before finished_before parameter"""
        result = conn.execute(self._clean_query, (finished_before, )).rowcount
        self.logger.info(
            'Task queue %s was cleaned',
            self.__class__.__name__,
        )
        return result

    def run(self, **kwargs):
        raise NotImplemented
