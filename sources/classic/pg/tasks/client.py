from datetime import datetime
from typing import TypedDict

import psycopg.rows


class Task(TypedDict, total=False):
    id: int
    created_at: datetime
    scheduled_to: datetime
    started_at: datetime | None
    finished_at: datetime | None
    execution_count: int
    comment: str | None


class PGTasksClient:
    _acquire_query = (
        "SELECT * FROM {schema}.acquire_{task}("
            "%(batch_size)s"
        ")"
    )
    _finish_query = (
        "SELECT * FROM {schema}.finish_{task}("
            "%(task_id)s, "
            "%(comment)s"
        ")"
    )
    _retry_query = (
        "SELECT * FROM {schema}.retry_{task}("
            "%(task_id)s, "
            "%(scheduled_to), "
            "%(comment)s"
        ")"
    )
    _reset_query = (
        "SELECT * FROM {schema}.reset_{task}(%(task_id)s)"
    )
    _clean_query = (
        "SELECT * FROM {schema}.clean_{task}(%(finished_before)s)"
    )

    def __init__(self, default_batch_size: int = 50):
        self._default_batch_size = default_batch_size

    def acquire(
        self,
        schema: str,
        task: str,
        conn: psycopg.Connection,
        batch_size: int = None,
    ) -> list[Task]:
        sql = self._acquire_query.format(schema=schema, task=task)
        cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        return cursor.execute(
            sql, batch_size or self._default_batch_size,
        ).fetchall()

    def finish_task(
        self,
        schema: str,
        task: str,
        conn: psycopg.Connection,
        task_id: int,
        comment: str = None,
    ):
        sql = self._finish_query.format(schema=schema, task=task)
        conn.execute(
            sql, (task_id, comment),
        )

    def retry_task(
        self,
        schema: str,
        task: str,
        conn: psycopg.Connection,
        task_id: int,
        scheduled_to: datetime = None,
        comment: str = None,
    ):
        sql = self._retry_query.format(schema=schema, task=task)
        conn.execute(
            sql, (task_id, scheduled_to, comment),
        )

    def reset_task(
        self,
        schema: str,
        task: str,
        conn: psycopg.Connection,
        task_id: int,
    ):
        sql = self._reset_query.format(schema=schema, task=task)
        conn.execute(
            sql, (task_id, ),
        )

    def clean_task(
        self,
        schema: str,
        task: str,
        conn: psycopg.Connection,
        finished_before: datetime,
    ):
        sql = self._finish_query.format(schema=schema, task=task)
        return conn.execute(
            sql, (finished_before, ),
        ).rowcount
