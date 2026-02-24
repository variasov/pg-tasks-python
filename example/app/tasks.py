from datetime import timedelta

from classic.components import component
from classic.db_tools import Engine
from pg_tasks import Task


@component
class DoSomething(Task):
    db: Engine

    def run(self, id, started_at, **kwargs):
        # запуск в транзакции с db-tools
        with self.db:
            self.finish(id, started_at, conn=self.db.conn)


@component
class DoAnother(Task):

    def run(self, id, started_at, payload, **kwargs):
        # Самостоятельное завершение без транзакции
        if payload == 'retry':
            self.retry(id, timedelta(minutes=1))
        else:
            self.finish(id, started_at)
