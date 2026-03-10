from datetime import timedelta

from classic.components import component
from classic.db_tools import Engine
from classic.operations import operation
from pg_tasks import Task

from . import use_cases


@component
class DoSomething(Task):
    db: Engine
    use_case: use_cases.DoSomething

    @operation
    def run(self, id, started_at, **kwargs):
        params = self.use_case.validator.validate_python(kwargs)
        self.use_case.run(params=params)
        self.finish(self.db.conn, id, started_at)


@component
class DoAnother(Task):
    db: Engine

    @operation
    def run(self, id, started_at, payload, **kwargs):
        print('тоже полезное')

        # Самостоятельное завершение без транзакции
        if payload == 'retry':
            self.retry(self.db.conn, id, timedelta(minutes=1))
        else:
            self.finish(self.db.conn, id, started_at)
