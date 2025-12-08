import time

from classic.db_tools import Engine
from classic.components import component


@component
class SomeTask:
    db: Engine

    def run(self, **kwargs):
        print('New task')
        # raise Exception

        with self.db:
            self.db.query(
                'SELECT finish_some_task(%(worker_id)s, %(id)s)', static=True,
            ).execute(**kwargs)
