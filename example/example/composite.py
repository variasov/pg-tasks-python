from classic.pg.tasks import Worker

from . import tasks


worker = Worker(threads=2)

worker.add(tasks.some_task)
