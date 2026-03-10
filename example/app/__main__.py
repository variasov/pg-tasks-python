import gevent.monkey
gevent.monkey.patch_all()

from classic.health_checks import HealthCheck

from . import composite


gevent.spawn(HealthCheck)

composite.TASKS.worker.run()
