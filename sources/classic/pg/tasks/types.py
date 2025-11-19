from typing import Callable

from psycopg import Connection


ConnFactory = Callable[[], Connection]
Task = Callable[[], None]
