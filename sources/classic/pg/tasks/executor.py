from abc import ABC, abstractmethod
from concurrent import futures

from .types import Task


class Executor(ABC):

    @abstractmethod
    def execute(self, *funcs: Task) -> None:
        pass

    @abstractmethod
    @property
    def is_watermark_reached(self) -> bool:
        pass


class SimpleExecutor(Executor):

    def execute(self, *funcs: Task) -> None:
        for fn in funcs:
            fn()

    @property
    def is_watermark_reached(self) -> bool:
        return False


class ThreadPoolExecutor(Executor):
    _executor: futures.ThreadPoolExecutor
    _watermark: int

    def __init__(
        self,
        max_workers: int,
        queue_size: int,
        watermark: int = 0,
    ) -> None:
        self._executor = futures.ThreadPoolExecutor(max_workers=max_workers)
        self._watermark = watermark
        self._queue_size = queue_size

    def execute(self, *funcs: Task) -> None:
        self._executor.map(lambda fn: fn(), funcs)

    @property
    def is_watermark_reached(self) -> bool:
        return
