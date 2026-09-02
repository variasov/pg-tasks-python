from collections.abc import Callable
from datetime import datetime
from typing import Any, TypedDict


class TaskFields(TypedDict, total=False):
    id: int
    created_at: datetime
    scheduled_to: datetime
    started_at: datetime | None
    finished_at: datetime | None
    execution_count: int
    comment: str | None

TaskFields.__required_keys__ = frozenset({
    'id', 'created_at', 'scheduled_to', 'started_at',
    'finished_at', 'execution_count', 'comment',
})


Callback = Callable[[Any], None]
