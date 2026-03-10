from dataclasses import dataclass
from uuid import UUID
from typing import ClassVar, Optional

from classic.components import component
from classic.scenarios import scenario
from classic.db_tools import Engine
from classic.operations import operation, Operation
from pydantic import TypeAdapter


@dataclass
class DoSomethingParams:
    payload: Optional[str] = None
    trace_id: Optional[UUID] = None


@component
class DoSomething:
    db: Engine
    operation_: Operation

    params: DoSomethingParams = None

    validator: ClassVar[
        TypeAdapter[DoSomethingParams]
    ] = TypeAdapter(DoSomethingParams)

    @scenario
    @operation
    def run(self):
        pass
