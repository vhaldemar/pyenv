from abc import abstractmethod
from pickle import Pickler, Unpickler
from typing import BinaryIO, Iterable, Dict

"""
serialization: extending pickler/unpickler
"""


class CustomPickler(Pickler):
    pass


class CustomUnpickler(Unpickler):
    pass


class PicklerResult:
    value: BinaryIO
    clusters: Iterable[Iterable[str]]
    non_serialized_vars: Iterable[str]


class PicklerAgent:
    @abstractmethod
    def dump_clustered(self, variables: Dict[str, object], dirty: Iterable[str]) -> PicklerResult:
        pass
