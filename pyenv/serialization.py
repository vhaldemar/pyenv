import copy
from abc import abstractmethod
from pickle import Pickler, Unpickler
from typing import BinaryIO, Iterable, Dict, Set
from .utils import StreamingUtils


class CustomPickler(Pickler):
    pass


class CustomUnpickler(Unpickler):
    pass


class PicklerResult:
    def __init__(self, value: BinaryIO, clusters: Iterable[Set[str]], non_serialized_vars: Set[str]):
        self._value = value
        self._clusters = copy.deepcopy(clusters)
        self._non_serialized_var = set(non_serialized_vars)
        self._processed = False

    def clusters(self) -> Iterable[Set[str]]:
        return copy.deepcopy(self._clusters)

    def non_non_serialized_vars(self) -> Set[str]:
        return set(self._non_serialized_var)

    def transfer(self, output: BinaryIO) -> None:
        if self._processed:
            raise IOError('Data has been already processed')
        self._processed = True
        StreamingUtils.transfer(self._value, output)


class PicklerAgent:
    @abstractmethod
    def dump_clustered(self, variables: Dict[str, object], dirty: Iterable[str]) -> PicklerResult:
        pass
