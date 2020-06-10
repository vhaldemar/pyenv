from abc import abstractmethod
from pickle import Pickler, Unpickler
from typing import BinaryIO, Iterable, Dict, Set

from .utils import StreamingUtils


class CustomPickler(Pickler):
    pass


class CustomUnpickler(Unpickler):
    pass


class DumpedComponent:
    def __init__(self, value: BinaryIO, var_names: Set[str], non_serialized_vars: Set[str]):
        self._value = value
        self._var_names = set(var_names)
        self._non_serialized_vars = set(non_serialized_vars)
        self._processed = False

    def var_names(self) -> Set[str]:
        return set(self._var_names)

    def non_serialized_vars(self) -> Set[str]:
        return set(self._non_serialized_vars)

    def transfer(self, output: BinaryIO) -> None:
        if self._processed:
            raise IOError('Data has been already processed')
        self._processed = True
        StreamingUtils.transfer(self._value, output)


class LoadedComponent:
    def __init__(self, variables: Dict[str, object], non_deserialized_vars: Set[str]):
        self._variables = dict(variables)
        self._non_deserialized_vars = set(non_deserialized_vars)

    def variables(self) -> Dict[str, object]:
        return dict(self._variables)

    def non_deserialized_vars(self) -> Set[str]:
        return set(self._non_deserialized_vars)


class Serialization:
    @abstractmethod
    def dump(self, variables: Dict[str, object], dirty: Iterable[str]) -> Iterable[DumpedComponent]:
        pass


class Deserialization:
    @abstractmethod
    def load(self, raw: BinaryIO) -> Iterable[LoadedComponent]:
        pass
