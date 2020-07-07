from abc import abstractmethod
from pickle import Pickler, Unpickler
from typing import BinaryIO, Iterable, Dict, Set

class Dump:
    def __init__(self, payload: BinaryIO, non_serialized_vars: Set[str]):
        self._payload = payload
        self._non_serialized_vars = set(non_serialized_vars)

    def payload(self) -> BinaryIO:
        return self._payload

    def non_serialized_vars(self) -> Set[str]:
        return set(self._non_serialized_vars)


class PrimitiveDump(Dump):
    def __init__(self, payload: BinaryIO, name: str, non_serialized_vars: Set[str]):
        super().__init__(payload, non_serialized_vars)
        self._name = name

    def name(self) -> str:
        return self._name


class ComponentDump(Dump):
    def __init__(self, payload: BinaryIO, var_names: Set[str], non_serialized_vars: Set[str]):
        super().__init__(payload, non_serialized_vars)
        self._var_names = set(var_names)

    def var_names(self) -> Set[str]:
        return set(self._var_names)


class LoadedComponent:
    def __init__(self, variables: Dict[str, object], non_deserialized_vars: Set[str]):
        self._variables = dict(variables)
        self._non_deserialized_vars = set(non_deserialized_vars)

    def variables(self) -> Dict[str, object]:
        return dict(self._variables)

    def non_deserialized_vars(self) -> Set[str]:
        return set(self._non_deserialized_vars)


class Serializer:
    @abstractmethod
    def dump(self, variables: Dict[str, object], dirty: Iterable[str]) -> Iterable[Dump]:
        pass


class Deserializer:
    @abstractmethod
    def load(self, raw: BinaryIO) -> LoadedComponent:
        pass
