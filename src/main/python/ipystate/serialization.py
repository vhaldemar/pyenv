from abc import abstractmethod
from typing import BinaryIO, Iterable, Dict, Set, Tuple

class ComponentStruct:
    def __init__(self, var_names: Set[str]):
        self._var_names = var_names

    def var_names(self) -> Set[str]:
        return self._var_names

class Dump:
    def __init__(self, payload: BinaryIO):
        self._payload = payload

    def payload(self) -> BinaryIO:
        return self._payload

class PrimitiveDump(Dump):
    def __init__(self, name: str, payload: BinaryIO):
        super().__init__(payload)
        self._name = name

    def name(self) -> str:
        return self._name

class ComponentDump(Dump):
    def __init__(self, var_names: Set[str], payload: BinaryIO, non_serialized_vars: Set[str]):
        super().__init__(payload)
        self._var_names = set(var_names)
        self._non_serialized_vars = set(non_serialized_vars)

    def var_names(self) -> Set[str]:
        return set(self._var_names)

    def non_serialized_vars(self) -> Set[str]:
        return set(self._non_serialized_vars)

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
    def dump(self, variables: Dict[str, object], dirty: Iterable[str]) -> Tuple[Iterable[ComponentStruct], Iterable[Dump]]:
        pass


class Deserializer:
    @abstractmethod
    def load(self, raw: BinaryIO) -> LoadedComponent:
        pass
