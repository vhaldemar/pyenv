from abc import abstractmethod
from typing import BinaryIO, Iterable, Dict, Set, Tuple, Any

from .impl.components_fuser import ComponentsFuser
from .impl.walker import Walker

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

    def __init__(self):
        self._walker = Walker()

    @abstractmethod
    def _is_persistable_var(self, name: str) -> bool:
        pass

    @abstractmethod
    def _is_primitive(self, value: Any) -> bool:
        pass

    def _compute_affected(self, variables: Dict[str, object], dirty: Iterable[str]) -> Tuple[Set[str],Iterable[Set[str]]]:
        touched_names = set(filter(self._is_persistable_var, dirty))

        all_components = self._walker.walk(
            {name: variables.get(name) for name in variables.keys() if self._is_persistable_var(name)}
        )

        affected_var_names = ComponentsFuser.fuse(touched_names, all_components)
        return affected_var_names, all_components

    def _dump_component(self, component: Set[str], all_variables: Dict[str, object]) -> Dump:
        if len(component) == 1 and self._is_primitive(all_variables.get(list(component)[0])):
            # TODO payload
            return PrimitiveDump(name=list(component)[0], payload=None)
        else:
            # TODO payload and non_serialized_vars
            return ComponentDump(var_names=component, payload=None, non_serialized_vars=set())

    def dump(self, variables: Dict[str, object], dirty: Iterable[str]) -> Tuple[Iterable[ComponentStruct], Iterable[Dump]]:
        affected_var_names, components = self._compute_affected(variables, dirty)

        dumps = []
        for component in components:
            if len(component & affected_var_names) > 0:
                dumps.append(self._dump_component(component, variables))

        components_structs = list(map(lambda c: ComponentStruct(c), components))
        return components_structs, dumps


class Deserializer:
    @abstractmethod
    def load(self, raw: BinaryIO) -> LoadedComponent:
        pass
