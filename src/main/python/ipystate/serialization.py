import sys

from abc import abstractmethod
from typing import BinaryIO, Iterable, Dict, Set, Tuple, Any

from pickle import Pickler, Unpickler

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

    def __init__(self, pickler: Pickler):
        self._pickler = pickler
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

    @abstractmethod
    def _primitive_var_payload(self, name: str, value: Any) -> BinaryIO:
        pass

    def _start_var_pickling(self, name: str, value: Any) -> Pickler:
        return self._pickler

    def _on_var_pickled(self, pickler: Pickler, name: str, value: Any):
        pass

    def _on_var_pickle_error(self, pickler: Pickler, name: str, value: Any, ex: Exception):
        pass

    def _on_var_pickle_finally(self, pickler: Pickler, name: str, value: Any):
        pass

    # @abstractmethod
    # def _component_payload(self, name, value):
    #     pass

    def _dump_component_pickle(self, component: Set[str], all_variables: Dict[str, object]) -> Dump:
        var_names = set()
        payload = bytearray()
        non_serialized_vars = set()

        comp_sorted_vars = sorted(component)
        for var_name in comp_sorted_vars:
            var_value = all_variables.get(var_name)
            pickler = self._start_var_pickling(var_name, var_value)
            try:
                self._on_var_pickled(pickler, var_name, var_value)
            except Exception as e:
                self._on_var_pickle_error(pickler, var_name, var_value, e)
            finally:
                self._on_var_pickle_finally(pickler, var_name, var_value)

        # TODO pickle
        return ComponentDump(var_names=var_names, payload=payload, non_serialized_vars=non_serialized_vars)

    def _dump_component(self, component: Set[str], all_variables: Dict[str, object]) -> Dump:
        if len(component) == 1 and self._is_primitive(all_variables.get(list(component)[0])):
            name = list(component)[0]
            value = all_variables.get(name)
            payload = self._primitive_var_payload(name, value)
            return PrimitiveDump(name=name, payload=payload)
        else:
            return self._dump_component_pickle(component, all_variables)

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
