import json

from abc import abstractmethod
from typing import BinaryIO, Iterable, Dict, Set, Tuple, Any, IO, Union

# from pickle import Pickler, Unpickler
from cloudpickle import CloudPickler

from .impl.components_fuser import ComponentsFuser
from .impl.walker import Walker
from .impl.memo import ChunkedFile, TransactionalDict

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

class VarDecl:
    def __init__(self, name: str, type: str):
        self._name = name
        self._type = type

    def name(self) -> str:
        return self._name

    def type(self) -> str:
        return self._type

class PrimitiveDump(Dump):
    def __init__(self, var: VarDecl, payload: BinaryIO):
        super().__init__(payload)
        self._var = var

    def var(self) -> VarDecl:
        return self._var

class ComponentDump(Dump):
    def __init__(self, all_vars: Set[VarDecl], serialized_vars: Iterable[str], payload: BinaryIO, non_serialized_vars: Set[str]):
        super().__init__(payload)
        self._all_vars = set(all_vars)
        self._serialized_vars = list(serialized_vars)
        # TODO should we compute it from all_vars - serialized_vars?
        self._non_serialized_vars = set(non_serialized_vars)

    def all_vars(self) -> Set[VarDecl]:
        return set(self._all_vars)

    def serialized_vars(self) -> Iterable[str]:
        return set(self._serialized_vars)

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

class BytesUtil:
    def _string_to_bytes(s: str) -> bytes:
        return s.encode('utf-8')

    def _bytes_to_string(b: bytes) -> str:
        return b.decode('utf-8')

    def _int_to_bytes(x: int) -> bytes:
        return x.to_bytes(8, 'big')

    def _bytes_to_int(b: bytes) -> int:
        return int.from_bytes(b, "big")

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

    @abstractmethod
    def _primitive_var_repr(self, value: Any) -> Tuple[BinaryIO, str]:
        '''
        Should return binary value representation and type string
        '''
        pass

    def _new_pickler(self, file: IO[bytes]):
        return CloudPickler(file)

    def _on_var_serialize_error(self, name: str, value: Any, e: Exception):
        pass

    def _dump_primitive_component(self, name: str, value: Any) -> Dump:
        # TODO allow subclass to skip serializing this variable
        # TODO  ... and report to non serialized
        # TODO  ... and self._on_var_serialize_error()
        payload, type = self._primitive_var_repr(value)
        var = VarDecl(name=name, type=type)

        return PrimitiveDump(var=var, payload=payload)

    def _no_refs(self, value: Any) -> bool:
        '''Variable has no outgoing references and should be pickled in the beginning'''
        return self._is_primitive(value)

    def _sort_component_vars(self, component: Set[str], ns: Dict[str, object]) -> Iterable[str]:
        # secondary sort by name:
        comp_sorted_vars = sorted(component)
        # sort vars with no outgoing references to be first:
        comp_sorted_vars = sorted(comp_sorted_vars, reverse=True, key=lambda varname: self._no_refs(ns.get(varname)))
        return comp_sorted_vars

    def _component_decl(self, component: Set[str], ns: Dict[str, object]) -> Set[VarDecl]:
        all_vars = set()
        for var_name in component:
            var_value = ns.get(var_name)
            var_type = type(var_value)
            var_type_str = var_type.__name__
            all_vars.add(VarDecl(var_name, var_type_str))
        return all_vars

    def _dump_pickle_component(self, component: Set[str], ns: Dict[str, object]) -> Dump:
        serialized_var_names = list()
        non_serialized_var_names = set()

        cf = ChunkedFile()
        pickler = self._new_pickler(cf)
        pickler.memo = TransactionalDict(pickler.memo)

        pickled = bytearray()

        comp_sorted_vars = self._sort_component_vars(component, ns)
        for var_name in comp_sorted_vars:
            var_value = ns.get(var_name)
            # TODO allow subclass to skip serializing this variable
            # TODO  and report to non serialized
            try:
                pickler.dump(var_value)
                pickler.memo.commit()
                chunk = cf.current_chunk()
                pickled.extend(BytesUtil._int_to_bytes(len(chunk)))
                pickled.extend(chunk)
                serialized_var_names.append(var_name)
            except Exception as e:
                pickler.memo.rollback()
                non_serialized_var_names.add(var_name)
                self._on_var_serialize_error(var_name, var_value, e)
            finally:
                cf.reset()

        component_decl = self._component_decl(component, ns)
        return ComponentDump(all_vars=component_decl, serialized_vars=serialized_var_names, payload=pickled, non_serialized_vars=non_serialized_var_names)

    def _dump_component(self, component: Set[str], ns: Dict[str, object]) -> Dump:
        if len(component) == 1 and self._is_primitive(ns.get(list(component)[0])):
            name = list(component)[0]
            value = ns.get(name)
            return self._dump_primitive_component(name, value)
        else:
            return self._dump_pickle_component(component, ns)

    def dump(self, ns: Dict[str, object], dirty: Iterable[str]) -> Tuple[Iterable[ComponentStruct], Iterable[Dump]]:
        affected_var_names, components = self._compute_affected(ns, dirty)

        dumps = []
        for component in components:
            if len(component & affected_var_names) > 0:
                dumps.append(self._dump_component(component, ns))

        components_structs = list(map(lambda c: ComponentStruct(c), components))
        return components_structs, dumps


class Deserializer:
    @abstractmethod
    def load(self, raw: BinaryIO) -> LoadedComponent:
        pass
