from abc import abstractmethod

from .decl import VarDecl
from .serialization import Deserializer
from typing import BinaryIO, Iterable, Set, Tuple

class AtomicChange:
    def __init__(self, change_id: str, deserialization: Deserializer):
        self._change_id = change_id
        self._deserialization = deserialization

    def id(self) -> str:
        return self._change_id

    @abstractmethod
    def apply(self, ns: 'Namespace') -> None:
        pass


class RemoveAtomicChange(AtomicChange):
    def __init__(self, change_id: str, name: str, deserialization: Deserializer):
        super().__init__(change_id, deserialization)
        self._name = name

    def name(self) -> str:
        return self._name

    def apply(self, ns: 'Namespace') -> None:
        ns.__delitem__(self._name)


class PrimitiveAtomicChange(AtomicChange):
    def __init__(self, change_id: str, var: VarDecl, payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, deserialization)
        self._var = var
        self._name = var.name()
        self._payload = payload

    def var(self) -> VarDecl:
        return self._var

    def payload(self) -> BinaryIO:
        return self._payload

    def _do_apply(self, ns: 'Namespace') -> None:
        loaded = self._deserialization.load(self._payload)
        value = loaded.variables().get(self._name)
        if value is not None:
            ns[self._name] = value
            ns.unmark_dirty(self._name)


class ComponentAtomicChange(AtomicChange):
    def __init__(self, change_id: str, all_vars: Set[VarDecl], serialized_vars: Iterable[Tuple[str, BinaryIO]], non_serialized_vars: Set[str],
                 deserialization: Deserializer):
        super().__init__(change_id, deserialization)
        self._all_vars = set(all_vars)
        self._serialized_vars = list(serialized_vars)
        self._non_serialized_vars = set(non_serialized_vars)

    def all_vars(self) -> Set[VarDecl]:
        return set(self._all_vars)

    def serialized_vars(self) -> Iterable[Tuple[str, BinaryIO]]:
        return list(self._serialized_vars)

    def non_serialized_vars(self) -> Set[str]:
        return set(self._non_serialized_vars)

    def _do_apply(self, ns: 'Namespace') -> None:
        loaded = self._deserialization.load(self._payload)
        variables = loaded.variables()
        for var in self._component_names:
            name = var.name()
            value = variables.get(name)
            if value is not None:
                ns[name] = value
                ns.unmark_dirty(name)


class ComponentStructure(AtomicChange):
    '''
    Unchanged component structure info
    payload is None
    '''
    def __init__(self, change_id: str, all_vars: Set[VarDecl]):
        super().__init__(change_id, deserialization=None)
        self._all_vars = set(all_vars)

    def all_vars(self) -> Set[VarDecl]:
        return set(self._all_vars)
