from abc import abstractmethod

from .decl import VarDecl
from .serialization import Deserializer
from .utils import StreamingUtils
from typing import BinaryIO, Set, Iterable

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


class PayloadAtomicChange(AtomicChange):
    def __init__(self, change_id: str, payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, deserialization)
        self._payload = payload
        self._processed = False

    def id(self) -> str:
        return self._change_id

    def apply(self, ns: 'Namespace') -> None:
        self._check_and_set_processed()
        self._do_apply(ns)

    def payload(self):
        return self._payload

    def transfer(self, output: BinaryIO) -> None:
        self._check_and_set_processed()
        StreamingUtils.transfer(self._payload, output)

    def _check_and_set_processed(self) -> None:
        if self._processed:
            raise IOError('Data has been already processed')
        self._processed = True

    @abstractmethod
    def _do_apply(self, ns: 'Namespace') -> None:
        pass


class PrimitiveAtomicChange(PayloadAtomicChange):
    def __init__(self, change_id: str, var: VarDecl, payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, payload, deserialization)
        self._var = var
        self._name = var.name()

    def var(self) -> VarDecl:
        return self._var

    def _do_apply(self, ns: 'Namespace') -> None:
        loaded = self._deserialization.load(self._payload)
        value = loaded.variables().get(self._name)
        if value is not None:
            ns[self._name] = value
            ns.unmark_dirty(self._name)


class ComponentAtomicChange(PayloadAtomicChange):
    def __init__(self, change_id: str, all_vars: Set[VarDecl], serialized_vars: Iterable[str], payload: BinaryIO, non_serialized_vars: Set[str],
                 deserialization: Deserializer):
        super().__init__(change_id, payload, deserialization)
        self._all_vars = set(all_vars)
        self._serialized_vars = list(serialized_vars)
        self._non_serialized_vars = set(non_serialized_vars)

    def all_vars(self) -> Set[VarDecl]:
        return set(self._all_vars)

    def serialized_vars(self) -> Iterable[str]:
        return set(self._serialized_vars)

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


class ComponentStructure(PayloadAtomicChange):
    '''
    Unchanged component structure info
    payload is None
    '''
    def __init__(self, change_id: str, all_vars: Set[VarDecl]):
        super().__init__(change_id, payload=None, deserialization=None)
        self._all_vars = set(all_vars)

    def all_vars(self) -> Set[VarDecl]:
        return set(self._all_vars)
