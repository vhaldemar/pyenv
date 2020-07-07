from abc import abstractmethod

from .serialization import Serializer, Deserializer, PrimitiveDump, ComponentDump
from .utils import StreamingUtils
from typing import BinaryIO, Set

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
    def __init__(self, change_id: str, name: str, payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, payload, deserialization)
        self._name = name

    def name(self) -> str:
        return self._name

    def _do_apply(self, ns: 'Namespace') -> None:
        loaded = self._deserialization.load(self._payload)
        value = loaded.variables().get(self._name)
        if value is not None:
            ns[self._name] = value
            ns.unmark_dirty(self._name)


class ComponentAtomicChange(PayloadAtomicChange):
    def __init__(self, change_id: str, var_names: Set[str], payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, payload, deserialization)
        self._component_names = set(var_names)

    def component_names(self) -> Set[str]:
        return set(self._component_names)

    def _do_apply(self, ns: 'Namespace') -> None:
        loaded = self._deserialization.load(self._payload)
        variables = loaded.variables()
        for name in self._component_names:
            value = variables.get(name)
            if value is not None:
                ns[name] = value
                ns.unmark_dirty(name)
