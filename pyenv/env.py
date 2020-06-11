import uuid
from abc import abstractmethod
from typing import Iterable, BinaryIO, Dict, Optional, Set

from .serialization import Serializer, Deserializer, PrimitiveDump, ComponentDump
from .utils import StreamingUtils


class Environment(dict):
    def __init__(self, init: Dict[str, object], serialization: Serializer, deserialization: Deserializer):
        super().__init__(init)
        self._serialization = serialization
        self._deserialization = deserialization
        self._dirty = set()

    def __setitem__(self, name: str, value: object) -> None:
        super().__setitem__(name, value)
        self.mark_dirty(name)

    def __getitem__(self, name: str) -> object:
        if super().__contains__(name):
            self.mark_dirty(name)
        return super().__getitem__(name)

    def __delitem__(self, name: str) -> None:
        if super().__contains__(name):
            self.mark_dirty(name)
        super().__delitem__(name)

    @abstractmethod
    def mark_dirty(self, path: str) -> None:
        self._dirty.add(path)

    @abstractmethod
    def unmark_dirty(self, path: str) -> None:
        self._dirty.remove(path)

    # noinspection PyUnresolvedReferences
    @abstractmethod
    def commit(self) -> Iterable[AtomicChange]:
        # 1. Find out dirty vars
        # 2. Compute connected components that contain dirty vars
        # 3. Serialize dirty vars/components using CustomPickler or some human-readable serialization for primitives
        dumps = self._serialization.dump(super(), self._dirty)
        changes = []
        for dump in dumps:
            change = None
            if isinstance(dump, PrimitiveDump):
                change = PrimitiveAtomicChange(str(uuid.uuid1()), dump.name(), dump.value(), self._deserialization)
            if isinstance(dump, ComponentDump):
                change = ComponentAtomicChange(str(uuid.uuid1()), dc.var_names(), dump.value(), self._deserialization)
            if change is not None:
                changes.append(change)
        return changes


class AtomicChange:
    def __init__(self, change_id: str, payload: BinaryIO, deserialization: Deserializer):
        self._change_id = change_id
        self._payload = payload
        self._deserialization = deserialization
        self._processed = False

    def id(self) -> str:
        return self._change_id

    @abstractmethod
    def apply(self, env: Environment) -> None:
        pass

    def transfer(self, output: BinaryIO):
        self._check_and_set_processed()
        StreamingUtils.transfer(self._payload, output)

    def _check_and_set_processed(self) -> None:
        if self._processed:
            raise IOError('Data has been already processed')
        self._processed = True


class PrimitiveAtomicChange(AtomicChange):
    def __init__(self, change_id: str, name: str, payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, payload, deserialization)
        self._name = name

    def name(self) -> str:
        return self._name

    def apply(self, env: Environment) -> None:
        self._check_and_set_processed()
        loaded = self._deserialization.load(self._payload)
        value = loaded.variables().get(self._name)
        if value is not None:
            env[self._name] = value
            env.unmark_dirty(self._name)


class ComponentAtomicChange(AtomicChange):
    def __init__(self, change_id: str, var_names: Set[str], payload: BinaryIO, deserialization: Deserializer):
        super().__init__(change_id, payload, deserialization)
        self._component_names = set(var_names)

    def component_names(self) -> Set[str]:
        return set(self._component_names)

    def apply(self, env: Environment) -> None:
        self._check_and_set_processed()

        loaded = self._deserialization.load(self._payload)
        variables = loaded.variables()

        for name in self._component_names:
            value = variables.get(name)
            if value is not None:
                env[name] = value
                env.unmark_dirty(name)
