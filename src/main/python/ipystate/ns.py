import uuid

from typing import Iterable, Dict, Tuple

from ipystate.serialization import Serializer, Deserializer, ComponentStruct, PrimitiveDump, ComponentDump
from ipystate.change import AtomicChange, PrimitiveAtomicChange, ComponentAtomicChange, RemoveAtomicChange

class Namespace(dict):
    def __init__(self, init: Dict[str, object], serialization: Serializer, deserialization: Deserializer):
        super().__init__(init)
        self._serialization = serialization
        self._deserialization = deserialization
        self._dirty = set()
        self._deleted = set()

    def __setitem__(self, name: str, value: object) -> None:
        super().__setitem__(name, value)
        self.mark_dirty(name)
        if name in self._deleted:
            self._deleted.remove(name)

    def __getitem__(self, name: str) -> object:
        if super().__contains__(name):
            self.mark_dirty(name)
        return super().__getitem__(name)

    def __delitem__(self, name: str) -> None:
        if super().__contains__(name):
            self._deleted.add(name)
        super().__delitem__(name)

    def mark_dirty(self, path: str) -> None:
        self._dirty.add(path)

    def unmark_dirty(self, path: str) -> None:
        if path in self._dirty:
            self._dirty.remove(path)

    # noinspection PyUnresolvedReferences
    def commit(self) -> Tuple[Iterable[ComponentStruct],Iterable[AtomicChange]]:
        components, dumps = self._serialization.dump(super(), self._dirty)
        changes = []
        for dump in dumps:
            change = None
            change_id = str(uuid.uuid1())
            if isinstance(dump, PrimitiveDump):
                change = PrimitiveAtomicChange(change_id, dump.name(), dump.payload(), self._deserialization)
            elif isinstance(dump, ComponentDump):
                change = ComponentAtomicChange(change_id, dump.var_names(), dump.payload(), self._deserialization)
            if change is not None:
                changes.append(change)

        for var_name in self._deleted:
            changes.append(RemoveAtomicChange(str(uuid.uuid1()), var_name, self._deserialization))

        self._deleted.clear()
        self._dirty.clear()

        return components, changes

