import uuid

from typing import Iterable, Dict, Tuple

from ipystate.serialization import Serializer, Deserializer, PrimitiveDump, ComponentDump
from ipystate.change import AtomicChange, PrimitiveAtomicChange, ComponentAtomicChange, RemoveAtomicChange

class Namespace(dict):
    def __init__(self, init: Dict[str, object], serializer: Serializer, deserializer: Deserializer):
        super().__init__(init)
        self._dirty = set()
        self._deleted = set()
        self._serializer = serializer
        self._deserializer = deserializer

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

    # TODO implement exclusions

    # noinspection PyUnresolvedReferences
    def commit(self) -> Iterable[AtomicChange]:
        dumps = self._serializer.dump(super(), self._dirty)

        for dump in dumps:
            change = None
            change_id = str(uuid.uuid1())
            if isinstance(dump, PrimitiveDump):
                change = PrimitiveAtomicChange(change_id, dump.var(), dump.payload(), None)
            elif isinstance(dump, ComponentDump):
                change = ComponentAtomicChange(change_id,
                                               dump.all_vars(),
                                               dump.serialized_vars(),
                                               dump.non_serialized_vars(),
                                               None)
            # elif isinstance(dump, ComponentStructDump):
            #     change = ComponentStructure(change_id, dump.all_vars())
            if change is not None:
                yield change

        for var_name in self._deleted:
            yield RemoveAtomicChange(str(uuid.uuid1()), var_name, None)

        self._deleted.clear()
        self._dirty.clear()
