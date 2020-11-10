import uuid

from typing import Iterable, Dict, Set

from ipystate.serialization import Serializer, Deserializer, PrimitiveDump, ComponentDump
from ipystate.change import AtomicChange, PrimitiveAtomicChange, ComponentAtomicChange, RemoveAtomicChange
from ipystate.impl.walker import Walker
from ipystate.impl.changedetector import ChangeStage, ChangedState, ChangeDetector


class Namespace(dict):
    def __init__(self, init: Dict[str, object], serializer: Serializer, deserializer: Deserializer, change_detector: ChangeDetector):
        super().__init__(init)
        self.armed = True
        self._touched = set()
        self._deleted = set()
        self._comps0 = []
        self._serializer = serializer
        self._deserializer = deserializer
        self._walker = Walker(dispatch_table=serializer.configurable_dispatch_table)
        self._change_detector = change_detector
        self._reset(new_comps=None)

    def _on_reset(self):
        """
        Intended for override
        :return:
        """
        pass
    
    def _reset(self, new_comps: Iterable[Set[str]]) -> None:
        '''
        Reset changes
        '''
        self._on_reset()

        self._touched.clear()
        self._deleted.clear()

        if new_comps is not None:
            self._comps0 = new_comps
        else:
            self._comps0 = self._compute_comps()

    def reset(self):
        # clear dirty/deleted and compute components:
        self._reset(new_comps=None)

    def _skip_variable(self, var_name: str) -> bool:
        """
        Subclasses may override variable skipping
        :param var_name:
        :return:
        """
        return False

    def _probably_dirty(self, name: str) -> bool:
        """
        Subclasses may override variable dirty check
        :param var_name:
        :return:
        """
        if name in self._deleted:
            return True
        value = super().__getitem__(name)

        change_state = self._change_detector.update(ChangeStage.RAW, name, value)

        return False if (ChangedState.UNCHANGED == change_state) else True

    def _compute_comps(self) -> Iterable[Set[str]]:
        return self._walker.walk(
            {name: self.get(name) for name in self.keys() if not self._skip_variable(name)}
        )

    def __setitem__(self, name: str, value: object) -> None:
        super().__setitem__(name, value)
        if self.armed:
            self.mark_touched(name)
            if name in self._deleted:
                self._deleted.remove(name)

    def __getitem__(self, name: str) -> object:
        if self.armed and super().__contains__(name):
            self.mark_touched(name)
        return super().__getitem__(name)

    def __delitem__(self, name: str) -> None:
        if self.armed and super().__contains__(name):
            # we assume deleted variable to be dirty as well,
            # so we can re-serialize components affected by del
            self._deleted.add(name)
            self._touched.add(name)
        super().__delitem__(name)

    def touched(self) -> Set[str]:
        return set(self._touched)

    def is_touched(self, name: str) -> bool:
        return name in self._touched

    def mark_touched(self, name: str) -> None:
        self._touched.add(name)

    def unmark_touched(self, name: str) -> None:
        if name in self._touched:
            self._touched.remove(name)

    def component_dump_changed(self, dump: ComponentDump) -> bool:
        if len(dump.serialized_vars()) == 0:
            # safety fallback
            return True

        if len(dump.non_serialized_vars()) > 0:
            # safety fallback
            return True

        for pickled_var in dump.serialized_vars():
            changed_state = self._change_detector.update(ChangeStage.PICKLED, pickled_var[0], pickled_var[1])
            if ChangedState.UNCHANGED != changed_state:
                return True

        return False

    # noinspection PyUnresolvedReferences
    def commit(self) -> Iterable[AtomicChange]:
        self._change_detector.begin()

        touched = set(filter(lambda v: not self._skip_variable(v), set(self._touched)))
        dirty = set(filter(self._probably_dirty, touched))
        comps1 = self._compute_comps()
        dumps = self._serializer.dump(super(), dirty, self._comps0, comps1)

        for dump in dumps:
            change = None
            change_id = str(uuid.uuid1())
            if isinstance(dump, PrimitiveDump):
                change = PrimitiveAtomicChange(change_id, dump.var(), dump.payload(), None)
            elif isinstance(dump, ComponentDump) and self.component_dump_changed(dump):
                change = ComponentAtomicChange(change_id,
                                               dump.all_vars(),
                                               dump.serialized_vars(),
                                               dump.non_serialized_vars(),
                                               None)
            if change is not None:
                yield change

        for var_name in self._deleted:
            yield RemoveAtomicChange(str(uuid.uuid1()), var_name, None)

        self._reset(new_comps=comps1)
        self._change_detector.end()