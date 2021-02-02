import abc
import uuid

from typing import Iterable, Set, FrozenSet

from ipystate.change import AtomicChange, PrimitiveAtomicChange, ComponentAtomicChange, RemoveAtomicChange
from ipystate.serialization import Serializer, PrimitiveDump, ComponentDump
from ipystate.impl.changedetector import ChangeDetector, ChangeStage, ChangedState
from ipystate.impl.walker import Walker
from ipystate.logger import Logger


class CellEffects:
    def __init__(self, touched: Iterable[str], deleted: Iterable[str]):
        self._touched = frozenset(touched)
        self._deleted = frozenset(deleted)

    @property
    def touched(self) -> FrozenSet[str]:
        return self._touched

    @property
    def deleted(self) -> FrozenSet[str]:
        return self._deleted


class State(abc.ABC):
    @abc.abstractmethod
    def start_transaction(self) -> None:
        pass

    @abc.abstractmethod
    def pre_cell(self) -> None:
        pass

    @abc.abstractmethod
    def post_cell(self) -> None:
        pass

    @abc.abstractmethod
    def end_transaction(self) -> CellEffects:
        pass

    @abc.abstractmethod
    def varnames(self) -> Iterable[str]:
        pass

    @abc.abstractmethod
    def __contains__(self, item):
        pass

    @abc.abstractmethod
    def __getitem__(self, varname):
        pass

    @abc.abstractmethod
    def __setitem__(self, varname, value):
        pass

    @abc.abstractmethod
    def __delitem__(self, varname):
        pass


class StateManager(abc.ABC):
    def __init__(self, state: State, serializer: Serializer, change_detector: ChangeDetector, logger: Logger = None):
        self._state = state
        self._comps0 = []
        self._serializer = serializer
        self._walker = Walker(logger=logger, dispatch_table=serializer.configurable_dispatch_table)
        self._change_detector = change_detector
        self._in_transaction = False
        self._logger = logger

    @property
    def state(self) -> State:
        return self._state

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    @property
    def change_detector(self) -> ChangeDetector:
        return self._change_detector

    def _set_components(self, new_comps: Iterable[Set[str]]) -> None:
        if new_comps is not None:
            self._comps0 = new_comps
        else:
            self._comps0 = self._compute_comps()

    def _fill_ns(self):
        pass

    def pre_cell(self) -> None:
        self._fill_ns()
        self._state.pre_cell()
        if not self._in_transaction:
            self._in_transaction = True
            self._state.start_transaction()

    def post_cell(self) -> None:
        self._state.post_cell()

    @abc.abstractmethod
    def clear_state(self) -> None:
        pass

    def load_failed(self, varname: str, message: str) -> None:
        pass

    @abc.abstractmethod
    def _skip_variable(self, var_name: str) -> bool:
        """
        Subclasses should define variable skipping
        :param var_name:
        :return:
        """
        pass

    def _probably_dirty(self, name: str) -> bool:
        """
        Subclasses may override variable dirty check
        :param var_name:
        :return:
        """
        value = self._state[name]
        change_state = self._change_detector.update(ChangeStage.RAW, name, value)

        return False if (ChangedState.UNCHANGED == change_state) else True

    def _compute_comps(self) -> Iterable[Set[str]]:
        return self._walker.walk(
            {name: self._state[name] for name in self._state.varnames() if not self._skip_variable(name)}
        )

    def _component_dump_changed(self, dump: ComponentDump) -> bool:
        if len(dump.serialized_vars()) == 0:
            # safety fallback
            return True

        if len(dump.non_serialized_vars()) > 0:
            # safety fallback
            return True

        has_changed = False
        for pickled_var in dump.serialized_vars():
            changed_state = self._change_detector.update(ChangeStage.PICKLED, pickled_var[0], pickled_var[1])
            if ChangedState.UNCHANGED != changed_state:
                has_changed = True

        return has_changed

    def post_cell_commit(self) -> Iterable[AtomicChange]:
        self._change_detector.begin()
        try:
            effects = self._state.end_transaction()
            touched = frozenset(filter(lambda v: not self._skip_variable(v), effects.touched))
            deleted = effects.deleted
            probably_dirty = frozenset(filter(self._probably_dirty, touched)).union(deleted)
            comps1 = self._compute_comps()
            dumps = self._serializer.dump(self._state.ns, probably_dirty, self._comps0, comps1)

            for dump in dumps:
                change = None
                change_id = str(uuid.uuid4())
                if isinstance(dump, PrimitiveDump):
                    change = PrimitiveAtomicChange(change_id, dump.var(), dump.payload(), None)
                elif isinstance(dump, ComponentDump) and self._component_dump_changed(dump):
                    change = ComponentAtomicChange(change_id,
                                                   dump.all_vars(),
                                                   dump.serialized_vars(),
                                                   dump.non_serialized_vars(),
                                                   None)
                if change is not None:
                    yield change

            for var_name in deleted:
                yield RemoveAtomicChange(str(uuid.uuid4()), var_name, None)

            self._set_components(new_comps=comps1)
        finally:
            self._change_detector.end()
            self._in_transaction = False
