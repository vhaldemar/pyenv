from abc import abstractmethod
from enum import Enum


class ChangedState(Enum):
    NEW = 0
    CHANGED = 1
    UNKNOWN = 2
    UNCHANGED = 3


class ChangeStage(Enum):
    RAW = 0
    PICKLED = 1


class ChangeDetector:
    def __init__(self):
        self._raw_cache = dict()

    def begin(self):
        pass

    def end(self):
        pass

    @abstractmethod
    def update(self, stage: ChangeStage, name: str, value: object) -> ChangedState:
        pass


class DummyChangeDetector(ChangeDetector):
    def __init__(self):
        super().__init__()

    def update(self, stage: ChangeStage, name: str, value: object) -> ChangedState:
        return ChangedState.UNKNOWN


class HashChangeDetector(ChangeDetector):
    def __init__(self):
        super().__init__()
        self._hashes = dict()
        self._dispatch = dict()

    def reset_raw_cache(self):
        self._raw_cache = dict()

    def begin(self):
        self.reset_raw_cache()

    def end(self):
        self.reset_raw_cache()

    def _update(self, stage: ChangeStage, name: str, value: object) -> ChangedState:
        try:
            hash_fun = self._dispatch.get(type(value))
            if hash_fun is None:
                return ChangedState.UNKNOWN

            key = str(stage) + "/" + name
            hash1 = hash_fun(value)

            if key not in self._hashes:
                self._hashes[key] = hash1
                return ChangedState.NEW

            hash0 = self._hashes[key]
            self._hashes[key] = hash1

            return ChangedState.UNCHANGED if (hash0 == hash1) else ChangedState.CHANGED
        except Exception as e:
            # TODO log error
            return ChangedState.UNKNOWN

    def update(self, stage: ChangeStage, name: str, value: object) -> ChangedState:
        state = None
        if stage == ChangeStage.PICKLED:
            if name in self._raw_cache:
                cached_state = self._raw_cache[name]
                if cached_state != ChangedState.UNKNOWN:
                    state = cached_state

        if state is None:
            state = self._update(stage, name, value)

        if stage == ChangeStage.RAW:
            self._raw_cache[name] = state

        return state
