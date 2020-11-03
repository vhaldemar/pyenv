from abc import abstractmethod
from typing import Dict
from enum import Enum

import numpy as np
import pandas as pd
import pandas.api.types as pd_types
import xxhash


class ChangedState(Enum):
    NEW = 0
    CHANGED = 1
    CANT_HASH = 2
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



# class DummyChangeDetector(ChangeDetector):
#     def __init__(self):
#         super().__init__()
#
#     def _update(self, stage: ChangeStage, name: str, value: object) -> ChangedState:
#         pass


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
                return ChangedState.CANT_HASH

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
            return ChangedState.CANT_HASH

    def update(self, stage: ChangeStage, name: str, value: object) -> ChangedState:
        state = None
        if stage == ChangeStage.PICKLED:
            if name in self._raw_cache:
                cached_state = self._raw_cache[name]
                if cached_state != ChangedState.CANT_HASH:
                    state = cached_state

        if state is None:
            state = self._update(stage, name, value)

        if stage == ChangeStage.RAW:
            self._raw_cache[name] = state

        return state

    def store(self) -> Dict[str,str]:
        # TODO implement
        pass

    def load(self, hashes: Dict[str,str]) -> None:
        # TODO implement
        pass


class XXHashChangeDetector(HashChangeDetector):
    @staticmethod
    def hash_np_array(arr: np.ndarray):
        if arr.dtype == object:
            raise TypeError(f"dtype {arr.dtype} is not supported")
        try:
            xx = xxhash.xxh3_64()
            xx.update(arr)
            # h = xx.digest()
            # return h.hex()
            return xx.digest()
        except Exception as e:
            raise TypeError(f"failed to hash {np.ndarray} due to {str(e)}")

    @staticmethod
    def hash_df(df: pd.DataFrame):
        xx = xxhash.xxh3_64()
        xx.update(df.columns.values)
        xx.update(df.index.values)
        for c in df.columns:
            na = df[c].values
            if pd_types.is_categorical_dtype(df[c].dtype):
                na = df[c].factorize()[0]

            if na is not None:
                if not na.flags.contiguous:
                    raise TypeError("not C-contiguous column in dataframe")
                xx.update(na.data)

        # h = xx.digest()
        # return h.hex()
        return xx.digest()

    @staticmethod
    def hash_bytearray(ba: bytearray):
        xx = xxhash.xxh3_64()
        xx.update(ba)
        return xx.digest()

    def __init__(self):
        super().__init__()
        self._dispatch[np.ndarray] = self.hash_np_array
        self._dispatch[pd.DataFrame] = self.hash_df
        self._dispatch[bytearray] = self.hash_bytearray
