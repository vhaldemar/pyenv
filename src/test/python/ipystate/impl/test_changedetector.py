import unittest

from ipystate.impl.changedetector import ChangedState, ChangeStage, XXHashChangeDetector

import pandas as pd
import numpy as np

import pickle


class BufferFile:
    def __init__(self):
        self._ba = bytearray()

    def write(self, inp: bytes):
        self._ba.extend(inp)

    @property
    def buffer(self):
        return self._ba


class TestXXHashChangeDetector(unittest.TestCase):
    def test_unchanged_pandas_df(self):
        xxcd = XXHashChangeDetector()

        df0 = pd.DataFrame(np.random.rand(1024**2, 1), columns=list('A'))

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.UNCHANGED)

        state = xxcd.update(ChangeStage.PICKLED, "df0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.UNCHANGED)

    def test_changed_pandas_df(self):
        xxcd = XXHashChangeDetector()

        df0 = pd.DataFrame(np.random.rand(1024**2, 1), columns=list('A'))

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.UNCHANGED)

        df0 = pd.DataFrame(np.random.rand(1024**2, 1), columns=list('A'))

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.CHANGED)

        state = xxcd.update(ChangeStage.PICKLED, "df0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.CHANGED)

    def test_raw_cache_reset(self):
        xxcd = XXHashChangeDetector()

        df0 = pd.DataFrame(np.random.rand(1024**2, 1), columns=list('A'))

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.UNCHANGED)

        df0 = pd.DataFrame(np.random.rand(1024**2, 1), columns=list('A'))

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.CHANGED)

        state = xxcd.update(ChangeStage.PICKLED, "df0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.CHANGED)

        state = xxcd.update(ChangeStage.PICKLED, "df0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.CHANGED)

        xxcd.end()
        xxcd.begin()

        state = xxcd.update(ChangeStage.PICKLED, "df0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.NEW)

    def test_non_contiguous_pandas_df(self):
        xxcd = XXHashChangeDetector()

        df0 = pd.DataFrame(np.random.rand(1024**2, 4), columns=list('ABCD'))

        state = xxcd.update(ChangeStage.RAW, "df0", df0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        serialized = BufferFile()
        pickle.dump(df0, serialized)


        state = xxcd.update(ChangeStage.PICKLED, "df0", serialized.buffer)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.PICKLED, "df0", serialized.buffer)
        self.assertEqual(state, ChangedState.UNCHANGED)

    def test_unchanged_numpy_array(self):
        xxcd = XXHashChangeDetector()

        a0 = np.random.rand(1024**2, 1)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.UNCHANGED)

        state = xxcd.update(ChangeStage.PICKLED, "a0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.UNCHANGED)

    def test_changed_numpy_array(self):
        xxcd = XXHashChangeDetector()

        a0 = np.random.rand(1024**2, 1)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.UNCHANGED)

        np.put(a0, range(3), np.array([2, 3, 9]))

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CHANGED)

        state = xxcd.update(ChangeStage.PICKLED, "a0", bytearray())
        # should return from raw_cache
        self.assertEqual(state, ChangedState.CHANGED)

    def test_int(self):
        xxcd = XXHashChangeDetector()

        a0 = 123

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.UNCHANGED)

        a0 = 234
        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.CHANGED)

    def test_float(self):
        xxcd = XXHashChangeDetector()

        a0 = 0.123

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.UNCHANGED)

        a0 = 1.23
        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.CHANGED)


    def test_bool(self):
        xxcd = XXHashChangeDetector()

        a0 = True

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.UNCHANGED)

        a0 = False
        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.CHANGED)

    def test_None(self):
        xxcd = XXHashChangeDetector()

        a0 = None

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.UNCHANGED)

        a0 = 123
        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.CHANGED)

    def test_dict(self):
        xxcd = XXHashChangeDetector()

        a0 = {"a": "a", "b": "b", "c": 123}

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        state = xxcd.update(ChangeStage.RAW, "a0", a0)
        self.assertEqual(state, ChangedState.CANT_HASH)

        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.NEW)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.UNCHANGED)

        a0["c"] = 234
        serialized = BufferFile()
        pickle.dump(a0, serialized)

        state = xxcd.update(ChangeStage.PICKLED, "a0", serialized.buffer)
        self.assertEqual(state, ChangedState.CHANGED)
