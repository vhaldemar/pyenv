import unittest
import pandas as pd
import numpy as np

from ipystate.impl.dispatch.dataframe import DataframeDispatcher


def dump_load(df):
    f, (bb,) = DataframeDispatcher._reduce_dataframe(df)
    return f(bb)


class SerializationDataframeTest(unittest.TestCase):

    def test_str_columns(self):
        before = pd.DataFrame({"A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
                               "B": [1.243, 4324, 314, 423422.23432, 0, 0, 0, 0, 5.34],
                               "C": ["small", "large", "large", "small", "small", "large", "small", "small", "large"],
                               "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                               "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]})
        after = dump_load(before)
        self.assertTrue(before.equals(after))

    def test_numeric_columns(self):
        before = pd.DataFrame({1: [1, 2, 3, 4, 5],
                               1000: [5, 4, 3, 2, 1],
                               200: [-100, 200, 0.4, 300, 0]})
        after = dump_load(before)
        self.assertTrue(before.equals(after))

    def test_multi_index(self):
        before = pd.DataFrame({(1, 2): ['q', 'w'], (1, 3): ['e', 'r'], (2, 2): ['q', 'w'], (2, 3): ['e', 'r']})
        after = dump_load(before)
        self.assertTrue(before.equals(after))

    def test_partial_tuple_columns(self):
        before = pd.DataFrame({1: ['q', 'w'], 2: ['e', 'r'], (2, 2): ['q', 'w'], (2, 3): ['e', 'r']})
        after = dump_load(before)
        self.assertTrue(before.equals(after))

    def test_pivot(self):
        before = pd.DataFrame(
            {"A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
             "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
             "C": ["small", "large", "large", "small", "small", "large", "small", "small", "large"],
             "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
             "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]}
        ).pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.sum)
        after = dump_load(before)
        self.assertTrue(before.equals(after))

    def test_multi_index_pivot(self):
        before = pd.DataFrame(
            {'id': [1, 1, 1, 2, 2, 2],
             'item1': [1, 2, 1, 2, 1, 2],
             'item2': [1, 1, 2, 2, 2, 2],
             'value': [1, 2, 3, 4, 5, 6]}
        ).pivot_table(index='id', columns='item1')
        after = dump_load(before)
        self.assertTrue(before.equals(after))
