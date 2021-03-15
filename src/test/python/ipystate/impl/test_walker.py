from unittest import TestCase

from ipystate.impl.walker import Walker


class TestWalker(TestCase):
    def test_walk_modules(self):
        import time
        self.assertCountEqual([frozenset({'a'}), frozenset({'b'})], Walker().walk({'a': [time], 'b': [time]}))
        self.assertCountEqual(
            [frozenset({'a'}), frozenset({'b'}), frozenset({'time'})],
            Walker().walk({'a': [time], 'b': [time], 'time': time}),
        )
