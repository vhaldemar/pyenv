import io
from unittest import TestCase

from ipystate.serialization import Pickler, Unpickler
from cloudpickle import CloudPickler


class TestSerializer(TestCase):
    def test_persisting_namespace(self):
        output = io.BytesIO()
        Pickler(globals(), CloudPickler.dispatch_table, output).dump(lambda: a)
        get_a = Unpickler(globals(), io.BytesIO(output.getvalue())).load()
        globals()['a'] = 'a'
        self.assertEqual('a', get_a())
