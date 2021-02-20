import io
from unittest import TestCase

from ipystate.serialization import Pickler, Unpickler
from cloudpickle import CloudPickler


class TestSerializer(TestCase):
    def test_persisting_namespace(self):
        __globals__ = {}
        output = io.BytesIO()
        Pickler(__globals__, CloudPickler.dispatch_table, output, protocol=4).dump(eval('lambda: a', __globals__))
        unpickled = Unpickler(__globals__, io.BytesIO(output.getvalue())).load()
        __globals__['a'] = 'a'
        self.assertEqual('a', unpickled())

    def test_persisting_namespace_when_global_changes(self):
        __globals__ = {'a': 'old'}
        output = io.BytesIO()
        Pickler(__globals__, CloudPickler.dispatch_table, output, protocol=4).dump(eval('lambda: a', __globals__))
        self.assertNotIn(b'old', output.getvalue())
        unpickled = Unpickler(__globals__, io.BytesIO(output.getvalue())).load()
        self.assertEqual('old', unpickled())
        __globals__['a'] = 'new'
        self.assertEqual('new', unpickled())
