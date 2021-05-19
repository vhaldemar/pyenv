import sys

from ipystate.impl.dispatch.dispatcher import Dispatcher
import tensorflow as tf
from tensorflow.python.keras.layers import deserialize, serialize
from tensorflow.python.keras.saving import saving_utils
import os
import json
import pybase64


class TensorflowDispatcher(Dispatcher):
    def __init__(self, tmp_path):
        self._tmp_path = tmp_path
        self._sess_prefix = 'sess'

    @staticmethod
    def _make_model(model, training_config, weights):
        restored_model = deserialize(model)
        if training_config is not None:
            restored_model.compile(
                **saving_utils.compile_args_from_training_config(
                    training_config
                )
            )
        restored_model.set_weights(weights)
        return restored_model

    @staticmethod
    def _reduce_tf_model(model):
        model_metadata = saving_utils.model_metadata(model)
        training_config = model_metadata.get("training_config", None)
        weights = model.get_weights()
        model = serialize(model)
        return TensorflowDispatcher._make_model, (model, training_config, weights)

    @staticmethod
    def _get_tensor_by_name(name: str, graph):
        return graph.get_tensor_by_name(name)

    @staticmethod
    def _reduce_tf_tensor(tensor):
        return TensorflowDispatcher._get_tensor_by_name, (tensor.name, tensor.graph)

    @staticmethod
    def _make_variable(proto, graph):
        with graph.as_default():
            return tf.Variable(variable_def=proto)

    @staticmethod
    def _reduce_tf_var(var):
        return TensorflowDispatcher._make_variable, (var.to_proto(), var.graph)

    @staticmethod
    def _make_operation(name: str, graph):
        return graph.get_operation_by_name(name)

    @staticmethod
    def _reduce_tf_op(op):
        return TensorflowDispatcher._make_operation, (op.name, op.graph)

    @staticmethod
    def _make_graph(data: bytes):
        graph_def = tf.compat.v1.GraphDef()
        graph_def.ParseFromString(data)
        g = tf.Graph()
        with g.as_default():
            tf.import_graph_def(graph_def, name='')
        return g

    def _reduce_tf_graph(self, graph):
        path = tf.compat.v1.train.write_graph(graph, self._tmp_path, 'graph.pb', as_text=False)
        with open(path, 'rb') as file:
            data = file.read()
        os.remove(path)
        return TensorflowDispatcher._make_graph, (data,)

    def _make_session(self, json_data: str, saver_def, graph):
        data = json.loads(json_data)
        with graph.as_default():
            for filename, value in data.items():
                path = self._tmp_path + '/' + filename
                with open(path, 'wb') as file:
                    file.write(pybase64.b64decode(value))

            saver = tf.compat.v1.train.Saver(saver_def=saver_def, allow_empty=True)
            sess = tf.compat.v1.Session()
            saver.restore(sess, self._tmp_path + '/' + self._sess_prefix)

            for filename in data.keys():
                os.remove(self._tmp_path + '/' + filename)

            return sess

    def _reduce_tf_session(self, sess):
        saver = tf.compat.v1.train.Saver(allow_empty=True)
        save_path = self._tmp_path + '/' + self._sess_prefix
        saver.save(sess, save_path)

        data = {}
        prefixed = [filename for filename in os.listdir(self._tmp_path) if filename.startswith(self._sess_prefix)]
        for filename in prefixed:
            path = self._tmp_path + '/' + filename
            with open(path, 'rb') as file:
                data[filename] = pybase64.b64encode(file.read()).decode("ascii")
            os.remove(path)
        json_data = json.dumps(data)
        return self._make_session, (json_data, saver.as_saver_def(), sess.graph)

    def register(self, dispatch):
        dispatch[tf.Tensor] = self._reduce_tf_tensor
        dispatch[tf.compat.v1.Session] = self._reduce_tf_session
        dispatch[tf.Graph] = self._reduce_tf_graph
        dispatch[tf.Variable] = self._reduce_tf_var
        dispatch[tf.Operation] = self._reduce_tf_op
        dispatch[tf.keras.Model] = self._reduce_tf_model
        dispatch[tf.keras.Sequential] = self._reduce_tf_model
        if int(tf.__version__.split('.')[0]) <= 1:
            pass
        else:
            try:
                from tensorflow.python.ops.variable_scope import _VariableScopeStore
                dispatch[_VariableScopeStore] = self._reduce_without_args(_VariableScopeStore)
                if tf.__version__ < '2.5':
                    from tensorflow.python._tf_stack import StackSummary
                    dispatch[StackSummary] = self._reduce_without_args(StackSummary)
            except ModuleNotFoundError:
                print(
                    "Warning: some TensorFlow objects may not be serialized. Try to use TensorFlow 1.5 or 2.3 for full compatibility.",
                    file=sys.stderr)
