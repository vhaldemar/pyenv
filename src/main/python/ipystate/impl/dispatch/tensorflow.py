from ipystate.impl.dispatch.dispatcher import Dispatcher
import tensorflow as tf
import os
import json
import pybase64


class TensorflowDispatcher(Dispatcher):
    def __init__(self, tmp_path):
        self._tmp_path = tmp_path

    @staticmethod
    def _reduce_tf_model(model):
        from tensorflow.python.keras.layers import deserialize, serialize
        from tensorflow.python.keras.saving import saving_utils

        def make_model(model, training_config, weights):
            restored_model = deserialize(model)
            if training_config is not None:
                restored_model.compile(
                    **saving_utils.compile_args_from_training_config(
                        training_config
                    )
                )
            restored_model.set_weights(weights)
            return restored_model

        model_metadata = saving_utils.model_metadata(model)
        training_config = model_metadata.get("training_config", None)
        weights = model.get_weights()
        model = serialize(model)
        return make_model, (model, training_config, weights)

    @staticmethod
    def _reduce_tf_tensor(tensor):
        def get_tensor_by_name(name: str, graph):
            return graph.get_tensor_by_name(name)

        return get_tensor_by_name, (tensor.name, tensor.graph)

    @staticmethod
    def _reduce_tf_var(var):
        def make_variable(proto, graph):
            from tensorflow import Variable
            with graph.as_default():
                return Variable(variable_def=proto)

        return make_variable, (var.to_proto(), var.graph)

    @staticmethod
    def _reduce_tf_op(op):
        def make_operation(name: str, graph):
            return graph.get_operation_by_name(name)

        return make_operation, (op.name, op.graph)

    def _reduce_tf_graph(self, graph):
        import tensorflow as tf

        def make_graph(data: bytes):
            graph_def = tf.compat.v1.GraphDef()
            graph_def.ParseFromString(data)
            g = tf.Graph()
            with g.as_default():
                tf.import_graph_def(graph_def, name='')
            return g

        path = tf.compat.v1.train.write_graph(graph, self._tmp_path, 'graph.pb', as_text=False)
        with open(path, 'rb') as file:
            data = file.read()
        os.remove(path)
        return make_graph, (data,)

    def _reduce_tf_session(self, sess):
        import tensorflow as tf
        prefix = 'sess'

        def make_session(self, json_data: str, saver_def, graph):
            data = json.loads(json_data)
            with graph.as_default():
                for filename, value in data.items():
                    path = self._tmp_path + '/' + filename
                    with open(path, 'wb') as file:
                        file.write(pybase64.b64decode(value))

                saver = tf.compat.v1.train.Saver(saver_def=saver_def, allow_empty=True)
                sess = tf.compat.v1.Session()
                saver.restore(sess, self._tmp_path + '/' + prefix)

                for filename in data.keys():
                    os.remove(self._tmp_path + '/' + filename)

                return sess

        saver = tf.compat.v1.train.Saver(allow_empty=True)
        save_path = self._tmp_path + '/' + prefix
        saver.save(sess, save_path)

        data = {}
        prefixed = [filename for filename in os.listdir(self._tmp_path) if filename.startswith(prefix)]
        for filename in prefixed:
            path = self._tmp_path + '/' + filename
            with open(path, 'rb') as file:
                data[filename] = pybase64.b64encode(file.read()).decode("ascii")
            os.remove(path)
        json_data = json.dumps(data)
        return make_session, (json_data, saver.as_saver_def(), sess.graph)

    def register(self, dispatch):
        dispatch[tf.Tensor] = self._reduce_tf_tensor
        dispatch[tf.keras.models.Model] = self._reduce_tf_model
        dispatch[tf.keras.models.Sequential] = self._reduce_tf_model
        dispatch[tf.compat.v1.Session] = self._reduce_tf_session
        dispatch[tf.Graph] = self._reduce_tf_graph
        dispatch[tf.Variable] = self._reduce_tf_var
        dispatch[tf.Operation] = self._reduce_tf_op

        if int(tf.__version__.split('.')[0]) <= 1:
            pass
        else:
            # noinspection PyUnresolvedReferences
            dispatch[tf.python.ops.variable_scope._VariableScopeStore] = self._reduce_without_args(
                tf.python.ops.variable_scope._VariableScopeStore)
            # noinspection PyUnresolvedReferences
            dispatch[tf.python._tf_stack.StackSummary] = self._reduce_without_args(tf.python._tf_stack.StackSummary)
