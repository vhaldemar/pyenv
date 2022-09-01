from ipystate.impl.dispatch.dispatcher import Dispatcher
import tensorflow as tf
from tensorflow.python.keras.layers import deserialize, serialize
from tensorflow.python.keras.saving import saving_utils
import os
import json
import pybase64
from packaging import version
import shutil


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

    def _clear_model_files(self, model_folder_path, model_zip_path):
        if os.path.exists(model_zip_path):
            os.remove(model_zip_path)
        if os.path.exists(model_folder_path) and os.path.isdir(model_folder_path):
            shutil.rmtree(model_folder_path)

    def _disable_tf_logs(self):
        prev_level = tf.get_logger().getEffectiveLevel()
        tf.get_logger().setLevel('ERROR')
        prev_cpp_level = os.getenv('TF_CPP_MIN_LOG_LEVEL')
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
        return prev_level, prev_cpp_level

    def _rollback_tf_logger_levels(self, prev_level, prev_cpp_level):
        tf.get_logger().setLevel(prev_level)
        if prev_cpp_level is None:
            del os.environ['TF_CPP_MIN_LOG_LEVEL']
        else:
            os.environ['TF_CPP_MIN_LOG_LEVEL'] = prev_cpp_level

    def _make_model_new(self, data):
        prev_level, prev_cpp_level = self._disable_tf_logs()
        model_path = self._tmp_path + '/model'
        zip_path = model_path + '.zip'
        try:
            with open(zip_path, 'wb') as file:
                file.write(data)
            shutil.unpack_archive(zip_path, model_path, 'zip')
            restored_model = tf.keras.models.load_model(model_path)
        finally:
            self._clear_model_files(model_path, zip_path)
            self._rollback_tf_logger_levels(prev_level, prev_cpp_level)
        return restored_model

    def _reduce_tf_model(self, model):
        prev_level, prev_cpp_level = self._disable_tf_logs()
        model_path = self._tmp_path + '/model'
        zip_path = model_path + '.zip'
        try:
            model.save(model_path)
            shutil.make_archive(model_path, 'zip', model_path)
            with open(zip_path, 'rb') as file:
                data = file.read()
        finally:
            self._clear_model_files(model_path, zip_path)
            self._rollback_tf_logger_levels(prev_level, prev_cpp_level)
        return self._make_model_new, (data,)

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
        if version.parse('2.0.0') <= version.parse(tf.__version__):
            from tensorflow.python.ops.variable_scope import _VariableScopeStore
            dispatch[_VariableScopeStore] = self._reduce_without_args(_VariableScopeStore)
            from tensorflow.python.client._pywrap_tf_session import TF_Graph
            dispatch[TF_Graph] = self._reduce_without_args(TF_Graph)
            if version.parse(tf.__version__) < version.parse('2.5.0'):
                from tensorflow.python._tf_stack import StackSummary
                dispatch[StackSummary] = self._reduce_without_args(StackSummary)
