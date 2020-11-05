import _thread
import copyreg
import importlib
import json
import os
import threading
import weakref
from types import CodeType, FunctionType, ModuleType
from ipystate.impl.utils import constructor, SAVE_GLOBAL_FUNC_ATTR, SAVE_GLOBAL, is_local_object
import pandas as pd
import pyarrow as pa
import pybase64


class _DataFrameParquetAdapter:
    """
    Handles limitations of parquet format
    """

    PREFIX = 'pq_converted'
    PREFIX_INT = PREFIX + ':' + 'int' + ':'

    @staticmethod
    def _all_str(cols) -> bool:
        for c in cols:
            if type(c) is not str:
                return False
        return True

    @staticmethod
    def _is_pq_converted(cols) -> bool:
        for c in cols:
            if isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX):
                return True
        return False

    @staticmethod
    def prepare_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
        df1 = df.copy(deep=False)
        if not _DataFrameParquetAdapter._all_str(df1):
            cols = []
            for c in df.columns:
                typed_name = str(c)
                if isinstance(c, int):
                    typed_name = _DataFrameParquetAdapter.PREFIX_INT + str(c)
                cols.append(typed_name)
            df1.columns = cols
        return df1

    @staticmethod
    def restore_after_parquet(df: pd.DataFrame) -> pd.DataFrame:
        df1 = df
        if _DataFrameParquetAdapter._is_pq_converted(df.columns):
            cols = []
            for c in df.columns:
                if isinstance(c, str) and c.startswith(_DataFrameParquetAdapter.PREFIX_INT):
                    try:
                        ci = int(c[len(_DataFrameParquetAdapter.PREFIX_INT):])
                        cols.append(ci)
                    except:
                        cols.append(c)
                else:
                    cols.append(c)
            df1.columns = cols
        return df1


class Dispatcher:
    def __init__(self):
        self._tmp_path = os.getenv('IPYSTATE_DISPATCHER_TMP', '/tmp/ipystate/dispatcher')

    @staticmethod
    def _reduce_without_args(_type):
        def reduce_impl(_):
            return _type, ()

        return reduce_impl

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

    @staticmethod
    def _reduce_weakref(wkref):
        def create_weakref(obj, *args):
            from weakref import ref
            if obj is None:  # it's dead
                from collections import UserDict
                return ref(UserDict(), *args)
            return ref(obj, *args)

        obj = wkref()
        return create_weakref, (obj,)

    @staticmethod
    def _reduce_code(code):
        @constructor
        def code_constructor(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                             varnames, filename, name, firstlineno, lnotab, freevars, cellvars):
            # noinspection PyTypeChecker
            return CodeType(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                            varnames, filename, name, firstlineno, lnotab, freevars, cellvars)

        # noinspection PyUnresolvedReferences
        return code_constructor, (
            code.co_argcount, code.co_kwonlyargcount, code.co_nlocals, code.co_stacksize, code.co_flags, code.co_code,
            code.co_consts, code.co_names, code.co_varnames, code.co_filename, code.co_name, code.co_firstlineno,
            code.co_lnotab, code.co_freevars, code.co_cellvars
        )

    @staticmethod
    def _reduce_func(func):
        @constructor
        def function_constructor(code, fglobals, name, argdefs, closure, kwdefaults, fdict, annotations, qualname,
                                 doc,
                                 module):
            # noinspection PyTypeChecker
            func = FunctionType(code, fglobals, name, argdefs, closure)
            func.__kwdefaults__ = kwdefaults
            func.__dict__ = fdict
            func.__annotations__ = annotations
            func.__qualname__ = qualname
            func.__doc__ = doc
            func.__module__ = module
            return func

        if hasattr(func, SAVE_GLOBAL_FUNC_ATTR):
            return SAVE_GLOBAL

        is_lambda_func = func.__name__ == '<lambda>'
        non_global_func = func.__module__ in (None, '__main__', 'builtins') or 'namedtuple' in func.__module__
        modified_dict = func.__dict__ and not (len(func.__dict__) == 1 and '__wrapped__' in func.__dict__)
        if not (is_lambda_func or modified_dict or non_global_func or is_local_object(func)):
            return SAVE_GLOBAL

        # noinspection PyUnresolvedReferences
        return function_constructor, (
            func.__code__, func.__globals__, func.__name__, func.__defaults__, func.__closure__, func.__kwdefaults__,
            func.__dict__, func.__annotations__, func.__qualname__, func.__doc__, func.__module__,
        )

    @staticmethod
    def _reduce_module(module):
        return importlib.import_module, (module.__name__,)

    @staticmethod
    def _reduce_dataframe(df):
        def create_df(bb) -> pd.DataFrame:
            br = pa.BufferReader(bb)
            df_ = pd.read_parquet(br, engine='pyarrow')
            return _DataFrameParquetAdapter.restore_after_parquet(df_)

        buffer_os = pa.BufferOutputStream()
        df1 = _DataFrameParquetAdapter.prepare_for_parquet(df)
        df1.to_parquet(buffer_os, engine='pyarrow')
        buffer = buffer_os.getvalue()
        return create_df, (buffer,)

    def register_common_reducers(self, dispatch):
        dispatch[CodeType] = self._reduce_code
        dispatch[FunctionType] = self._reduce_func
        dispatch[ModuleType] = self._reduce_module
        dispatch[_thread.LockType] = self._reduce_without_args(_thread.LockType)
        # noinspection PyUnresolvedReferences
        dispatch[_thread.RLock] = self._reduce_without_args(_thread.RLock)
        # noinspection PyUnresolvedReferences
        dispatch[_thread._local] = self._reduce_without_args(_thread._local)
        dispatch[threading.Thread] = self._reduce_without_args(threading.Thread)
        dispatch[weakref.ReferenceType] = self._reduce_weakref
        dispatch[pd.DataFrame] = self._reduce_dataframe

    def register_tf_reducers(self, dispatch):
        try:
            import tensorflow as tf
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
            copyreg.dispatch_table.update(dispatch)
        except ImportError:
            pass
        except Exception:
            pass
