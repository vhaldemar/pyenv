import importlib
import json
from abc import abstractmethod
from types import CodeType, FunctionType, ModuleType
from typing import Type, Any
import tensorflow as tf
import os
import pybase64

from ipystate.impl.utils import constructor, SAVE_GLOBAL_FUNC_ATTR, SAVE_GLOBAL, is_local_object

class Registry:
    @abstractmethod
    def type(self) -> Type:
        pass

    @abstractmethod
    def reduce(self, obj: object) -> Any:
        pass


@constructor
def _code_constructor(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                      varnames, filename, name, firstlineno, lnotab, freevars, cellvars):
    # noinspection PyTypeChecker
    return CodeType(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                    varnames, filename, name, firstlineno, lnotab, freevars, cellvars)


class CodeRegistry(Registry):
    def type(self) -> Type:
        return CodeType

    def reduce(self, code: object):
        # noinspection PyUnresolvedReferences
        return _code_constructor, (
            code.co_argcount, code.co_kwonlyargcount, code.co_nlocals, code.co_stacksize, code.co_flags, code.co_code,
            code.co_consts, code.co_names, code.co_varnames, code.co_filename, code.co_name, code.co_firstlineno,
            code.co_lnotab, code.co_freevars, code.co_cellvars
        )


@constructor
def _function_constructor(code, fglobals, name, argdefs, closure, kwdefaults, fdict, annotations, qualname,
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


class FuncRegistry(Registry):
    def type(self) -> Type:
        return FunctionType

    def reduce(self, func: object) -> Any:
        if hasattr(func, SAVE_GLOBAL_FUNC_ATTR):
            return SAVE_GLOBAL

        is_lambda_func = func.__name__ == '<lambda>'
        non_global_func = func.__module__ in (None, '__main__', 'builtins') or 'namedtuple' in func.__module__
        modified_dict = func.__dict__ and not (len(func.__dict__) == 1 and '__wrapped__' in func.__dict__)
        if not (is_lambda_func or modified_dict or non_global_func or is_local_object(func)):
            return SAVE_GLOBAL

        # noinspection PyUnresolvedReferences
        return _function_constructor, (
            func.__code__, func.__globals__, func.__name__, func.__defaults__, func.__closure__, func.__kwdefaults__,
            func.__dict__, func.__annotations__, func.__qualname__, func.__doc__, func.__module__,
        )


@constructor
def _module_constructor(name):
    return importlib.import_module(name)


class ModuleRegistry(Registry):
    def type(self) -> Type:
        return ModuleType

    def reduce(self, module: object) -> Any:
        return _module_constructor, (module.__name__,)


# noinspection PyUnresolvedReferences
class TfSessionRegistry(Registry):
    def __init__(self, tmp_path: str):
        self._tmp_path = tmp_path
        self._prefix = 'sess'

    def type(self) -> Type:
        return tf.compat.v1.Session

    def reduce(self, sess: object) -> Any:
        saver = tf.compat.v1.train.Saver(allow_empty=True)
        save_path = self._tmp_path + '/' + self._prefix
        saver.save(sess, save_path)

        data = {}
        prefixed = [filename for filename in os.listdir(self._tmp_path) if filename.startswith(self._prefix)]
        for filename in prefixed:
            path = self._tmp_path + '/' + filename
            with open(path, 'rb') as file:
                data[filename] = pybase64.b64encode(file.read()).decode("ascii")
            os.remove(path)
        json_data = json.dumps(data)
        return self._make_session, (json_data, saver.as_saver_def(), sess.graph)

    def _make_session(self, json_data: str, saver_def, graph):
        data = json.loads(json_data)
        with graph.as_default():
            for filename, value in data.items():
                path = self._tmp_path + '/' + filename
                with open(path, 'wb') as file:
                    file.write(pybase64.b64decode(value))

            saver = tf.compat.v1.train.Saver(saver_def=saver_def, allow_empty=True)
            sess = tf.compat.v1.Session()
            saver.restore(sess, self._tmp_path + '/' + self._prefix)

            for filename in data.keys():
                os.remove(self._tmp_path + '/' + filename)

            return sess


# noinspection PyUnresolvedReferences
class TfTensorRegistry(Registry):
    def type(self) -> Type:
        return tf.Tensor

    def reduce(self, tensor: object) -> Any:
        return self._get_tensor_by_name, (tensor.name, tensor.graph)

    @staticmethod
    def _get_tensor_by_name(name: str, graph):
        return graph.get_tensor_by_name(name)


# noinspection PyUnresolvedReferences
class TfGraphRegistry(Registry):
    def __init__(self, tmp_path: str):
        self._tmp_path = tmp_path

    def type(self) -> Type:
        return tf.Graph

    def reduce(self, graph: object) -> Any:
        path = tf.compat.v1.train.write_graph(graph, self._tmp_path, 'graph.pb', as_text=False)
        with open(path, 'rb') as file:
            data = file.read()
        os.remove(path)
        return self._make_graph, (data,)

    @staticmethod
    def _make_graph(data: bytes):
        graph_def = tf.compat.v1.GraphDef()
        graph_def.ParseFromString(data)
        g = tf.Graph()
        with g.as_default():
            tf.import_graph_def(graph_def, name='')
        return g


# noinspection PyUnresolvedReferences
class TfVarRegistry(Registry):
    def type(self) -> Type:
        return tf.Variable

    def reduce(self, var: object) -> Any:
        return self._make_variable, (var.to_proto(), var.graph)

    @staticmethod
    def _make_variable(proto, graph):
        with graph.as_default():
            return tf.Variable(variable_def=proto)


# noinspection PyUnresolvedReferences
class TfOpRegistry(Registry):
    def type(self) -> Type:
        return tf.Operation

    def reduce(self, op: object) -> Any:
        return self._make_operation, (op.name, op.graph)

    @staticmethod
    def _make_operation(name: str, graph):
        return graph.get_operation_by_name(name)
