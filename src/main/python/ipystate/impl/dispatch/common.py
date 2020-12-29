from ipystate.impl.dispatch.dispatcher import Dispatcher
import threading
import weakref
from types import CodeType, FunctionType, ModuleType
from ipystate.impl.utils import constructor, SAVE_GLOBAL_FUNC_ATTR, SAVE_GLOBAL, is_local_object
import importlib
import _thread


@constructor
def _code_constructor(
    argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names, varnames, filename, name,
    firstlineno, lnotab, freevars, cellvars
):
    # noinspection PyTypeChecker
    return CodeType(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                    varnames, filename, name, firstlineno, lnotab, freevars, cellvars)


def _function_constructor(code):
    pass


class CommonDispatcher(Dispatcher):
    @staticmethod
    def _create_weakref(obj, *args):
        from weakref import ref
        if obj is None:  # it's dead
            from collections import UserDict
            return ref(UserDict(), *args)
        return ref(obj, *args)

    @staticmethod
    def _reduce_weakref(wkref):
        obj = wkref()
        return CommonDispatcher._create_weakref, (obj,)

    @staticmethod
    def _reduce_code(code):
        return _code_constructor, (
            code.co_argcount, code.co_kwonlyargcount, code.co_nlocals, code.co_stacksize, code.co_flags, code.co_code,
            code.co_consts, code.co_names, code.co_varnames, code.co_filename, code.co_name, code.co_firstlineno,
            code.co_lnotab, code.co_freevars, code.co_cellvars
        )

    @staticmethod
    def _reduce_func(func):
        if hasattr(func, SAVE_GLOBAL_FUNC_ATTR):
            return SAVE_GLOBAL

        is_lambda_func = func.__name__ == '<lambda>'
        non_global_func = func.__module__ in (None, '__main__', 'builtins') or 'namedtuple' in func.__module__
        modified_dict = func.__dict__ and not (len(func.__dict__) == 1 and '__wrapped__' in func.__dict__)
        if not (is_lambda_func or modified_dict or non_global_func or is_local_object(func)):
            return SAVE_GLOBAL

        # noinspection PyUnresolvedReferences
        return _function_constructor, (
            func.__code__,
            # don't pass __globals__, etc to avoid grabbing function's scope
        )

    @staticmethod
    def _reduce_module(module):
        return importlib.import_module, (module.__name__,)

    def register(self, dispatch):
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
