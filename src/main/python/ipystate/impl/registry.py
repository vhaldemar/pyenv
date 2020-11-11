import importlib
from abc import abstractmethod
from types import CodeType, FunctionType, ModuleType
from typing import Type, Any

from ipystate.impl.utils import constructor, SAVE_GLOBAL_FUNC_ATTR, SAVE_GLOBAL, is_local_object


class Registry:
    @abstractmethod
    def type(self) -> Type:
        raise NotImplementedError

    @abstractmethod
    def reduce(self, obj: object) -> Any:
        raise NotImplementedError


@constructor
def _code_constructor(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                      varnames, filename, name, firstlineno, lnotab, freevars, cellvars):
    return CodeType(argcount, kwonlyargcount, nlocals, stacksize, flags, codestring, constants, names,
                    varnames, filename, name, firstlineno, lnotab, freevars, cellvars)


class CodeRegistry(Registry):
    def type(self) -> Type:
        return CodeType

    def reduce(self, code: CodeType):
        return _code_constructor, (
            code.co_argcount, code.co_kwonlyargcount, code.co_nlocals, code.co_stacksize, code.co_flags, code.co_code,
            code.co_consts, code.co_names, code.co_varnames, code.co_filename, code.co_name, code.co_firstlineno,
            code.co_lnotab, code.co_freevars, code.co_cellvars
        )


@constructor
def _function_constructor(code, fglobals, name, argdefs, closure, kwdefaults, fdict, annotations, qualname,
                          doc,
                          module):
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

    def reduce(self, func: FunctionType) -> Any:
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

    def reduce(self, module: ModuleType) -> Any:
        return _module_constructor, (module.__name__,)
