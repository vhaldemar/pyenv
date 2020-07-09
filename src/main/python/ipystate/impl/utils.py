import builtins
import contextlib
import sys
from typing import Any, Optional, Tuple
from typing import Union

SAVE_GLOBAL = ...
SAVE_GLOBAL_FUNC_ATTR = '_dl_pickle_save_by_name'


class UnsupportedTypeError(Exception):
    pass


def constructor(f):
    setattr(f, SAVE_GLOBAL_FUNC_ATTR, None)  # always save constructor by name
    return f


def is_local_object(obj):
    return '<locals>' in obj.__qualname__


class UnsupportedObject:
    def __init__(self, message):
        self.message = message

    def __getattr__(self, item):
        if item.startswith('__'):
            return self.__getattribute__(item)
        raise UnsupportedTypeError(self.message)


def _getattribute(obj: Any, name: str) -> Tuple[Any, Any]:
    cur = obj
    parent = None
    for subpath in name.split('.'):
        if subpath == '<locals>':
            raise AttributeError("Can't get local attribute {!r} on {!r}".format(name, obj))

        parent = cur
        try:
            cur = getattr(cur, subpath)
        except AttributeError as e:
            raise AttributeError("Can't get attribute {!r} on {!r}".format(name, obj)) from e
    return cur, parent


def _which_module(obj: Any, name: str) -> str:
    """Find the module an object belong to."""
    module_name = getattr(obj, '__module__', None)
    if module_name is not None:
        return module_name
    # Protect the iteration by using a list copy of sys.modules against dynamic
    # modules that trigger imports of other modules upon calls to getattr.
    for module_name, module in list(sys.modules.items()):
        if module_name == '__main__' or module is None:
            continue

        with contextlib.suppress(AttributeError):
            if _getattribute(module, name)[0] is obj:
                return module_name
    return '__main__'


def check_object_importable_by_name(obj: Any, name: Optional[str] = None) -> Optional[Tuple]:
    if name is None:
        name = getattr(obj, '__qualname__', None)
    if name is None:
        name = obj.__name__

    module_name = _which_module(obj, name)
    try:
        __import__(module_name, level=0)
        module = sys.modules[module_name]
        imported_by_name, parent = _getattribute(module, name)
    except (ImportError, KeyError, AttributeError):
        return None

    # check if imported object is same as the original one
    if imported_by_name is not obj:
        return None

    return name, module_name, module, parent


def _type_needs_to_be_saved_as_local(type_: type) -> bool:
    return (is_local_object(type_)
            or type_.__module__ in ('builtins', '__main__') and type_.__name__ not in vars(builtins)
            or not check_object_importable_by_name(type_))


def reduce_type(type_: type) -> Union[Tuple, type(SAVE_GLOBAL)]:
    if _type_needs_to_be_saved_as_local(type_):
        tdict = dict(type_.__dict__)
        tdict.pop('__weakref__', None)
        tdict.pop('__dict__', None)
        return id, (type_.__bases__, tdict)

    return SAVE_GLOBAL
