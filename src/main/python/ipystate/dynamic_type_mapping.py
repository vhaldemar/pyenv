from abc import ABC
from collections import defaultdict, MutableMapping
from typing import Mapping, Tuple, Union, VT_co


class DynamicTypeMapping(MutableMapping, ABC):
    Key = Union[type, Tuple[str, str]]

    def __init__(self, data: Mapping[Key, VT_co] = ()):
        self._module_name_value = defaultdict(dict)
        for item in data:
            print(item)
            self[item] = data[item]

    def __getitem__(self, key: Key):
        if isinstance(key, type):
            module = key.__module__
            name = key.__name__
        elif isinstance(key, tuple):
            module, name = key
        else:
            raise TypeError(key)
        name_value = self._module_name_value[module]
        if name not in name_value:
            raise KeyError(key)
        return name_value[name]

    def __delitem__(self, key: Key):
        if isinstance(key, type):
            module = key.__module__
            name = key.__name__
        elif isinstance(key, tuple):
            module, name = key
        else:
            raise TypeError(key)
        del self._module_name_value[module][name]

    def __setitem__(self, key: Key, value):
        if isinstance(key, type):
            module = key.__module__
            name = key.__name__
        elif isinstance(key, tuple):
            module, name = key
        else:
            raise TypeError(key)
        self._module_name_value[module][name] = value

    def __iter__(self):
        return ((module, name) for module, name_value in self._module_name_value.items() for name in name_value)

    def __len__(self):
        return sum(map(len, self._module_name_value), 0)

    def __repr__(self):
        return repr(self._module_name_value)
