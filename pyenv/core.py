from abc import abstractmethod
from pickle import Pickler, Unpickler
from typing import Iterable, BinaryIO, Dict, Optional

from .utils import transfer

"""
serialization: extending pickler/unpickler
"""


class CustomPickler(Pickler):
    pass


class CustomUnpickler(Unpickler):
    pass


"""
environment
"""


class Environment(dict):
    def __init__(self, init: Dict[str, object]):
        super().__init__(init)

    def __setitem__(self, name: str, value: object) -> None:
        super().__setitem__(name, value)
        self.mark_dirty(name)

    def __getitem__(self, name: str) -> object:
        if super().__contains__(name):
            self.mark_dirty(name)
        return super().__getitem__(name)

    def __delitem__(self, name: str) -> None:
        if super().__contains__(name):
            self.mark_dirty(name)
        super().__delitem__(name)

    @abstractmethod
    def mark_dirty(self, path: str) -> None:
        pass

    @abstractmethod
    def unmark_dirty(self, path: str) -> None:
        pass

    # noinspection PyUnresolvedReferences
    @abstractmethod
    def commit(self) -> Iterable[AtomicChange]:
        # 1. Find out dirty vars
        # 2. Compute connected components that contain dirty vars
        # 3. Serialize dirty vars/components using CustomPickler or some human-readable serialization for primitives
        pass


class AtomicChange:
    def __init__(self, change_id: str):
        self._change_id = change_id

    def id(self) -> str:
        return self._change_id

    @abstractmethod
    def apply(self, env: Environment) -> None:
        pass

    @abstractmethod
    def transfer(self, output: BinaryIO) -> None:
        pass


class PrimitiveAtomicChange(AtomicChange):
    def __init__(self, change_id: str, name: str, value: bytes):
        super().__init__(change_id)
        self._value = value
        self._name = name

    def name(self) -> str:
        return self._name

    def apply(self, env: Environment) -> None:
        env[self._name] = self._deserialize()
        env.unmark_dirty(self._name)

    def transfer(self, output: BinaryIO) -> None:
        output.write(self._value)

    def _deserialize(self) -> object:
        pass


class PickleComponentAtomicChange(AtomicChange):
    def __init__(self, change_id: str, var_names: Iterable[str], payload: BinaryIO):
        super().__init__(change_id)
        self._payload = payload
        self._component_names = list(var_names)
        self._processed = False

    def component_names(self) -> Iterable[str]:
        return list(self._component_names)

    def transfer(self, output: BinaryIO):
        self._check_and_set_processed()
        transfer(self._payload, output)

    def apply(self, env: Environment) -> None:
        self._check_and_set_processed()
        for name in self._component_names:
            value = self._deserialize_value()
            if value is not None:
                env[name] = value
                env.unmark_dirty(name)

    def _check_and_set_processed(self) -> None:
        if self._processed:
            raise IOError('Data has been already processed')
        self._processed = True

    def _deserialize_value(self) -> Optional[object]:
        # deserialize using CustomUnpickler
        pass
