from typing import Dict

class ChunkedFile:
    def __init__(self):
        self._ba = bytearray()

    def write(self, inp: bytes):
        self._ba.extend(inp)

    def current_chunk(self) -> bytearray:
        return self._ba

    def reset(self) -> None:
        self._ba = bytearray()


class TransactionalDict(dict):
    def __init__(self, original: Dict):
        self._dirty = set()
        self._prev = {}
        super().__init__(original)

    def __setitem__(self, key, value) -> None:
        if key not in self._dirty:
            self._dirty.add(key)
            self._prev[key] = super().get(key, None)
        super().__setitem__(key, value)

    def rollback(self) -> None:
        for k, v in self._prev.items():
            if v is None:
                super().__delitem__(k)
            else:
                super().__setitem__(k, v)
        self.commit()

    def commit(self) -> None:
        self._dirty = set()
        self._prev = {}
