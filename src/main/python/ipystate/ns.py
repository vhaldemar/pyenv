from typing import Dict, FrozenSet, Iterable

from ipystate.state import State, CellEffects


class Namespace(dict, State):
    def __init__(self, init: Dict[str, object]):
        super().__init__(init)
        self.armed = True
        self._touched = set()
        self._deleted = set()

    def pre_cell(self) -> None:
        self.reset()

    def post_cell(self) -> CellEffects:
        pass

    def varnames(self) -> Iterable[str]:
        return super().keys()

    def reset(self):
        # clear dirty/deleted and compute components:
        self._touched.clear()
        self._deleted.clear()

    def __setitem__(self, name: str, value: object) -> None:
        super().__setitem__(name, value)
        if self.armed:
            self.mark_touched(name)
            if name in self._deleted:
                self._deleted.remove(name)

    def __getitem__(self, name: str) -> object:
        # self.__contains__ as we can override this method
        if self.armed and self.__contains__(name):
            self.mark_touched(name)
        return super().__getitem__(name)

    def __delitem__(self, name: str) -> None:
        # self.__contains__ as we can override this method
        if self.armed and self.__contains__(name):
            # we assume deleted variable to be dirty as well,
            # so we can re-serialize components affected by del
            self._deleted.add(name)
            self._touched.add(name)
        super().__delitem__(name)

    def touched(self) -> FrozenSet[str]:
        return frozenset(self._touched)

    def deleted(self) -> FrozenSet[str]:
        return frozenset(self._deleted)

    def is_touched(self, name: str) -> bool:
        return name in self._touched

    def mark_touched(self, name: str) -> None:
        self._touched.add(name)

    def unmark_touched(self, name: str) -> None:
        if name in self._touched:
            self._touched.remove(name)
