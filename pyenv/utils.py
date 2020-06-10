from pickle import Pickler
from typing import BinaryIO, Dict, Iterable, TypeVar, Generic, Type


def transfer(src: BinaryIO, dst: BinaryIO):
    pass


def to_bytes(src: BinaryIO) -> bytes:
    pass


T = TypeVar('T', bound=Pickler)


class PicklerAgent(Generic[T]):
    def __init__(self,  cls: Type[T]):
        self._cls = cls
        pass

    def clusters(self, variables: Dict[str, object], dirty: Iterable[str]) -> Iterable[Iterable[str]]:
        pass

    def dump_cluster(self, variables: Dict[str, object], cluster: Iterable[str]) -> BinaryIO:
        pass

    def dump_clustered(self, variables: Dict[str, object], dirty: Iterable[str]) -> (BinaryIO, Iterable[Iterable[str]]):
        # pickler = self._cls(...)
        pass
