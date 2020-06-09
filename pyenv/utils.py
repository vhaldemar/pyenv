from pickle import Pickler
from typing import BinaryIO, Dict, Iterable


def transfer(src: BinaryIO, dst: BinaryIO):
    pass


def to_bytes(src: BinaryIO) -> bytes:
    pass


class PicklerAgent:
    def __init__(self, pickle: Pickler):
        pass

    def clusters(self, variables: Dict[str, object], dirty: Iterable[str]) -> Iterable[Iterable[str]]:
        pass

    def dump_cluster(self, variables: Dict[str, object], cluster: Iterable[str]) -> BinaryIO:
        pass

    def dump_clustered(self, variables: Dict[str, object], dirty: Iterable[str]) -> (BinaryIO, Iterable[Iterable[str]]):
        pass
