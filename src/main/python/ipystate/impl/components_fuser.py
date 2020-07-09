from typing import Set, Iterable


class ComponentsFuser:
    @staticmethod
    def fuse(touched_names: Set[str], components: Iterable[Set[str]]) -> Set[str]:
        names_to_serialize = set()
        for comp in components:
            common = touched_names & comp
            if common:
                names_to_serialize.update(comp)
        names_to_serialize.update(touched_names)
        return names_to_serialize
