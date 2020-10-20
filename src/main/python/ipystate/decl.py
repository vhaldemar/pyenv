
class VarDecl:
    def __init__(self, name: str, type: str):
        self._name = name
        self._type = type

    def name(self) -> str:
        return self._name

    def type(self) -> str:
        return self._type
