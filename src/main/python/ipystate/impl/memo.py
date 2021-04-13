class ChunkedFile:
    def __init__(self):
        self._ba = bytearray()

    def write(self, inp: bytes):
        self._ba.extend(inp)

    def current_chunk(self) -> bytearray:
        return self._ba

    def reset(self) -> None:
        self._ba = bytearray()
