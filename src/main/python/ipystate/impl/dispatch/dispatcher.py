from abc import abstractmethod


class Dispatcher:
    @staticmethod
    def _reduce_without_args(_type):
        return lambda _: _type, ()

    @abstractmethod
    def register(self, dispatch):
        pass
