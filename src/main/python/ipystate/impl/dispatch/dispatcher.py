from abc import abstractmethod


class Dispatcher:
    @staticmethod
    def _reduce_without_args(_type):
        def reduce_impl(_):
            return _type, ()

        return reduce_impl

    @abstractmethod
    def register(self, dispatch):
        pass
