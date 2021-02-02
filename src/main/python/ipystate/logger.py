from abc import abstractmethod
from typing import Any
import traceback


class Logger:
    @abstractmethod
    def logger(self, logger_name: str) -> 'Logger':
        """
        Create new logger
        :param logger_name:
        :return:
        """
        pass

    @abstractmethod
    def info(self, message: str) -> None:
        pass

    @abstractmethod
    def debug(self, message: str) -> None:
        pass

    @abstractmethod
    def warn(self, message: str) -> None:
        pass

    @abstractmethod
    def error(self, message: str) -> None:
        pass

    def exception(self, message: Any, e: Exception):
        self.error(message + f' {e}:\n' + traceback.format_exc())