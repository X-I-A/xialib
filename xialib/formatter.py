import abc
import io
import logging
from typing import Union, List, Generator

__all__ = ['Formatter']


class Formatter(metaclass=abc.ABCMeta):
    """
    Attributes:
        support_formats (:obj:`list`): formats supported by Formatter
    """
    support_formats = list()

    def __init__(self, **kwargs):
        self.logger = logging.getLogger("XIA.Fromatter")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @abc.abstractmethod
    def _format_to_record(self, data_or_io: Union[io.IOBase, bytes],
                          from_format: str, **kwargs) -> List[dict]:
        """ To be implemented function

        The function to be implemented by customized formatter.

        Args:
            data_or_io (:obj:`io.IOBase` or :obj:`bytes`): data to be decoded
            from_format (str): source format

        Yields:
            :obj:`list` of :obj:`dict`
        """
        raise NotImplementedError  # pragma: no cover

    def formatter(self, data_or_io: Union[io.IOBase, bytes],
                  from_format: str, **kwargs) -> Generator[dict, None, None]:
        """ Public function

        This function can format data or io flow into python dictionary data.

        Args:
            data_or_io (:obj:`io.IOBase` or :obj:`bytes`): data to be decoded
            from_format (str): source format

        Yields:
            :obj:`dict`
        """
        if not data_or_io:
            self.logger.warning("No data or IO found at {}".format(self.__class__.__name__))
            raise ValueError("XIA-000004")

        if from_format not in self.support_formats:
            self.logger.error("Formatter of {} not found at {}".format(from_format, self.__class__.__name__))
            raise TypeError("XIA-000005")

        if not isinstance(data_or_io, (bytes, io.IOBase)):
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise TypeError("XIA-000002")

        for output in self._format_to_record(data_or_io, from_format, **kwargs):
            for line in output:
                yield line
