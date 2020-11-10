import abc
import io
import logging
from xialib.exceptions import XIAFormatError

__all__ = ['Formatter']


class Formatter(metaclass=abc.ABCMeta):
    def __init__(self):
        self.support_formats = []
        self.logger = logging.getLogger("XIA.Fromatter")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    @abc.abstractmethod
    def _format_to_record(self, data_or_io, from_format, **kwargs):
        """
        Format data to record format
        Input must be decoded to 'flat' or 'BufferedIO'
        :param data:
        :param format:
        :return:
        """
        raise NotImplementedError  # pragma: no cover

    def formatter(self, data_or_io, from_format, **kwargs):
        if not data_or_io:
            self.logger.warning("No data or IO found at {}".format(self.__class__.__name__))
            raise XIAFormatError("XIA-000010")

        if from_format not in self.support_formats:
            self.logger.error("Formatter of {} not found at {}".format(from_format, self.__class__.__name__))
            raise XIAFormatError("XIA-000010")

        if not isinstance(data_or_io, (bytes, io.BufferedIOBase)):
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XIAFormatError("XIA-000010")

        for output in self._format_to_record(data_or_io, from_format, **kwargs):
            for line in output:
                yield line
