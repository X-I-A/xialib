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
        """ To be implemented function

        The function to be implemented by customized formatter.

        Args:
            data_or_io (:obj:`io.BufferedIOBase` or :obj:`bytes`): data to be decoded
            from_format (str): source format

        Yields:
            :obj:`list` of :obj:`dict`
        """
        raise NotImplementedError  # pragma: no cover

    def formatter(self, data_or_io, from_format, **kwargs):
        """ Public function

        This function can format data or io flow into python dictionary data.

        Args:
            data_or_io (:obj:`io.BufferedIOBase` or :obj:`bytes`): data to be decoded
            from_format (str): source format

        Yields:
            :obj:`dict`
        """
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
