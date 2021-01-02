import abc
import io
import logging
from typing import Union

__all__ = ['Storer']


class Storer(metaclass=abc.ABCMeta):
    """
    Attributes:
        support_formats (:obj:`list`): Data store type supported by Storer
    """
    store_types = list()

    def __init__(self, **kwargs):
        self.logger = logging.getLogger("XIA.Storer")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @abc.abstractmethod
    def exists(self, location: str) -> bool:
        """ To be implemented function

        The function to be implemented by customized storer to check if the specified location exists

        Args:
            location (:obj:`str`): resource location

        Returns:
            True if exists else False
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def join(self, *args) -> str:
        """ To be implemented function

        The function to be implemented by customized storer to join all parameters

        Args:
            location (:obj:`str`): resource location

        Returns:
            :obj:`str` : joined path
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def read(self, location: str) -> bytes:
        """ To be implemented function

        The function to be implemented by customized storer to return raw content

        Args:
            location (:obj:`str`): resource location

        Returns:
            :obj:`bytes` : raw content
        """
        raise NotImplementedError  # pragma: no cover

    def get_io_stream(self, location: str):
        """ Return a fake IO stream

        Args:
            location (:obj:`str`): resource location

        Yields:
            :obj:`io.IOBase` : IO flow
        """
        yield io.BytesIO(self.read(location))  # pragma: no cover

class RWStorer(Storer):
    @abc.abstractmethod
    def write(self, data_or_io: Union[io.IOBase, bytes], location: str) -> str:
        """ To be implemented function

        The function to be implemented by customized storer to write to the location

        Args:
            data_or_io (:obj:`io.IOBase` or :obj:`bytes`): Input
            location (:obj:`str`): target location

        Returns:
            :obj:`bytes` : target location
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def remove(self, location: str) -> bool:
        """ To be implemented function

        The function to be implemented by remove resource of specified location

        Args:
            location (:obj:`str`): resource location

        Returns:
            True if ok else False
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def mkdir(self, path: str) -> bool:
        """ To be implemented function

        The function to be implemented to create a directory at the specified path

        Args:
            path (:obj:`str`): directory path to be created

        Returns:
            True if ok else False
        """
        raise NotImplementedError  # pragma: no cover


class IOStorer(RWStorer):
    @abc.abstractmethod
    def get_io_stream(self, location: str):
        """ To be implemented optionaly function

        The function to be implemented by a real IO storer to yield an IO flow.

        Args:
            location (:obj:`str`): resource location

        Yields:
            :obj:`io.IOBase` : IO flow
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_io_wb_stream(self, location: str):
        """ To be implemented function

        The function to be implemented by customized storer to yield an IO Writable flow.

        Args:
            location (:obj:`str`): resource location

        Yields:
            :obj:`io.IOBase` : IO flow
        """
        raise NotImplementedError  # pragma: no cover




