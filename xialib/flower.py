import abc
import logging
from typing import List, Tuple


__all__ = ['Flower']

class Flower(metaclass=abc.ABCMeta):
    """Flower can modify data content with x-i-a specifications

    """
    def __init__(self, **kwargs):
        self.logger = logging.getLogger("XIA.Flower")
        self.log_context = {'context': ''}
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @abc.abstractmethod
    def _header_flow(self, data_header: dict, data_body: List[dict]) -> Tuple[dict, list]:
        """ To be implemented function

        The function to be implemented to proceed header data

        Args:
            data_header (:obj:`dict`): Header of header
            data_body (:obj:`list` of :obj:`dict`): Header data in Python dictioany list format

        Returns:
            :obj:`dict` : proceeded header
            :obj:`dict` : proceeded body

        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _body_flow(self, data_header: dict, data_body: List[dict]) -> Tuple[dict, list]:
        """ To be implemented function

        The function to be implemented to proceed body data

        Args:
            data_header (:obj:`dict`): Header of data
            data_body (:obj:`list` of :obj:`dict`): Data in Python dictioany list format

        Returns:
            :obj:`dict` : proceeded header
            :obj:`dict` : proceeded body

        """
        raise NotImplementedError  # pragma: no cover

    def proceed(self, data_header: dict, data_body: List[dict]) -> Tuple[dict, list]:
        """ Public function

        Stateless proceed

        Args:
            data_header (:obj:`dict`): Header of data
            data_body (:obj:`list` of :obj:`dict`): Data in Python dictioany list format

        Returns:
            :obj:`dict` : proceeded header
            :obj:`dict` : proceeded body

        """
        if int(data_header.get('age', 0)) == 1:
            return self._header_flow(data_header, data_body)
        else:
            return self._body_flow(data_header, data_body)

