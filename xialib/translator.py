import abc
import logging
from typing import List

__all__ = ['Translator']


class Translator(metaclass=abc.ABCMeta):
    """
    Attributes:
        spec_list (:obj:`list`): Data specifications supported by Translator
    """
    spec_list = list()

    def __init__(self, **kwargs):
        self.translate_method = None
        self.logger = logging.getLogger("XIA.Translator")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @abc.abstractmethod
    def compile(self, header: dict, data: List[dict]):
        """ To be implemented function

        The function to be implemented by customized translator to set the `translate_method` to the correct
        translation method

        Args:
            header (:obj:`dict`): source header
            data (:obj:`list` of :obj:`dict`): source data
        """
        raise NotImplementedError  # pragma: no cover

    def get_translated_line(self, line: dict, age=None, start_seq=None) -> dict:
        if not self.translate_method:
            raise NotImplementedError
        return self.translate_method(line, age=age, start_seq=start_seq)
