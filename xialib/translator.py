import abc
import logging

__all__ = ['Translator']


class Translator(metaclass=abc.ABCMeta):
    def __init__(self):
        self.spec_list = list()
        self.translate_method = None
        self.logger = logging.getLogger("XIA.Translator")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    @abc.abstractmethod
    def init_translator(self, header: dict, data: list):
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
