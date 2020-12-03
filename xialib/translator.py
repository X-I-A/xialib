import abc
import logging
from typing import List

__all__ = ['Translator']


class Translator(metaclass=abc.ABCMeta):
    """Translate customer data header / body to x-i-a One

    Attributes:
        spec_list (:obj:`list`): Data specifications supported by Translator

    """
    spec_list = list()

    prefix_dict = {
        'null': [],
        'blob': [],
        'char': [],
        'rowid': ['char'],
        'c_': ['char'],
        'n_': ['char', 'c_@p2@'],
        'date': ['char', 'c_@format_len@'],
        'time': ['char', 'c_@format_len@'],
        'datetime': ['char', 'c_@format_len@'],
        'json': ['char'],
        'int': [],
        'bool': ['int', 'i_1'],
        'i_': ['int'],
        'unix-time': ['int', 'i_4'],
        'ui_': ['int'],
        'real': [],
        'float': ['real'],
        'double': ['real'],
        'd_': ['real'],
        'jd': ['real', 'double']
    }

    def __init__(self, **kwargs):
        self.translate_method = None
        self.logger = logging.getLogger("XIA.Translator")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @classmethod
    def _type_assign(cls, type_item: str, type_syntax: list, type_format: str) -> str:
        if '@p2@' in type_item:
            type_item = type_item.replace('@p2@', type_syntax[1])
        if '@format_len@' in type_item:
            type_item = type_item.replace('@format_len@', str(len(type_format.replace('HH24', 'HH'))))
        return type_item

    @classmethod
    def get_type_chain(cls, src_type: str, type_format: str = None) -> List[str]:
        """Public function

        The function returns all compatible types order by hiearchy

        Args:
            type (:obj:`str`): input type
            type (:obj:`str`): input format
        """
        type_syntax = src_type.split('_')
        if len(type_syntax) > 1:
            type_chain = cls.prefix_dict.get(type_syntax[0] + '_')
        else:
            type_chain = cls.prefix_dict.get(type_syntax[0])
        type_chain = [cls._type_assign(item, type_syntax, type_format) for item in type_chain]
        type_chain.append(src_type)
        return type_chain

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
