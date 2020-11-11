import abc

__all__ = ['Translator']


class Translator(metaclass=abc.ABCMeta):
    def __init__(self):
        self.spec_list = list()
        self.translate_method = None

    @abc.abstractmethod
    def init_translator(self, header: dict, data: list):
        raise NotImplementedError  # pragma: no cover

    def get_translated_line(self, line: dict, age=None, start_seq=None) -> dict:
        if not self.translate_method:
            raise NotImplementedError
        return self.translate_method(line, age=age, start_seq=start_seq)
