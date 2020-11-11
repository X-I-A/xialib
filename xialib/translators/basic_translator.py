from xialib.translator import Translator

class BasicTranslator(Translator):
    def __init__(self):
        super().__init__()
        self.spec_list = ['x-i-a']

    def _get_origin_line(self, line: dict, **kwargs) -> dict:
        return line

    def _get_header_line(self, line: dict, **kwargs) -> dict:
        return line

    def _get_aged_line(self, line: dict, **kwargs) -> dict:
        line['_AGE'] = int(kwargs['age'])
        return line

    def _get_normal_line(self, line: dict, **kwargs) -> dict:
        line['_SEQ'] = kwargs['start_seq']
        return line

    def init_translator(self, header: dict, data: list):
        if header.get('data_spec', '') == 'x-i-a':
            self.translate_method = self._get_origin_line
        elif int(header.get('age', 0)) == 1:
            self.translate_method = self._get_header_line
        elif 'age' in header:
            self.translate_method = self._get_aged_line
        else:
            self.translate_method = self._get_normal_line
