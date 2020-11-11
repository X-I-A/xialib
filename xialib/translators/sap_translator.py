from xialib.translator import Translator

class SapTranslator(Translator):
    def __init__(self):
        super().__init__()
        self.spec_list = ['slt', 'ddic']
        self.line_oper = dict()

    def _get_ddic_line(self, line: dict, **kwargs):
        return line

    def _get_slt_line(self, line: dict, **kwargs):
        line['_AGE'] = int(kwargs['age'])
        if 'IUUT_OPERAT_FLAG' not in line:
            line.pop('_RECNO')
        else:
            line['_NO'] = line.pop('_RECNO')
            line['_OP'] = line.pop('IUUT_OPERAT_FLAG')
        return line

    def init_translator(self, header: dict, data: list):
        if header['data_spec'] == 'slt':
            self.translate_method = self._get_slt_line
        elif header['data_spec'] == 'ddic':
            self.translate_method = self._get_ddic_line
