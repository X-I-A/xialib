from xialib.translator import Translator

class SapTranslator(Translator):
    """
    Supported data formats: ``slt``, ``ddic``
    """
    spec_list = ['slt', 'ddic']

    ddic_dict = {
        'C': ['c_@leng@', None, None],
        'N': ['n_@leng@', None, None],
        'D': ['date', 'YYYYMMDD', None],
        'T': ['time', 'HHMMSS', None],
        'X': ['blob', None, 'b16'],
        'I': ['i_@leng@', None, None],
        'b': ['i_@leng@', None, None],
        's': ['i_@leng@', None, None],
        'P': ['d_@leng@_@decimals@', None, None],
        'F': ['float', None, None],
        'g': ['char', None, None],
        'y': ['blob', None, None],
        'u': ['json', None, None],
        'v': ['json', None, None],
        'h': ['json', None, None],
        'V': ['char', None, None],
        'r': ['json', None, None],
        'l': ['json', None, None],
        'a': ['float', None, None],
        'e': ['double', None, None],
        'j': ['c_1', None, None],
        'k': ['c_1', None, None],
        'z': ['json', None, None],
        '8': ['i_8', None, None],
    }

    def __init__(self):
        super().__init__()
        self.line_oper = dict()

    def _get_ddic_line(self, line: dict, **kwargs):
        new_line = {'_' + key: value for key, value in line.items()}
        new_line['field_name'] = new_line['_FIELDNAME']
        new_line['key_flag'] = new_line.get('_KEYFLAG', '')
        new_line['description'] = new_line.get('_FIELDTEXT', '')
        ddic_parse = self.ddic_dict.get(new_line['_INTTYPE']).copy()
        if '@leng@' in ddic_parse[0]:
            ddic_parse[0] = ddic_parse[0].replace('@leng@', str(new_line['_LENG']))
        if '@decimals@' in ddic_parse[0]:
            ddic_parse[0] = ddic_parse[0].replace('@decimals@', str(new_line['_DECIMALS']))
        new_line['type_chain'] = self.get_type_chain(ddic_parse[0], ddic_parse[1])
        new_line['format'] = ddic_parse[1]
        new_line['encode'] = ddic_parse[2]
        return new_line

    def _get_slt_line(self, line: dict, **kwargs):
        line['_AGE'] = int(kwargs['age'])
        if 'IUUT_OPERAT_FLAG' not in line:
            line.pop('_RECNO')
        else:
            line['_NO'] = line.pop('_RECNO')
            line['_OP'] = line.pop('IUUT_OPERAT_FLAG')
        return line

    def compile(self, header: dict, data: list):
        if header['data_spec'] == 'slt':
            self.translate_method = self._get_slt_line
        elif header['data_spec'] == 'ddic':
            self.translate_method = self._get_ddic_line
