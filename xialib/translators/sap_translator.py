from xialib.translator import Translator

class SapTranslator(Translator):
    """
    Supported data formats: ``slt``, ``ddic``
    """
    spec_list = ['slt', 'ddic']

    ddic_dict = {
        'C': ['c_@leng@', None, None, ''],
        'N': ['n_@leng@', None, None, '0*@leng@'],
        'D': ['date', 'YYYY-MM-DD', None, None],
        'T': ['time', 'HH24:MI:SS', None, None],
        'X': ['blob', None, 'b16', ''],
        'I': ['i_@leng@', None, None, 0],
        'b': ['i_@leng@', None, None, None],
        's': ['i_@leng@', None, None, None],
        'P': ['d_@leng@_@decimals@', None, None, 0],
        'F': ['float', None, None, 0.0],
        'g': ['char', None, None, None],
        'y': ['blob', None, None, None],
        'u': ['json', None, None, None],
        'v': ['json', None, None, None],
        'h': ['json', None, None, None],
        'V': ['char', None, None, None],
        'r': ['json', None, None, None],
        'l': ['json', None, None, None],
        'a': ['float', None, None, None],
        'e': ['double', None, None, None],
        'j': ['c_1', None, None, None],
        'k': ['c_1', None, None, None],
        'z': ['json', None, None, None],
        '8': ['i_8', None, None, None],
    }

    slt_op_dict = {
        'I': 'I',
        'U': 'U',
        'N': 'U',
        'D': 'D',
        'A': 'D',
        'M': 'D',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.line_oper = dict()

    def _get_ddic_line(self, line: dict, **kwargs):
        new_line = {'_' + key: value for key, value in line.items()}
        new_line['field_name'] = new_line['_FIELDNAME']
        new_line['key_flag'] = new_line.get('_KEYFLAG', '') == 'X'
        new_line['description'] = new_line.get('_FIELDTEXT', '')
        ddic_parse = self.ddic_dict.get(new_line['_INTTYPE']).copy()
        if '@leng@' in ddic_parse[0]:
            ddic_parse[0] = ddic_parse[0].replace('@leng@', str(new_line['_LENG']))
        if '@decimals@' in ddic_parse[0]:
            ddic_parse[0] = ddic_parse[0].replace('@decimals@', str(new_line.get('_DECIMALS', '0')))
        new_line['type_chain'] = self.get_type_chain(ddic_parse[0], ddic_parse[1])
        new_line['format'] = ddic_parse[1]
        new_line['encode'] = ddic_parse[2]
        if isinstance(ddic_parse[3], str) and '*@leng@' in ddic_parse[3]:
            new_line['default'] = ddic_parse[3][0] * new_line['_LENG']
        else:
            new_line['default'] = ddic_parse[3]
        return new_line

    def _get_slt_line(self, line: dict, **kwargs):
        line['_AGE'] = int(kwargs['age'])
        if 'IUUC_OPERAT_FLAG' not in line:
            line.pop('_RECNO')
        else:
            line['_NO'] = line.pop('_RECNO')
            line['_OP'] = self.slt_op_dict.get(line.pop('IUUC_OPERAT_FLAG'))
        return line

    def compile(self, header: dict, data: list):
        if header['data_spec'] == 'slt':
            self.translate_method = self._get_slt_line
        elif header['data_spec'] == 'ddic':
            self.translate_method = self._get_ddic_line
