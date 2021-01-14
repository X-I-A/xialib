import json
from collections import Counter
from functools import reduce
from typing import List, Dict
from xialib.translator import Translator

class BasicTranslator(Translator):
    """
    Supported data specification in the case of no `data_spec` specified or type ``x-i-a``

    Notes:
        If the 'data_spec' is not specified, the body data should be the real data to
        help to guess the data type
    """
    spec_list = ['x-i-a']


    def _get_origin_line(self, line: dict, **kwargs) -> dict:
        return line

    def _get_aged_line(self, line: dict, **kwargs) -> dict:
        line['_AGE'] = int(kwargs['age'])
        return line

    def _get_normal_line(self, line: dict, **kwargs) -> dict:
        line['_SEQ'] = kwargs['start_seq']
        return line

    def record_to_list(self, record_data: List[dict]) -> Dict[str, list]:
        field_list = reduce(lambda a, b: set(a) | set(b), record_data)
        return {k: [x.get(k, None) for x in record_data] for k in field_list}

    def generate_table_header(self, header: dict, body_data: List[dict]) -> List[dict]:
        guessed_header = list()
        if not body_data:
            self.logger.error("Cannot guess header with empty data")
            raise ValueError("XIA-000022")
        list_data = self.record_to_list(body_data)
        for field_name, field_data in list_data.items():
            field_description = {'field_name': field_name, 'key_flag': True, 'format': None, 'encode': None,
                                 'description': ''}
            field_type = set(Counter([type(item) for item in field_data]))
            if field_type.issubset({type(None)}):
                field_description['type_chain'] = ['char']
            elif field_type.issubset({type(None), int}):
                field_description['type_chain'] = ['int']
            elif field_type.issubset({type(None), int, float}):
                field_description['type_chain'] = ['real']
            elif field_type.issubset({type(None), int, float, str}):
                field_description['type_chain'] = ['char']
                fl = list(Counter([len(str(item)) for item in field_data if item is not None and len(str(item)) > 0]))
                if fl:
                    guessed_len = 2 * max(fl) - min(fl)
                    field_description['type_chain'].append('c_' + str(guessed_len))
            elif field_type.issubset({type(None), int, float, str, bytes}):
                field_description['type_chain'] = ['blob']
            else:
                try:
                    jsonify = [json.dumps(item) for item in field_data]
                    field_description['type_chain'] = ['json']
                except Exception as e:
                    self.logger.error("Guessing unsupported data")
                    raise ValueError("XIA-000023")
            guessed_header.append(field_description)
        return guessed_header

    def compile(self, header: dict, data: List[dict]):
        if header.get('data_spec', '') == 'x-i-a':
            self.translate_method = self._get_origin_line
        elif int(header.get('age', 0)) == 1:
            # Guess Header in the case of empty header
            guessed_header = self.generate_table_header(header, data)
            for i in range(len(data)):
                data.pop()
            for line in guessed_header:
                data.append(line)
            self.translate_method = self._get_origin_line
        elif 'age' in header:
            self.translate_method = self._get_aged_line
        else:
            self.translate_method = self._get_normal_line
