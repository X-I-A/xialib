import os
import json
import pytest
from xialib import BasicTranslator

with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
    data_body = json.loads(f.read().decode())
    age_header = {'age': '2', 'data': data_body}
    normal_header = {'start_seq': '2020111119150000000', 'data': data_body}
    xia_header = {'data_spec': 'x-i-a', 'data': data_body}

with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
    data_header = json.loads(f.read().decode())
    header = {'age': '1', 'data': data_header}

@pytest.fixture()
def translator():
    translator = BasicTranslator()
    yield translator

def test_simple_header(translator):
    translator.init_translator(header, header['data'])
    for line in header['data']:
        result_line = translator.get_translated_line(line)
        assert result_line == line

def test_xia_body(translator):
    translator.init_translator(xia_header, xia_header['data'])
    for line in xia_header['data']:
        result_line = translator.get_translated_line(line)
        assert result_line == line

def test_aged_body(translator):
    translator.init_translator(age_header, age_header['data'])
    for line in age_header['data']:
        result_line = translator.get_translated_line(line, age=age_header['age'])
        assert result_line['_AGE'] == 2

def test_normal_body(translator):
    translator.init_translator(normal_header, normal_header['data'])
    for line in normal_header['data']:
        result_line = translator.get_translated_line(line, start_seq=normal_header['start_seq'])
        assert result_line['_SEQ'] == '2020111119150000000'

def test_exceptions(translator):
    with pytest.raises(NotImplementedError):
        for line in header['data']:
            result_line = translator.get_translated_line(line)
            assert result_line == line