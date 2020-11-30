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
    field_data = data_header.pop('columns')
    header = {'age': '1', 'data': field_data, 'meta-data':data_header}

@pytest.fixture()
def translator():
    translator = BasicTranslator()
    yield translator

def test_simple_header(translator):
    translator.compile(header, header['data'])
    for line in header['data']:
        result_line = translator.get_translated_line(line)
        assert result_line == line

def test_xia_body(translator):
    translator.compile(xia_header, xia_header['data'])
    for line in xia_header['data']:
        result_line = translator.get_translated_line(line)
        assert result_line == line

def test_aged_body(translator):
    translator.compile(age_header, age_header['data'])
    for line in age_header['data']:
        result_line = translator.get_translated_line(line, age=age_header['age'])
        assert result_line['_AGE'] == 2
        result_line.pop('_AGE')

def test_normal_body(translator):
    translator.compile(normal_header, normal_header['data'])
    for line in normal_header['data']:
        result_line = translator.get_translated_line(line, start_seq=normal_header['start_seq'])
        assert result_line['_SEQ'] == '2020111119150000000'
        result_line.pop('_SEQ')

def test_type_transcode(translator):
    assert translator.get_type_chain('null') == ['null']
    assert translator.get_type_chain('blob') == ['blob']
    assert translator.get_type_chain('char') == ['char']
    assert translator.get_type_chain('c_1') == ['char', 'c_1']
    assert translator.get_type_chain('n_8') == ['char', 'c_8', 'n_8']
    assert translator.get_type_chain('d_13_3') == ['real', 'd_13_3']
    assert translator.get_type_chain('date', 'yyyymmdd') == ['char', 'c_8', 'date']
    assert translator.get_type_chain('time', 'hhmmss') == ['char', 'c_6', 'time']
    assert translator.get_type_chain('datetime', 'yyyymmddhhmmssffffff') == ['char', 'c_20', 'datetime']
    assert translator.get_type_chain('json') == ['char', 'json']
    assert translator.get_type_chain('int') == ['int']
    assert translator.get_type_chain('bool') == ['int', 'i_1', 'bool']
    assert translator.get_type_chain('i_1') == ['int', 'i_1']
    assert translator.get_type_chain('ui_1') == ['int', 'ui_1']
    assert translator.get_type_chain('real') == ['real']
    assert translator.get_type_chain('float') == ['real', 'float']
    assert translator.get_type_chain('double') == ['real', 'double']
    assert translator.get_type_chain('jd') == ['real', 'double', 'jd']

def test_exceptions(translator):
    with pytest.raises(NotImplementedError):
        for line in header['data']:
            result_line = translator.get_translated_line(line)
