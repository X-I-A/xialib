import os
import json
import pytest
from xialib import SapTranslator

slt_init_load = [{'_RECNO': 1000, 'MANDT': '100', 'BUKRS': '1001'}]
slt_transfer = [{'_RECNO': 1000, 'MANDT': '100', 'BUKRS': '1001', 'IUUT_OPERAT_FLAG': 'U'}]
slt_init_header = {'age': '2', 'data_spec': 'slt', 'data': slt_init_load}
slt_transfer_header = {'age': '2', 'data_spec': 'slt', 'data': slt_transfer}
ddic_header = {'age': '1', 'data_spec': 'ddic', 'data': slt_init_load}
ddic_data = [{'FIELDNAME': 'F1', 'INTTYPE': 'C', 'LENG': 4, 'FIELDTEXT': 'Field 1'},
             {'FIELDNAME': 'F2', 'INTTYPE': 'N', 'LENG': 4, 'FIELDTEXT': 'Field 2'},
             {'FIELDNAME': 'F3', 'INTTYPE': 'P', 'LENG': 15, 'DECIMALS': 4, 'FIELDTEXT': 'Field 3'},
             {'FIELDNAME': 'F4', 'INTTYPE': 'D', 'FIELDTEXT': 'Field 4'},
             {'FIELDNAME': 'F5', 'INTTYPE': 'T', 'FIELDTEXT': 'Field 5'}, ]

@pytest.fixture()
def translator():
    translator = SapTranslator()
    yield translator

def test_init_load_header(translator):
    translator.compile(slt_init_header, slt_init_header['data'])
    for line in slt_init_header['data']:
        result_line = translator.get_translated_line(line, age=2)
        assert result_line['_AGE'] == 2
        assert '_RECNO' not in result_line

def test_transfer_header(translator):
    translator.compile(slt_transfer_header, slt_transfer_header['data'])
    for line in slt_transfer_header['data']:
        result_line = translator.get_translated_line(line, age=2)
        assert result_line['_AGE'] == 2
        assert '_NO' in result_line
        assert '_OP' in result_line

def test_ddic_header(translator):
    translator.compile(ddic_header, ddic_data)
    ddic_translated = [translator.get_translated_line(line) for line in ddic_data]
    assert ddic_translated[0].get('type_chain') == ['char', 'c_4']
    assert ddic_translated[1].get('type_chain') == ['char', 'c_4', 'n_4']
    assert ddic_translated[2].get('type_chain') == ['char', 'c_16', 'd_15_4']
    assert ddic_translated[3].get('type_chain') == ['char', 'c_8', 'date']
    assert ddic_translated[4].get('type_chain') == ['char', 'c_6', 'time']
