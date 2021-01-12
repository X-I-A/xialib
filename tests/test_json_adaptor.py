import os
import json
import sqlite3
import pytest
from xialib import JsonAdaptor, BasicStorer

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    field_data = json.load(fp)

table_id = "...simple_person"
log_table_id = "...XIA_simple_person"
log_0_table_id = "...XIA_0_simple_person"
log_1_table_id = "...XIA_1_simple_person"
log_2_table_id = "...XIA_2_simple_person"
new_table_id = "...simple_person_2"
sql_count = "SELECT COUNT(*) FROM simple_person"
sql_ctrl_select = "SELECT * FROM X_I_A_C_T_R_L"
sql_raw_count = "SELECT COUNT(*) FROM XIA_simple_person WHERE _AGE > 500"
sql_upd_count = "SELECT COUNT(*) FROM simple_person WHERE city = 'Paris'"

@pytest.fixture(scope='module')
def adaptor():
    adaptor = JsonAdaptor(location=os.path.join('.', 'input', 'module_specific', 'adaptor'))
    yield adaptor

def test_simple_operation(adaptor: JsonAdaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    for line in data_02:
        line['_AGE'] = line['id'] + 1
    delete_list = [{"_AGE": 1002, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 1003, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    data_02.extend(delete_list)
    data_02.extend(update_list)
    assert adaptor.drop_table(table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data, False, None)
    assert adaptor.upsert_data(table_id, field_data, data_02)
