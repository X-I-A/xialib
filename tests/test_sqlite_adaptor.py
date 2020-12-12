import os
import json
import sqlite3
import pytest
from xialib import SQLiteAdaptor

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    field_data = json.load(fp)

table_id = "...simple_person"
log_table_id = "...XIA_simple_person"
new_table_id = "...simple_person_2"
sql_count = "SELECT COUNT(*) FROM simple_person"
sql_ctrl_select = "SELECT * FROM X_I_A_C_T_R_L"
sql_raw_count = "SELECT COUNT(*) FROM XIA_simple_person WHERE _AGE > 500"
sql_upd_count = "SELECT COUNT(*) FROM simple_person WHERE city = 'Paris'"

@pytest.fixture(scope='module')
def adaptor():
    conn = sqlite3.connect(':memory:')
    adaptor = SQLiteAdaptor(connection=conn)
    adaptor.create_table(SQLiteAdaptor._ctrl_table_id, '', dict(), SQLiteAdaptor._ctrl_table)
    yield adaptor

def test_simple_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 1, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data)
    assert adaptor.upsert_data(table_id, field_data, data_02)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (1000,)
    assert adaptor.rename_table(table_id, new_table_id)
    with pytest.raises(sqlite3.OperationalError):
        c.execute(sql_count)
    assert adaptor.rename_table(table_id, table_id)
    assert adaptor.upsert_data(table_id, field_data, delete_list)
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    assert adaptor.upsert_data(table_id, field_data, update_list)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.alter_column(table_id, dict())
    adaptor.set_ctrl_info(table_id, start_seq='20200101000000000000')
    line = adaptor.get_ctrl_info(table_id)
    assert line['START_SEQ'] == '20200101000000000000'
    adaptor.drop_table(table_id)
    with pytest.raises(sqlite3.OperationalError):
        c.execute(sql_count)

def test_raw_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    data_03 = list()
    for item in data_02:
        item['_AGE'] = item['id']
        data_03.append(item)
    c = adaptor.connection.cursor()
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data)
    assert adaptor.insert_raw_data(log_table_id, field_data, data_03)
    c.execute(sql_raw_count)
    assert c.fetchone() == (500,)
    assert adaptor.drop_table(table_id)

def test_raw_merge_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 2, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 3, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data)
    assert adaptor.upsert_data(table_id, field_data, data_02)
    assert adaptor.insert_raw_data(log_table_id, field_data, delete_list + update_list)
    assert adaptor.load_log_data(table_id, 2, 3)
    assert adaptor.insert_raw_data(log_table_id, field_data, delete_list + update_list)
    assert adaptor.load_log_data(table_id, None, 3)
    assert adaptor.insert_raw_data(log_table_id, field_data, delete_list + update_list)
    assert adaptor.load_log_data(table_id, 3, None)
    assert adaptor.load_log_data(table_id)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.drop_table(table_id)

def test_extrem_case_01(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    update_list_2 = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id,'20200101000000000000', {}, field_data)
    assert adaptor.upsert_data(table_id, field_data, data_02 + update_list + update_list_2)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (1000,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.drop_table(table_id)

def test_combo_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 1, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data)
    assert adaptor.upsert_data(table_id, field_data, data_02 + delete_list + update_list)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.drop_table(table_id)

def test_combo_replay_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 1, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id,'20200101000000000000', {}, field_data)
    assert adaptor.upsert_data(table_id, field_data, data_02 + delete_list + update_list, True)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.drop_table(table_id)

def test_exceptions():
    with pytest.raises(TypeError):
        adap = SQLiteAdaptor(connection=object())