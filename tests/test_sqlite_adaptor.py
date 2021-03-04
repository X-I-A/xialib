import os
import json
import sqlite3
import pytest
from xialib import SQLiteAdaptor, SegmentFlower

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
    #conn = sqlite3.connect(':memory:')
    conn = sqlite3.connect(os.path.join('.', 'test.sqlite'))
    adaptor = SQLiteAdaptor(db=conn)
    adaptor.drop_table(SQLiteAdaptor._ctrl_table_id)
    adaptor.drop_table(SQLiteAdaptor._ctrl_log_id)
    adaptor.create_table(SQLiteAdaptor._ctrl_table_id, '', dict(), SQLiteAdaptor._ctrl_table, False, "")
    adaptor.create_table(SQLiteAdaptor._ctrl_log_id, '', dict(), SQLiteAdaptor._ctrl_log_table, False, "")
    adaptor.drop_table(table_id)
    yield adaptor

def test_simple_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 1, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.drop_table(table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data, False, None)
    assert adaptor.upsert_data(table_id, field_data, data_02)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (1000,)
    assert adaptor.upsert_data(table_id, field_data, delete_list)
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    assert adaptor.upsert_data(table_id, field_data, update_list)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.alter_column(table_id, {'type_chain': ['char', 'c_8']}, {'type_chain': ['char', 'c_9']})
    adaptor.set_ctrl_info(table_id, '', start_seq='20200101000000000000')
    line = adaptor.get_ctrl_info(table_id)
    assert line['START_SEQ'] == '20200101000000000000'
    adaptor.purge_table(table_id)

def test_log_operation(adaptor):
    log_data = [{'TABLE_ID': '...person_simple', 'SEGMENT_ID': '', 'START_AGE': 2, 'END_AGE': 3, 'LOADED_FLAG': ''},
                {'TABLE_ID': '...person_simple', 'SEGMENT_ID': '', 'START_AGE': 4, 'END_AGE': 5, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data)
    results = adaptor.get_log_info('...person_simple')
    assert len(results) == 2

def test_raw_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    data_03 = list()
    for item in data_02:
        item['_AGE'] = item['id']
        data_03.append(item)
    c = adaptor.connection.cursor()
    assert adaptor.drop_table(table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data, False, None)
    assert adaptor.insert_raw_data(log_table_id, field_data, data_03)
    c.execute(sql_raw_count)
    assert c.fetchone() == (500,)
    assert adaptor.drop_table(table_id)

def test_log_load_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 2, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 3, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data, False, None)
    assert adaptor.upsert_data(table_id, field_data, data_02)
    assert adaptor.insert_raw_data(log_table_id, field_data, delete_list + update_list)
    assert adaptor.load_log_data(table_id, 2, 3)
    log_data = [{'TABLE_ID': '...person_simple', 'SEGMENT_ID': '', 'START_AGE': 2, 'END_AGE': 3, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data)
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
    assert adaptor.create_table(table_id,'20200101000000000000', {}, field_data, False, None)
    assert adaptor.upsert_data(table_id, field_data, data_02 + update_list + update_list_2)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (1000,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.purge_table(table_id)

def test_combo_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 1, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data, False, None)
    assert adaptor.upsert_data(table_id, field_data, data_02 + delete_list + update_list)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.purge_table(table_id)

def test_combo_replay_operation(adaptor):
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    delete_list = [{"_AGE": 1, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "_OP": 'D'}]
    update_list = [{"_AGE": 2, "id": 2, "first_name": "Rodge", "last_name": "Fratczak", "city": "Paris", "_OP": 'U'}]
    assert adaptor.create_table(table_id,'20200101000000000000', {}, field_data, False, None)
    assert adaptor.upsert_data(table_id, field_data, data_02 + delete_list + update_list)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (999,)
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.purge_table(table_id)

def test_range_segment(adaptor: SQLiteAdaptor):
    assert adaptor.drop_table(table_id)
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    upd_0_list = [{"_AGE": 2, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "city": "Paris", "_OP": 'U'}]
    upd_1_list = [{"_AGE": 2, "id": 3, "first_name": "Clevie", "last_name": "Jacquemet", "city": "Paris", "height": 151, "_OP": 'U'}]
    upd_2_list = [{"_AGE": 2, "id": 6, "first_name": "Hamilton", "last_name": "Hirtz", "city": "Paris", "height": 161, "_OP": 'U'}]
    segment_0 = {'id': '0', 'field_name': 'height', 'type_chain': ['int'], 'null': True}
    flower_0 = SegmentFlower(config=segment_0)
    segment_1 = {'id': '1', 'field_name': 'height', 'type_chain': ['int'], 'min': 150, 'max': 159}
    flower_1 = SegmentFlower(config=segment_1)
    segment_2 = {'id': '2', 'field_name': 'height', 'type_chain': ['int'], 'min': 160, 'max': 169}
    flower_2 = SegmentFlower(config=segment_2)

    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_0}, field_data, False, table_id)
    assert adaptor.create_table(table_id, '20190101000000000000', {'segment': segment_1}, field_data, False, table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_1}, field_data, False, table_id)
    assert adaptor.create_table(table_id, '20190101000000000000', {'segment': segment_1}, field_data, False, table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_2}, field_data, False, table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_2}, field_data, False, table_id)
    list = adaptor.get_ctrl_info_list(table_id)
    header_0, data_seg_0 = flower_0.proceed({}, data_02)
    header_1, data_seg_1 = flower_1.proceed({}, data_02)
    header_2, data_seg_2 = flower_2.proceed({}, data_02)
    assert adaptor.upsert_data(table_id, field_data, data_seg_0)
    assert adaptor.upsert_data(table_id, field_data, data_seg_1)
    assert adaptor.upsert_data(table_id, field_data, data_seg_2)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (468,)
    assert adaptor.insert_raw_data(log_0_table_id, field_data, upd_0_list)
    log_data_0 = [{'TABLE_ID': table_id, 'SEGMENT_ID': '0', 'START_AGE': 2, 'END_AGE': 2, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data_0)
    assert adaptor.load_log_data(table_id, 2, 2, '0')
    assert adaptor.insert_raw_data(log_1_table_id, field_data, upd_1_list)
    log_data_1 = [{'TABLE_ID': table_id, 'SEGMENT_ID': '1', 'START_AGE': 2, 'END_AGE': 2, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data_1)
    assert adaptor.load_log_data(table_id, 2, 2, '1')
    assert adaptor.insert_raw_data(log_2_table_id, field_data, upd_2_list)
    log_data_2 = [{'TABLE_ID': table_id, 'SEGMENT_ID': '2', 'START_AGE': 2, 'END_AGE': 2, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data_2)
    assert adaptor.load_log_data(table_id, 2, 2, '2')
    c.execute(sql_upd_count)
    assert c.fetchone() == (3,)
    assert adaptor.purge_table(table_id, list[0]['META_DATA']['segment'])
    c.execute(sql_upd_count)
    assert c.fetchone() == (2,)
    assert adaptor.purge_table(table_id, list[1]['META_DATA']['segment'])
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.purge_table(table_id, list[2]['META_DATA']['segment'])
    c.execute(sql_upd_count)
    assert c.fetchone() == (0,)

def test_list_segment(adaptor: SQLiteAdaptor):
    assert adaptor.drop_table(table_id)
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
        data_02 = json.load(fp)
    upd_0_list = [{"_AGE": 2, "id": 1, "first_name": "Naomi", "last_name": "Gumbrell", "city": "Paris", "_OP": 'U'}]
    upd_1_list = [{"_AGE": 2, "id": 3, "first_name": "Clevie", "last_name": "Jacquemet", "city": "Paris", "height": 151, "_OP": 'U'}]
    upd_2_list = [{"_AGE": 2, "id": 6, "first_name": "Hamilton", "last_name": "Hirtz", "city": "Paris", "height": 161, "_OP": 'U'}]
    segment_0 = {'id': '0', 'field_name': 'height', 'type_chain': ['int'], 'null': True}
    flower_0 = SegmentFlower(config=segment_0)
    segment_1 = {'id': '1', 'field_name': 'height', 'type_chain': ['int'], 'list': [150, 151, 152, 153]}
    flower_1 = SegmentFlower(config=segment_1)
    segment_2 = {'id': '2', 'field_name': 'height', 'type_chain': ['int'], 'list': [160, 161, 162, 163]}
    flower_2 = SegmentFlower(config=segment_2)
    segment_ko = {'id': '3', 'field_name': 'weight', 'type_chain': ['int'], 'list': [88]}

    field_data_0 = [field for field in field_data if field['field_name'] in ['id', 'first_name', 'last_name', 'city']]
    field_id = field_data[0].copy()
    field_id['type_chain'] = ['int', 'i_8']
    field_data_1 = field_data.copy()
    field_data_1[0] = field_id.copy()
    field_id['type_chain'] = ['int']
    field_data_2 = field_data.copy()
    field_data_2[0] = field_id.copy()

    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_0}, field_data_0, False, table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_1}, field_data_1, False, table_id)
    field_data_1[0] = field_id.copy()
    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_1}, field_data_1, False, table_id)
    assert adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_2}, field_data_2, False, table_id)
    assert not adaptor.create_table(table_id, '20200101000000000000', {'segment': segment_ko}, field_data, False, table_id)
    list = adaptor.get_ctrl_info_list(table_id)
    header_0, data_seg_0 = flower_0.proceed({}, data_02)
    header_1, data_seg_1 = flower_1.proceed({}, data_02)
    header_2, data_seg_2 = flower_2.proceed({}, data_02)
    assert adaptor.upsert_data(table_id, field_data_0, data_seg_0)
    assert adaptor.upsert_data(table_id, field_data_1, data_seg_1)
    assert adaptor.upsert_data(table_id, field_data_2, data_seg_2)
    c = adaptor.connection.cursor()
    c.execute(sql_count)
    assert c.fetchone() == (265,)
    assert adaptor.insert_raw_data(log_0_table_id, field_data_0, upd_0_list)
    log_data_0 = [{'TABLE_ID': table_id, 'SEGMENT_ID': '0', 'START_AGE': 2, 'END_AGE': 2, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data_0)
    assert adaptor.load_log_data(table_id, 2, 2, '0')
    assert adaptor.insert_raw_data(log_1_table_id, field_data_1, upd_1_list)
    log_data_1 = [{'TABLE_ID': table_id, 'SEGMENT_ID': '1', 'START_AGE': 2, 'END_AGE': 2, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data_1)
    assert adaptor.load_log_data(table_id, 2, 2, '1')
    assert adaptor.insert_raw_data(log_2_table_id, field_data_2, upd_2_list)
    log_data_2 = [{'TABLE_ID': table_id, 'SEGMENT_ID': '2', 'START_AGE': 2, 'END_AGE': 2, 'LOADED_FLAG': ''}]
    assert adaptor.upsert_data(adaptor._ctrl_log_id, adaptor._ctrl_log_table, log_data_2)
    assert adaptor.load_log_data(table_id, 2, 2, '2')
    c.execute(sql_upd_count)
    assert c.fetchone() == (3,)
    assert adaptor.purge_table(table_id, list[0]['META_DATA']['segment'])
    c.execute(sql_upd_count)
    assert c.fetchone() == (2,)
    assert adaptor.purge_table(table_id, list[1]['META_DATA']['segment'])
    c.execute(sql_upd_count)
    assert c.fetchone() == (1,)
    assert adaptor.purge_table(table_id, list[2]['META_DATA']['segment'])
    c.execute(sql_upd_count)
    assert c.fetchone() == (0,)

def test_exceptions():
    with pytest.raises(TypeError):
        adap = SQLiteAdaptor(db=object())