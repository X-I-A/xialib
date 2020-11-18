import os
import base64
import gzip
import time
import datetime
import json
import pytest
import random
from shutil import copy2
from xialib import FileDepositor
from xialib import BasicTranslator


@pytest.fixture(scope='module')
def depositor():
    depositor = FileDepositor(deposit_path=os.path.join('.', 'input', 'module_specific', 'depositor'))
    depositor.set_current_topic_table('test-001', 'person_complex')
    yield depositor
    os.rmdir(os.path.join(depositor.deposit_path, 'test-001', 'person_complex'))
    os.rmdir(os.path.join(depositor.deposit_path, 'test-001'))
    for file in os.listdir(os.path.join(depositor.deposit_path, 'test', 'aged_data')):
        os.remove(os.path.join(depositor.deposit_path, 'test', 'aged_data', file))
    for file in os.listdir(os.path.join(depositor.deposit_path, 'test', 'normal_data')):
        os.remove(os.path.join(depositor.deposit_path, 'test', 'normal_data', file))

def test_delete_documents(depositor):
    copy2(os.path.join(depositor.deposit_path, 'case_delete', '20201113222500000000.header'),
          os.path.join(depositor.deposit_path, 'test-001', 'person_complex', '20201113222500000000.header'))
    copy2(os.path.join(depositor.deposit_path, 'case_delete', '20201113222500000002-20201113222500000002.initial'),
          os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                       '20201113222500000002-20201113222500000002.initial'))
    copy2(os.path.join(depositor.deposit_path, 'case_delete', '20201113222500000003-20201113222500000003.initial'),
          os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                       '20201113222500000003-20201113222500000003.merged'))
    copy2(os.path.join(depositor.deposit_path, 'case_delete', '20201113222500000008-20201113222500000008.initial'),
          os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                       '20201113222500000008-20201113222500000008.packaged'))
    assert os.path.exists(os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                                       '20201113222500000002-20201113222500000002.initial'))
    depositor.delete_documents(['20201113222500000000.header',
                                '20201113222500000002-20201113222500000002',
                                '20201113222500000003-20201113222500000003',
                                '20201113222500000008-20201113222500000008.initial'])
    assert not os.path.exists(os.path.join(depositor.deposit_path,
                                           'test-001', 'person_complex', '20201113222500000000.header'))
    assert not os.path.exists(os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                              '20201113222500000002-20201113222500000002.initial'))
    assert not os.path.exists(os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                              '20201113222500000003-20201113222500000003.merged'))
    assert not os.path.exists(os.path.join(depositor.deposit_path, 'test-001', 'person_complex',
                              '20201113222500000008-20201113222500000008.packaged'))


def test_add_aged_document(depositor):
    translator = BasicTranslator()
    depositor.size_limit = 4096
    depositor.set_current_topic_table('test', 'aged_data')

    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'aged_data', 'aged': 'True',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    depositor.add_document(header, field_data)

    copy2(os.path.join(depositor.deposit_path, 'library', 'aged_data',
                       '20201113222500000014-20201113222500000014.initial'),
          os.path.join(depositor.deposit_path, 'test', 'aged_data',
                       '20201113222500000014-20201113222500000014.merged'))

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test', 'age': 2, 'table_id': 'aged_data', 'start_seq': '20201113222500000000'}

    start_age, current_age, test_data = 2, 2, list()
    translator.compile(age_header, test_data)
    for line in data_body:
        current_age = int(line['id']) + 1
        line['_NO'] = line['id']
        test_data.append(translator.get_translated_line(line, age=(int(line['id']) + 1)))
        if (int(line['id']) + line.get('weight', 0) + line.get('height', 0)) % 8 == 0:
            if current_age != age_header['age']:
                age_header['end_age'] = current_age
            else:
                age_header.pop('end_age', None)
            depositor.add_document(age_header, test_data)
            age_header['age'] = current_age + 1
            test_data = list()

    counter = 0
    for doc in depositor.get_stream_by_sort_key(status_list=['initial'],
                                                le_ge_key='20201113222500000267',
                                                reverse=True):
        doc_dict = depositor.get_header_from_ref(doc)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += len(doc_data)
    assert counter == 266

    copy2(os.path.join(depositor.deposit_path, 'library', 'overlap',
                       '20201113222500000133-20201113222500000133.initial'),
          os.path.join(depositor.deposit_path, 'test', 'aged_data',
                       '20201113222500000133-20201113222500000133.initial'))

def test_add_normal_document(depositor):
    translator = BasicTranslator()
    depositor.size_limit = 4096
    depositor.set_current_topic_table('test', 'normal_data')

    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test', 'table_id': 'normal_data', 'aged': 'False',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    depositor.add_document(header, field_data)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        normal_header = {'topic_id': 'test', 'table_id': 'normal_data'}

    test_data = list()
    translator.compile(normal_header, test_data)
    start_seq = '20201113222500000100'
    for line in data_body:
        line['_NO'] = line['id']
        test_data.append(translator.get_translated_line(line, start_seq=start_seq))
        if (int(line['id']) + line.get('weight', 0) + line.get('height', 0)) % 8 == 0:
            start_seq = str(int(start_seq) + int(line['id']) + line.get('weight', 0) + line.get('height', 0))
            normal_header['start_seq'] = start_seq
            depositor.add_document(normal_header, test_data)
            test_data = list()

    counter = 0
    doc = depositor.get_ref_by_merge_key('20201113222500065364')
    doc_dict = depositor.get_header_from_ref(doc)
    for doc in depositor.get_stream_by_sort_key(status_list=['initial'],
                                                le_ge_key=doc_dict['sort_key'],
                                                reverse=True):
        doc_dict = depositor.get_header_from_ref(doc)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += len(doc_data)
    assert counter == 864

def test_merge_aged_simple(depositor):
    depositor.set_current_topic_table('test', 'aged_data')
    depositor.size_limit = 5000
    assert not depositor.merge_documents('20201113222500000267', 2)
    assert depositor.merge_documents('20201113222500000267', 1)
    assert depositor.merge_documents('20201113222500000156', 1)
    assert not depositor.merge_documents('20201113222500000267', 2)
    depositor.size_limit = 2 ** 20
    assert depositor.merge_documents('20201113222500000221', 1)
    assert depositor.merge_documents('20201113222500000267', 2)
    assert not depositor.merge_documents('20201113222500000267', 1)
    counter, total_size = 0, 0
    for doc in depositor.get_stream_by_sort_key(status_list=['merged']):
        doc_dict = depositor.get_header_from_ref(doc)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += len(doc_data)
        total_size += doc_dict['data_size']
    header_ref = depositor.get_table_header()
    header_dict = depositor.get_header_from_ref(header_ref)
    assert counter == 266
    assert total_size == header_dict['merged_size']

def test_merge_normal_simple(depositor):
    depositor.set_current_topic_table('test', 'normal_data')
    depositor.size_limit = 2 ** 20
    assert depositor.merge_documents('20201113222500002444', 1)
    depositor.size_limit = 4096
    assert depositor.merge_documents('20201113222500006068', 1)
    assert not depositor.merge_documents('20201113222500065364', 2)
    assert depositor.merge_documents('20201113222500065364', 1)
    assert not depositor.merge_documents('20201113222500065364', 2)
    for doc in depositor.get_stream_by_sort_key(status_list=['initial'], min_merge_level=1):
        doc_dict = depositor.get_header_from_ref(doc)
        depositor.merge_documents(doc_dict['merge_key'], 1)
    counter, total_size = 0, 0
    for mlvl in range(2, 8):
        assert depositor.merge_documents('20201113222500000100', mlvl)
    for doc in depositor.get_stream_by_sort_key(status_list=['merged']):
        doc_dict = depositor.get_header_from_ref(doc)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += len(doc_data)
        total_size += doc_dict['data_size']
    assert counter == 882
    header_ref = depositor.get_table_header()
    header_dict = depositor.get_header_from_ref(header_ref)
    assert total_size == header_dict['merged_size']

def test_document_search(depositor):
    depositor.set_current_topic_table('test', 'aged_data')
    for doc in depositor.get_stream_by_sort_key():
        assert doc == '20201113222500000000.header'
        break
    for doc in depositor.get_stream_by_sort_key(reverse=True):
        assert doc == '20201113222500001000-20201113222500001000.initial'
        break
    for doc in depositor.get_stream_by_sort_key(reverse=True, equal=False):
        assert doc == '20201113222500001000-20201113222500001000.initial'
        break
    for doc in depositor.get_stream_by_sort_key(le_ge_key='20201113222500000999', reverse=True):
        assert doc == '20201113222500000993-20201113222500000993.initial'
        break
    for doc in depositor.get_stream_by_sort_key(le_ge_key='20201113222500000999'):
        assert doc == '20201113222500001000-20201113222500001000.initial'
        break
    for doc in depositor.get_stream_by_sort_key(status_list=['merged'], le_ge_key='20201113222500000999', reverse=True):
        assert doc == '20201113222500000267-20201113222500000267.merged'
        break
    for doc in depositor.get_stream_by_sort_key(le_ge_key='20201113222500000993', equal=False):
        assert doc == '20201113222500001000-20201113222500001000.initial'
        break

def test_update_document(depositor):
    depositor.set_current_topic_table('test', 'aged_data')
    doc_ref = depositor.get_ref_by_merge_key('20201113222500001000')
    assert doc_ref is not None
    depositor.update_document(doc_ref, {'hello': 'world'})
    depositor.update_document(doc_ref, {'hello': depositor.DELETE})
    depositor.update_document(doc_ref, {'merge_status': 'packaged'})
    header_ref = depositor.get_table_header()
    header_dict = depositor.get_header_from_ref(header_ref)
    header_data = depositor.get_data_from_header(header_dict)
    depositor.update_document(header_ref, header_dict, header_data)
    depositor.size_limit = 16
    with pytest.raises(ValueError):
        depositor.size_limit = 16
        depositor.update_document(header_ref, header_dict, header_data)
    depositor.size_limit = 4096

def test_error_1(depositor):
    depositor.set_current_topic_table('test', 'error_1')
    assert not depositor.get_table_header()
    assert not depositor.merge_documents('20201113222500000156', 1)
    assert not depositor.merge_documents('20201113222500000268', 2)
    assert not depositor.merge_documents('20201113222600000267', 1)

def test_error_2(depositor):
    depositor.set_current_topic_table('test', 'error_2')
    assert not depositor.merge_documents('20201113222500000156', 1)
    assert not depositor.merge_documents('20201113222500000221', 1)
    assert not depositor.merge_documents('20201113222500000267', 2)

def test_error_3(depositor):
    depositor.set_current_topic_table('test', 'error_3')
    assert not depositor.get_table_header()
    assert not depositor.merge_documents('20201113222500000780', 1)

def test_exceptions(depositor):
    with pytest.raises(ValueError):
        d = FileDepositor(deposit_path='wrong_path')

    with open(os.path.join(depositor.deposit_path, 'library', 'aged_data',
                           '20201113222500000014-20201113222500000014.initial'), 'rb') as f:
        header = json.loads(f.read().decode())
        data = json.loads(gzip.decompress(base64.b64decode(header.pop('data').encode())).decode())
        depositor.set_current_topic_table('test', 'aged_data')
        depositor.size_limit = 1024
        header.pop('end_age')
        with pytest.raises(ValueError):
            for chunk_data in depositor._get_aged_data_chunk(header, data):
                pass
        depositor.size_limit = 2048
        with pytest.raises(ValueError):
            for chunk_data in depositor._get_aged_data_chunk(header, data):
                pass