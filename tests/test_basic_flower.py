import os
import json
import pytest
from xialib import BasicFlower

filters1 = [[['gender', '=', 'Male'], ['height', '>=', 175]],
            [['gender', '=', 'Female'], ['weight', '<=', 100]]]
filters2 = [[['gender', '=', 'Male'], ['height', '>', 175]],
            [['gender', '!=', 'Male'], ['weight', '<', 100]]]

field_list = ['id', 'first_name', 'last_name', 'height', 'gender']

with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
    data_body = json.loads(f.read().decode())
with open(os.path.join('.', 'input', 'person_complex', '000003.json'), 'rb') as f:
    data_body.extend(json.loads(f.read().decode()))

@pytest.fixture(scope='module')
def flower():
    flower = BasicFlower()
    flower.set_params(field_list=field_list, filters=filters1)
    yield flower

def test_filter_operations():
    fields = ['id', 'first_name', 'last_name', 'height', 'children', 'lucky_numbers']
    result1 = BasicFlower.filter_table(data_body, fields, filters1)
    assert len(result1) == 999
    result2 = BasicFlower.filter_table(data_body, None, filters1)
    assert len(result2) == 999
    assert set([r['id'] for r in result1]) == set([r['id'] for r in result2])
    result3 = BasicFlower.filter_table(data_body, None, filters2)
    assert len(result3) == 980
    result4 = BasicFlower.filter_table(data_body, fields, None)
    assert max([len(line) for line in result4]) == 6
    result5 = BasicFlower.filter_table(data_body, None, None)
    assert max([len(line) for line in result5]) == 12
    all_need_fields = BasicFlower.get_minimum_fields(fields, filters1)
    assert len(all_need_fields) == 12

def test_header_flower(flower: BasicFlower):
    data_header = {'age': 1}
    with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
    flat_flower = BasicFlower()
    header, body = flat_flower.proceed(data_header, data_body)
    assert len(body) == 9
    header, body = flower.proceed(data_header, data_body)
    assert len(body) == 5

def test_body_flower(flower: BasicFlower):
    data_header = {'age': 2}
    with open(os.path.join('.', 'input', 'person_simple', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
    header, body = flower.proceed(data_header, data_body)
    assert len(body) == 518

def test_exceptions():
    with pytest.raises(ValueError):
        fl = BasicFlower(field_list=object())
    with pytest.raises(ValueError):
        fl = BasicFlower(filters=object())
    with pytest.raises(ValueError):
        fl = BasicFlower(filters=[object()])
    with pytest.raises(ValueError):
        fl = BasicFlower(filters=[[[]]])
