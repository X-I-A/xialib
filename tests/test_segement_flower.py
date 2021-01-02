import os
import json
import pytest
from xialib import SegmentFlower

config_1 = None
config_4 = {'id': '4', 'field_name': 'height', 'list': [151, 152, 153, 154, 155, 156], 'type_chain': ['int']}
config_5 = {'id': '5', 'field_name': 'height', 'min': 155, 'max': 159, 'type_chain': ['int']}
config_6 = {'id': '6', 'field_name': 'height', 'min': 150, 'max': 159, 'type_chain': ['int']}
config_7 = {'id': '7', 'field_name': 'extra', 'default': 'p1', 'type_chain': ['char', 'c_2']}
config_8 = {'id': '8', 'field_name': 'height', 'list': [156, 157], 'type_chain': ['int']}
config_9 = {'id': '9', 'field_name': 'height', 'min': 170, 'max': 190, 'type_chain': ['int']}
config_10 = {'id': '10', 'field_name': 'height', 'min': 152, 'max':160, 'type_chain': ['int']}
config_11 = {'id': '11', 'field_name': 'height', 'min': 160, 'max': 170, 'type_chain': ['int']}
ko_config_12 = {'id': '12', 'field_name': 'height', 'default': 149, 'min': 150, 'max': 200, 'type_chain': ['int']}
ko_config_15 = {'id': '15', 'field_name': 'height', 'min': 150, 'type_chain': ['int']}
ko_config_16 = {'id': '16', 'field_name': 'height', 'min': 150, 'max': 200, 'list': [151, 152, 153, 154, 155, 156], 'type_chain': ['int']}
ko_config_17 = {'id': '16', 'field_name': 'height', 'min': 150, 'max': 200, 'null': True, 'type_chain': ['int']}
ko_config_19 = {'id': '2', 'field_name': 'height', 'default': 153, 'min': 150, 'type_chain': ['int']}
config_20 = {'id': '20', 'field_name': 'height', 'min': 150, 'max': 200, 'type_chain': ['int']}
config_21 = {'id': '21', 'field_name': 'height', 'min': 161, 'max': 169, 'type_chain': ['int']}
config_22 = {'id': '22', 'field_name': 'height', 'default': 160, 'type_chain': ['int']}
config_23 = {'id': '23', 'field_name': 'height', 'null': True, 'type_chain': ['int']}
config_24 = {'id': '24', 'field_name': 'height', 'null': True, 'type_chain': ['int']}

with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
    body_data = json.loads(f.read().decode())
with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), 'rb') as f:
    header_data = json.loads(f.read().decode())

def test_flat_header_segment():
    data_header = {'age': 1}
    flr = SegmentFlower(config=config_1)
    header, body = flr.proceed(data_header, header_data)
    assert len(body) == 9

def test_flat_body_segment():
    data_header = {}
    flr = SegmentFlower(config=config_1)
    header, body = flr.proceed(data_header, body_data)
    assert len(body) == 1000

def test_fix_header_segment():
    data_header = {'age': 1}
    flr = SegmentFlower(config=config_1)
    flr.set_params(config=config_7)
    header, body = flr.proceed(data_header, header_data)
    assert len(body) == 10
    assert 'meta_data' not in data_header
    assert len(header_data) == 9

def test_fix_body_segment():
    data_header = {}
    flr = SegmentFlower(config=config_7)
    header, body = flr.proceed(data_header, body_data)
    for line in body:
        assert line['extra'] == 'p1'
    assert 'segment_id' in header
    assert 'segment_id' not in data_header
    assert len(body) == 1000
    assert 'list' not in flr.config

def test_list_body_segment():
    data_header = {}
    flr = SegmentFlower(config=config_4)
    header, body = flr.proceed(data_header, body_data)
    assert 'segment_id' in header
    assert 'segment_id' not in data_header
    assert len(body) == 108

def test_range_body_segment():
    data_header = {}
    flr = SegmentFlower(config=config_5)
    header, body = flr.proceed(data_header, body_data)
    assert 'segment_id' in header
    assert 'segment_id' not in data_header
    assert len(body) == 86

def test_compatiblity():
    flr = SegmentFlower()
    assert not flr.check_compatible(None)
    assert not flr.check_compatible(config_4)
    flr = SegmentFlower(config=config_4)
    assert not flr.check_compatible(config_4)
    assert not flr.check_compatible(config_6)
    assert not flr.check_compatible(config_7)
    assert not flr.check_compatible(config_8)
    assert flr.check_compatible(config_11)
    assert not flr.check_compatible(config_22)
    flr = SegmentFlower(config=config_11)
    assert flr.check_compatible(config_5)
    assert not flr.check_compatible(config_9)
    assert not flr.check_compatible(config_10)
    assert not flr.check_compatible(config_20)
    assert not flr.check_compatible(config_21)
    flr = SegmentFlower(config=config_10)
    assert not flr.check_compatible(config_4)
    flr = SegmentFlower(config=config_23)
    assert flr.check_compatible(config_21)
    assert not flr.check_compatible(config_24)

def test_exceptions():
    with pytest.raises(ValueError):
        flr = SegmentFlower(config=object())
    with pytest.raises(ValueError):
        flr = SegmentFlower(config=ko_config_12)
    with pytest.raises(ValueError):
        flr = SegmentFlower(config=ko_config_15)
    with pytest.raises(ValueError):
        flr = SegmentFlower(config=ko_config_16)
    with pytest.raises(ValueError):
        flr = SegmentFlower(config=ko_config_17)
    with pytest.raises(ValueError):
        flr = SegmentFlower(config=ko_config_19)
