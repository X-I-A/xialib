import os
import json
import pytest
from xialib import BasicFormatter

with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_simple.list'), 'rb') as f:
    simple_list = f.read()
with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_complex.list'), 'rb') as f:
    complex_list = f.read()
with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
    simple_record = f.read()


@pytest.fixture(scope='module')
def formatter():
    formatter = BasicFormatter()
    yield formatter


def test_simple_record(formatter):
    counter = 0
    for record in formatter.formatter(simple_record, 'record'):
        counter += 1
        for key, value in record.items():
            assert value is not None
    assert counter == 1000


def test_simple_list(formatter):
    counter = 0
    for record in formatter.formatter(simple_list, 'list'):
        counter += 1
        for key, value in record.items():
            assert value is not None
    assert counter == 1000


def test_complex_list(formatter):
    counter = 0
    for record in formatter.formatter(complex_list, 'list'):
        counter += 1
        for key, value in record.items():
            assert value is not None
    assert counter == 1000


def test_io_record(formatter):
    counter = 0
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
        for record in formatter.formatter(f, 'record'):
            counter += 1
            for key, value in record.items():
                assert value is not None
    assert counter == 1000


def test_io_list(formatter):
    counter = 0
    with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_simple.list'), 'rb') as f:
        for record in formatter.formatter(f, 'list'):
            counter += 1
            for key, value in record.items():
                assert value is not None
    assert counter == 1000


def test_exceptions(formatter):
    bad_list = {'a': [1, 2], 'b': [3, 4, 5]}
    with pytest.raises(ValueError):
        for record in formatter.formatter(json.dumps(bad_list).encode(), 'list'):
            break  # pragma: no cover
    with pytest.raises(ValueError):
        for record in formatter.formatter(b'', 'list'):
            break  # pragma: no cover
    with pytest.raises(TypeError):
        for record in formatter.formatter(json.dumps(bad_list), 'list'):
            break  # pragma: no cover
    with pytest.raises(TypeError):
        for record in formatter.formatter(simple_list, 'csv'):
            break  # pragma: no cover
