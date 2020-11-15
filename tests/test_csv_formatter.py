import os
import pytest
from xialib import CSVFormatter

with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_simple.csv'), 'rb') as f:
    simple_csv = f.read()


@pytest.fixture(scope='module')
def formatter():
    formatter = CSVFormatter()
    yield formatter


def test_simple_csv(formatter):
    counter = 0
    for record in formatter.formatter(simple_csv, 'csv'):
        counter += 1
        for key, value in record.items():
            assert value is not None
        if record['id'] == "65":
            assert record['city'] == "Novoukrainskiy"
    assert counter == 1000


def test_io_csv(formatter):
    counter = 0
    with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_simple.csv'), 'rb') as f:
        for record in formatter.formatter(f, 'csv'):
            counter += 1
            for key, value in record.items():
                assert value is not None
            if record['id'] == "65":
                assert record['city'] == "Novoukrainskiy"
    assert counter == 1000


def test_exceptions(formatter):
    with pytest.raises(TypeError):
        for record in formatter.formatter('a;b;c', 'csv'):
            break  # pragma: no cover
