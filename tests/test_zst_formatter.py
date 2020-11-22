import os
import pytest
from xialib import ZstFormatter

with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_complex.zst'), 'rb') as f:
    complex_zst = f.read()


@pytest.fixture(scope='module')
def formatter():
    formatter = ZstFormatter()
    yield formatter


def test_complex_zst(formatter):
    counter = 0
    for record in formatter.formatter(complex_zst, 'zst'):
        counter += 1
        for key, value in record.items():
            assert value is not None
        if int(record['id']) == 8 and counter < 1000:
            assert record['city'] == "Mörrum"
    assert counter == 5000


def test_io_zst(formatter):
    counter = 0
    with open(os.path.join('.', 'input', 'module_specific', 'formatter', 'person_complex.zst'), 'rb') as f:
        for record in formatter.formatter(f, 'zst'):
            counter += 1
            for key, value in record.items():
                assert value is not None
            if int(record['id']) == 8 and counter < 1000:
                assert record['city'] == "Mörrum"
    assert counter == 5000


def test_exceptions(formatter):
    with pytest.raises(TypeError):
        for record in formatter.formatter('a;b;c', 'csv'):
            break  # pragma: no cover
