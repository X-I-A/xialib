import os
import json
import pytest

from xialib import ZipDecoder


@pytest.fixture(scope='module')
def decoder():
    decoder = ZipDecoder()
    yield decoder


def test_simple_flow(decoder):
    total_file = 0
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.zip'), 'rb') as f:
        zipped_content = f.read()
    for source in decoder.decoder(zipped_content, 'zip', 'flat'):
        assert len(json.loads(source)) == 1000
        total_file += 1
    assert total_file == 2


def test_io_flat_flow(decoder):
    total_file = 0
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.zip'), 'rb') as f:
        for source in decoder.decoder(f, 'zip', 'flat'):
            assert len(json.loads(source)) == 1000
            total_file += 1
    assert total_file == 2


def test_io_io_flow(decoder):
    total_file = 0
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.zip'), 'rb') as f:
        for source in decoder.decoder(f, 'zip', 'blob'):
            source = source.read().decode()
            assert len(json.loads(source)) == 1000
            total_file += 1
    assert total_file == 2


def test_exceptions(decoder):
    with pytest.raises(TypeError):
        for conent in decoder.decoder('KO', 'zip', 'flat'):
            break  # pragma: no cover
