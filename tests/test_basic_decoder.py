import os
import codecs
import json
import pytest
from xialib import BasicDecoder

content_dict = dict()
with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.gzip'), 'rb') as f:
    content_dict['gzip'] = f.read()
with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.b64g'), 'r') as f:
    content_dict['b64g'] = f.read()
with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
    content_dict['blob'] = f.read()
with codecs.open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), encoding='utf-8') as f:
    content_dict['flat'] = f.read()


@pytest.fixture(scope='module')
def decoder():
    decoder = BasicDecoder()
    yield decoder


def test_simple_flow(decoder):
    assert len(json.loads(content_dict['flat'])) == 1000
    assert content_dict['gzip'] != content_dict['b64g']
    assert content_dict['blob'] != content_dict['flat']
    # Matrix Test (gzip and b64 is not reversible)
    for from_encode in ['flat', 'blob', 'gzip', 'b64g']:
        for to_encode in ['flat', 'blob']:
            for content in decoder.decoder(content_dict[from_encode], from_encode, to_encode):
                assert content == content_dict[to_encode]


def test_reverse_flow(decoder):
    for from_encode in ['flat', 'blob']:
        for to_encode in ['flat', 'blob', 'gzip', 'b64g']:
            for c1 in decoder.decoder(content_dict[from_encode], from_encode, to_encode):
                for c2 in decoder.decoder(c1, to_encode, from_encode):
                    assert content_dict[from_encode] == c2


def test_gzip_b64g_flow(decoder):
    for c1 in decoder.decoder(content_dict['flat'], 'flat', 'gzip'):
        for c2 in decoder.decoder(c1, 'gzip', 'b64g'):
            for c3 in decoder.decoder(c2, 'b64g', 'gzip'):
                for c4 in decoder.decoder(c3, 'gzip', 'flat'):
                    assert content_dict['flat'] == c4

def test_io_terminate_flow(decoder):
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
        data = f.read()
        for content in decoder.decoder(data, 'blob', 'flat'):
            assert content == content_dict['flat']
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.b64g'), 'rb') as f:
        data = f.read()
        for content in decoder.decoder(data, 'b64g', 'flat'):
            assert content == content_dict['flat']

def test_io_flat_flow(decoder):
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.gzip'), 'rb') as f:
        for content in decoder.decoder(f, 'gzip', 'flat'):
            assert content == content_dict['flat']
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
        for content in decoder.decoder(f, 'blob', 'flat'):
            assert content == content_dict['flat']
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.b64g'), 'rb') as f:
        for content in decoder.decoder(f, 'b64g', 'flat'):
            assert content == content_dict['flat']


def test_io_blob_flow(decoder):
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.gzip'), 'rb') as f:
        for data_io in decoder.decoder(f, 'gzip', 'blob'):
            content = data_io.read()
            assert content == content_dict['blob']
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
        for data_io in decoder.decoder(f, 'blob', 'blob'):
            content = data_io.read()
            assert content == content_dict['blob']
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.flat'), 'rb') as f:
        for content in decoder.decoder(f, 'flat', 'blob'):
            assert content == content_dict['blob']
    with open(os.path.join('.', 'input', 'module_specific', 'decoder', 'person_simple.b64g'), 'rb') as f:
        for content in decoder.decoder(f, 'b64g', 'blob'):
            assert content == content_dict['blob']


def test_exceptions(decoder):
    with pytest.raises(ValueError):
        for conent in decoder.decoder(content_dict['flat'], 'gzip', 'zip'):
            break  # pragma: no cover
    with pytest.raises(TypeError):
        for conent in decoder.decoder(content_dict['flat'], 'zip', 'gzip'):
            break  # pragma: no cover
    with pytest.raises(TypeError):
        for conent in decoder.decoder(['KO'], 'blob', 'flat'):
            break  # pragma: no cover
