import os
import json
import gzip
import base64
import pytest
from xialib import BasicPublisher
from xialib.exceptions import XIADataSpecError

data_1 = [{'_AGE': 2, 'MANDT': '100', 'BUKRS': '1001', 'NAME': 'Sébastien & Co'}]
gzdata_1 = gzip.compress(json.dumps(data_1, ensure_ascii=False).encode())
header_1 = {'age': 2, 'data_format': 'record', 'data_spec': 'x-i-a', 'data_encode': 'gzip'}
header_2 = {'age': 2, 'data_format': 'record', 'data_encode': 'gzip'}
header_3 = {'age': 2, 'data_format': 'record', 'data_spec': 'x-i-a', 'data_encode': 'gzip',
            'merged_data': [1, 2, 3, 4, 5],
            'encrypted_data': {'user': 'hello', 'password': 'world'}}

@pytest.fixture(scope='module')
def publisher():
    publisher = BasicPublisher()
    yield publisher

def test_simple_flow(publisher):
    filename = publisher.publish(os.path.join('.', 'input'), 'module_specific', header_1, gzdata_1)
    assert os.path.exists(filename)
    with open(filename, 'rb') as fp:
        check_data = json.loads(fp.read().decode())
        for line in json.loads(gzip.decompress(base64.b64decode(check_data['data'])).decode()):
            assert 'é' in line['NAME']
    os.remove(filename)

def test_complex_header(publisher):
    filename = publisher.publish(os.path.join('.', 'input'), 'module_specific', header_3, gzdata_1)
    assert os.path.exists(filename)
    with open(filename, 'rb') as fp:
        check_data = json.loads(fp.read().decode())
        assert isinstance(check_data['merged_data'], list)
        assert isinstance(check_data['encrypted_data'], dict)
    os.remove(filename)

def test_exceptions(publisher):
    with pytest.raises(XIADataSpecError):
        filename = publisher.publish(os.path.join('.', 'input'), 'module_specific', header_2, gzdata_1)
