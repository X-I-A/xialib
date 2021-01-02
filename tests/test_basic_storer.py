import os
import json
import pytest
from xialib import BasicStorer


file_path = os.path.join('.', 'input', 'person_complex', 'schema.json')
dest_file = os.path.join('.', 'input', 'module_specific', 'test.json')

@pytest.fixture(scope='module')
def storer():
    storer = BasicStorer()
    yield storer

def test_simple_flow(storer: BasicStorer):
    data_copy1 = storer.read(file_path)
    for data_io in storer.get_io_stream(file_path):
        new_file = storer.write(data_io, dest_file)
        assert new_file == dest_file
    for data_io in storer.get_io_stream(dest_file):
        data_copy2 = data_io.read()
        assert data_copy1 == data_copy2
    assert storer.remove(dest_file)
    assert not storer.remove(dest_file)
    storer.write(data_copy1, dest_file)
    for data_wb_io in storer.get_io_wb_stream(dest_file):
        data_wb_io.write(data_copy1)
    storer.remove(dest_file)
    assert not storer.exists(dest_file)
