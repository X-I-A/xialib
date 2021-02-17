import os
import json
import pytest
from xialib import Service, service_factory, backlog, secret_composer
from xialib import BasicStorer

def get_secret(key: str):
    return 'Hello World'

def test_service_factory():
    test_file_path = os.path.join('.', 'input', 'module_specific', 'service')
    with open(os.path.join(test_file_path, 'global_connectors.json'), 'r', encoding='utf-8') as fp:
        global_config = json.load(fp)
        global_dict = service_factory(global_config, None, get_secret)

    with open(os.path.join(test_file_path, 'object_config.json'), 'r', encoding='utf-8') as fp:
        service_config = json.load(fp)
        service_object = service_factory(service_config, global_dict, get_secret)
        assert isinstance(service_object, Service)
        assert isinstance(global_dict.get('basicstorer'), BasicStorer)
        assert global_dict.get('basicstorer') == service_object.archiver.storer

def test_secret_composer():
    assert secret_composer("${{pwd}}") == "${{pwd}}"
    assert secret_composer("${{pwd}}", get_secret) == "Hello World"
    assert secret_composer("${{ pwd }}", get_secret) == "Hello World"
    assert secret_composer("${{pwd}}1", get_secret) == "Hello World1"
    assert secret_composer("1${{pwd}}1", get_secret) == "1Hello World1"
    assert secret_composer("1${{pwd", get_secret) == "1${{pwd"
    assert secret_composer("pwd}}1", get_secret) == "pwd}}1"
    assert secret_composer("1${{pwd}}${{pwd}}1", get_secret) == "1Hello WorldHello World1"
    assert secret_composer("1${{${{pwd}}}}1", get_secret) == "1${{Hello World}}1"

def test_backlog():
    srv = Service()
    @backlog
    def xia_error(srv):
        wrong_ins = Service(depositor=object())
    xia_error(srv)

def test_exceptions():
    with pytest.raises(TypeError):
        wrong_ins = Service(depositor=object())
    with pytest.raises(TypeError):
        wrong_ins = Service(publisher={'dummy': object()})