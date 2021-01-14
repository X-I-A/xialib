import importlib
import datetime
import traceback
import logging
from functools import wraps
from typing import List

__all__ = ['Service', 'service_factory', 'backlog']


class Service():
    """Service is the combinaison of multiple modules.
    If there are multiple modules, they must be arranged by using dictionary

    """
    log_level = logging.WARNING
    MODULE_TYPE_LIST = ['adaptor', 'archiver', 'decoder', 'depositor', 'flower', 'formatter', 'publisher', 'subscriber',
                        'storer', 'translator']

    def __init__(self, **kwargs):
        self.logger = logging.getLogger("Service")
        self.log_context = {'context': ''}
        self.logger.setLevel(self.log_level)

        for module_type in self.MODULE_TYPE_LIST:
            if module_type in kwargs:
                module_object = kwargs[module_type]
                module_lib = importlib.import_module('xialib')
                module_class = getattr(module_lib, module_type.capitalize())
                if isinstance(module_object, dict):
                    if not all(isinstance(module_object, module_class) for key, module_object in module_object.items()):
                        self.logger.error("{} should match its type".format(module_type), extra=self.log_context)
                        raise TypeError("XIA-000033")
                else:
                    if not isinstance(module_object, module_class):
                        self.logger.error("{} should match its type".format(module_type), extra=self.log_context)
                        raise TypeError("XIA-000033")
                setattr(self, module_type, module_object)


    @classmethod
    def trigger_backlog(cls, header: dict, error_body: List[dict]):
        pass

def service_factory(service_config, global_dict=None, secret_manager=None):
    """ Object Factory: Create service by using configuration json

    Args:
        service_config (:obj:`str`): json structure
        global_dict (:obj:`str`): objects which should be configured as global object
        secrect_manager (:obj:`callable`): Any callable object to get the value by using key

    Notes:
        The secrets, such as API key, password, should have the format `{{key}}`. secret_manager will get its value

    """
    if global_dict is None:
        global_dict = {}
    if isinstance(service_config, dict):
        if service_config.get('_type', '') == 'object':
            module_name = importlib.import_module(service_config.pop('_module'))
            class_type = getattr(module_name, service_config.pop('_class'))
            service_config.pop('_type')
            return class_type(**service_factory(service_config, global_dict, secret_manager))
        elif service_config.get('_type', '') == 'global':
            return global_dict.get(service_config['_name'], None)
        else:
            return {key: service_factory(value, global_dict, secret_manager) for key, value in service_config.items()}
    elif isinstance(service_config, str) and \
            service_config.strip().startswith('${{') and \
            service_config.strip().endswith('}}') and \
            callable(secret_manager):
        return secret_manager(service_config.strip()[3:-2].strip())
    else:
        return service_config

def backlog(func):
    """Send all errors to backlog

    """
    @wraps(func)
    def wrapper(a, *args, **kwargs):
        try:
            return func(a, *args, **kwargs)
        except Exception as e:
            start_seq = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
            exception_msg = format(e)
            header = {'topic_id': 'insight',
                      'table_id': 'GENERAL',
                      'data_encode': 'gzip',
                      'data_format': 'record',
                      'data_spec': 'x-i-a',
                      'data_store': 'body',
                      'start_seq': start_seq}
            body = [{'_SEQ': start_seq,
                     'action_type': a.__class__.__name__,
                     'function': func.__name__,
                     'exception_type': e.__class__.__name__,
                     'exception_msg': exception_msg,
                     'args': args,
                     'kwargs': kwargs,
                     'trace': traceback.format_exc()}]
            if format(e)[:3] in ['XIA', 'INS', 'XED', 'AGT']:
                header['table_id'] = exception_msg
            Service.trigger_backlog(header, body)
            return True
    return wrapper