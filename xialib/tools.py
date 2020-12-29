import json
import importlib


def get_object(**kwargs):
    """ Single Object Factory

    Create object by using predefined parameters

    Args:
        module (:obj:`str`): module name
        class (:obj:`str`): class name
        secrect_manager (:obj:`Callable`): Any callable object to get the value by using key

    Notes:
        The secrets, such as API key, password, should have the format @secret@.`key`. secret_manager will get its value

    Notes:
        Complex structure can be used by json syntax. Which means having the fomrat @secret@.`json_string`

    Return:
        Desired objects
    """
    object_config = dict(kwargs)
    # Step 1: Get Meta-Class
    module_name = importlib.import_module(object_config.pop('module'))
    class_type = getattr(module_name, object_config.pop('class'))

    # Step 2: Get Secret value
    for key, value in object_config.items():
        if isinstance(value, str) and value.startswith('@secret@.'):
            secret_key = value.split('.', 1)[1]
            secret_manager = object_config['secret_manager']
            object_config[key] = secret_manager(secret_key)
    object_config.pop('secret_manager', None)

    # Step 3: Get Json Data
    for key, value in object_config.items():
        if isinstance(value, str) and value.startswith('@json@.'):
            object_config[key] = json.loads(value[7:])

    # Step 4: Return Object
    return class_type(**object_config)

def get_object_list(config_dict: dict):
    """ List Object Factory

    Return:
        List of object ordered by key

    """
    return [get_object(**config_dict[key]) for key in sorted(list(config_dict))]

def get_object_dict(config_dict: dict):
    """ Dictionary Object Factory

    Notes:
        config dict should has key as type of "namespace.key" format

    Return:
        Dictionary of object with defined key

    """
    return {key.split('.', 1)[1]: get_object(**config_dict[key]) for key in config_dict}
