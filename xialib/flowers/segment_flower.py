from typing import Tuple, List, Union
from collections import deque
from xialib.flower import Flower

class SegmentFlower(Flower):
    """Segment Flower: Adding segment information to data flow

    Args:
        config (:obj:`dict`): field_name, default, type_chain, min, max, list, null

    Notes:
        Main purpose of this flower is preparing N to 1 replication while each segment must be separated

    """

    def __init__(self, config: Union[dict, None] = None):
        super().__init__(config=config)
        self.check_config(config)
        self.config = config

    def check_config(self, config: Union[dict, None]):
        if config is None:
            return
        if not isinstance(config, dict) or \
                any([key not in config for key in ['id', 'field_name', 'type_chain']]) or \
                all([key not in config for key in ['default', 'list', 'min', 'max', 'null']]):
            self.logger.error("Wrong Configuration of Segment FLower")
            raise ValueError("XIA-000024")
        config = config.copy()
        if 'default' in config:
            if any([key in config for key in ['min', 'max', 'list']]):
                self.logger.error("Segment FLower default value error")
                raise ValueError("XIA-000025")
        else:
            if ('min' in config) != ('max' in config):
                self.logger.error('Segment Flower min or max missing in range configuration')
                raise ValueError("XIA-000029")
            elif ('list' in config or 'none' in config) and len(config) > 4:
                self.logger.error("Segment FLower can not have multiple constraints at the same time")
                raise ValueError("XIA-000028")
            elif ('min' in config and len(config) != 5):
                self.logger.error("Segment FLower can not have multiple constraints at the same time")
                raise ValueError("XIA-000028")

    def check_line(self, line: dict) -> bool:
        check_value = line.get(self.config['field_name'], None)
        if 'null' in self.config:
            return True if check_value is None else False
        if check_value is None:
            return True if 'null' in self.config else False
        if 'list' in self.config and check_value not in self.config['list']:
            return False
        elif ('min' in self.config or 'max' in self.config) and \
                not (self.config['min'] <= check_value <= self.config['max']) :
            return False
        return True

    def check_compatible(self, config: Union[dict, None]) -> bool:
        # Check 1: Structure Check
        if self.config is None or config is None:
            return False
        if self.config['id'] == config['id']:
            return False
        if self.config['field_name'] != config['field_name']:
            return False
        if ('default' in self.config) != ('default' in config):
            return False
        # Check 2.1: Null Value Check
        if ('null' in self.config) != ('null' in config):
            return True
        elif 'null' in self.config and 'null' in config:
            return False
        # Check 2.3: Not Null Value Check
        if 'list' in self.config and 'list' in config and set(self.config['list']) & set(config['list']):
            return False
        elif 'min' in self.config and 'min' in config and \
                        self.config['min'] <= config['max'] and self.config['max'] >= config['min']:
            return False
        elif 'list' in self.config and 'min' in config and \
                any([config['min'] <= v <= config['max'] for v in self.config['list']]):
            return False
        elif 'list' in config and 'min' in self.config and \
                any([self.config['min'] <= v <= self.config['max'] for v in config['list']]):
            return False
        return True

    def set_params(self, config: dict):
        self.check_config(config)
        self.config = config

    def _header_flow(self, data_header: dict, data_body: List[dict]):
        if self.config is None:
            return data_header, data_body
        header_meta = data_header.get('meta_data', {}).copy()
        header_meta.update({'segment': self.config})
        data_header = data_header.copy()
        data_header['meta_data'] = header_meta

        header_lines = [line for line in data_body if line['field_name'] == self.config['field_name']]
        if not header_lines:
            data_body = data_body.copy()
            data_body.append({'field_name': self.config['field_name'], 'type_chain': self.config['type_chain'],
                              'key_flag': True, 'format': None, 'encode': None, 'description': ''})

        return data_header, data_body

    def _body_flow(self, data_header: dict, data_body: List[dict]):
        if self.config is None:
            return data_header, data_body
        data_header = data_header.copy()
        data_header['segment_id'] = self.config['id']
        # We need to keep the list order / do not modify data_body / memory efficient
        in_list = deque(data_body)
        out_list = list()
        for i in range(len(in_list)):
            line = in_list.popleft()
            if self.config['field_name'] not in line and 'default' in self.config:
                new_line = line.copy()
                new_line[self.config['field_name']] = self.config['default']
            else:
                new_line = line
            out_list.append(new_line)
        if all([key not in self.config for key in ['min', 'max', 'list', 'null']]):
            return data_header, out_list
        return data_header, [line for line in out_list if self.check_line(line)]
