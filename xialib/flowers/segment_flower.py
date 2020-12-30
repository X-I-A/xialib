from typing import Tuple, List, Union
from collections import deque
from xialib.flower import Flower

class SegmentFlower(Flower):
    """Segment Flower: Adding segment information to data flow

    Args:
        config (:obj:`dict`): field_name, default, type_chain, min, max, list

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
        if not isinstance(config, dict) or any([key not in config for key in ['id', 'field_name',
                                                                              'default', 'type_chain']]):
            self.logger.error("Wrong Configuration of Segment FLower")
            raise ValueError("XIA-000024")
        elif 'min' in config and config['default'] is not None and config['default'] < config['min']:
            self.logger.error("Segment FLower default value cannot pass check")
            raise ValueError("XIA-000025")
        elif 'max' in config and config['default'] is not None and config['default'] > config['max']:
            self.logger.error("Segment FLower default value cannot pass check")
            raise ValueError("XIA-000025")
        elif 'list' in config and config['default'] is not None and config['default'] not in config['list']:
            self.logger.error("Segment FLower default value cannot pass check")
            raise ValueError("XIA-000025")
        elif 'list' in config and ('min' in config or 'max' in config):
            self.logger.error("Segment FLower can not have min/max and list constraints at the same time")
            raise ValueError("XIA-000028")

    def _check_value(self, check_value) -> bool:
        if 'list' in self.config and check_value not in self.config['list']:
            return False
        else:
            if 'min' in self.config and check_value < self.config['min']:
                return False
            if 'max' in self.config and check_value > self.config['max']:
                return False
        return True

    def check_line(self, line: dict) -> bool:
        if self.config['field_name'] not in line or line[self.config['field_name']] is None:
            return False
        check_value = line[self.config['field_name']]
        return self._check_value(check_value)

    def check_comptabilbe(self, config: Union[dict, None]) -> bool:
        if self.config is None or config is None:
            return False
        if self.config['id'] == config['id']:
            return False
        if self.config['field_name'] != config['field_name']:
            return False
        if self.config['default'] == config['default'] and self.config['default'] is not None:
            return False
        if ('list' in self.config) != ('list' in config):
            return False
        if 'list' in config and set(self.config['list']) & set(config['list']):
            return False
        if 'min' in config and 'max' not in config and 'min' in self.config and 'max' not in self.config:
            return False
        if 'max' in config and 'min' not in config and 'max' in self.config and 'min' not in self.config:
            return False
        if 'min' in config and self._check_value(config['min']):
            return False
        if 'max' in config and self._check_value(config['max']):
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
            if self.config['field_name'] not in line and self.config['default'] is not None:
                new_line = line.copy()
                new_line[self.config['field_name']] = self.config['default']
            else:
                new_line = line
            out_list.append(new_line)
        if all([key not in self.config for key in ['min', 'max', 'list']]):
            return data_header, out_list
        return data_header, [line for line in out_list if self.check_line(line)]
