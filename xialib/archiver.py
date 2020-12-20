import abc
import json
import logging
import itertools
import random
import struct
from typing import List, Dict, Union
from functools import reduce, partial
from collections import Counter

__all__ = ['Archiver']


class Archiver(metaclass=abc.ABCMeta):
    """
    Attributes:
        data_encode (:obj:`str`): Each archiver subclass should has its pre-defined data encode
        data_format (:obj:`str`): Each archiver subclass should has its pre-defined data format
        zero_data (:obj:`any`): The data object contains no data

    """
    data_encode = None
    data_format = None
    zero_data = None

    def __init__(self, **kwargs):
        """
        Attributes:
            topic_id (:obj:`str`): Topic ID
            table_id (:obj:`str`): Table ID
            merge_key (:obj:`str`): Merge key (identical and defined by 'age' / 'start_seq' value)
            workspace (:obj:`list` of `any`): In-memory data
            workspace_size: Estimated size of workspace
        """
        self.topic_id = None
        self.table_id = None
        self.merge_key = None
        self.workspace = [self.zero_data]
        self.workspace_size = 0
        self.logger = logging.getLogger("XIA.Archiver")
        self.log_context = {'context': ''}
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

        def map_full(ori_value):
            return ori_value  # pragma: no cover

        def map_number(ori_value: Union[int, float]) -> int:
            [bits] = struct.unpack(">Q", struct.pack(">d", float(ori_value)))
            bit_string = "{:064b}".format(bits)
            exp = int(bit_string[1:12], 2)
            return exp if bit_string[0] == '0' else exp * -1

        def map_string(ori_value: str, nb: int) -> str:
            return ori_value[:nb]

        self.func_map = {
            'full': map_full,
            'number': map_number
        }
        for i in range(1, 22):
            self.func_map['c_' + str(i)] = partial(map_string, nb=i)

    def set_current_topic_table(self, topic_id: str, table_id: str):
        """Public function

        This function will prepare the archive instance to work with another topic / table

        Args:
            topic_id (:obj:`str`): Topic ID
            table_id (:obj:`str`): Table ID
        """
        self.remove_data()
        self.topic_id = topic_id
        self.table_id = table_id
        self.log_context['context'] = topic_id + '-' + table_id
        self._set_current_topic_table(topic_id, table_id)

    @abc.abstractmethod
    def _set_current_topic_table(self, topic_id: str, table_id: str):
        """ To be implemented function

        This function will make spectifc changes related to topic/table changes

        Note:
            Workspace will be cleaned!

        Args:
            topic_id (:obj:`str`): Topic ID
            table_id (:obj:`str`): Table ID
        """
        raise NotImplementedError  # pragma: no cover

    def set_merge_key(self, merge_key: str):
        """Set merge key

        Warning:
            Workspace will NOT be cleaned!

        Args:
            merge_key (:obj:`str`): Merge key
        """
        self.merge_key = merge_key

    def load_archive(self, merge_key: str, fields: List[str] = None):
        """ Public function

        This function loads the needed fields of an archive to workspace
        The workspace will be re-initilized after this operation

        Args:
            merge_key (:obj:`str`): Merge key of archive
            fields (:obj:`list` of :obj:`str`): Field list
        """
        self.remove_data()
        self.set_merge_key(merge_key)
        self.append_archive(merge_key, fields)

    def remove_data(self):
        """ Public Function

        This function remove everything thing related to workspace
        """
        self.merge_key, self.workspace, self.workspace_size = '', [self.zero_data], 0

    @abc.abstractmethod
    def add_data(self, data: List[dict]):
        """ To be implemented public function

        This function will empty workspace

        Notes:
            The workspace of an archiver is a list of data object.
            All data added by add_data function is considered as a single data object

        Args:
            data (:obj:`list` of :obj:`dict`): Python dictionary List
        """
        raise NotImplementedError  # pragma: no cover

    def get_data(self) -> List[dict]:
        """Public function

        This function return the current workspace data as Python dictionary list

        Returns:
            (:obj:`list` of :obj:`dict`): Python dictionary List
        """
        if len(self.workspace) > 1:
            self._merge_workspace()
        return self._get_data()

    @abc.abstractmethod
    def _get_data(self) -> List[dict]:
        """To be implemented function

        This function return the current workspace data as Python dictionary list
        Workspace should only have one element because _merge_workspace has been called in the public get_data method.

        Returns:
            (:obj:`list` of :obj:`dict`): Python dictionary List
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _merge_workspace(self) -> List[dict]:
        """To be implemented function

        This function will merge all data objects of workspace into the first data objects (self.workspace[0])
        """
        raise NotImplementedError  # pragma: no cover

    def archive_data(self) -> str:
        """Public function

        This function will archive workspace data into persistent storage

        Returns:
            (:obj:`dict`): Archive location
        """
        if len(self.workspace) > 1:
            self._merge_workspace()
        return self._archive_data()

    @abc.abstractmethod
    def _archive_data(self) -> str:
        """To be implemented function

        This function will archive the first data object of workspace into persistent storage

        Returns:
            (:obj:`dict`): Archive location
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def append_archive(self, append_merge_key: str, fields: List[str] = None):
        """To be implemented public function

        This function will load the data of the current topic_id, table_id and specified append_merge_key
        into workspace.

        Args:
            append_merge_key (:obj:`str`):
            fields (:obj:`list` of :obj:`str`): Field list
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def remove_archives(self, merge_key_list: List[str]):
        """To be implemented public function

                This function will load the data of the current topic_id, table_id and specified append_merge_key
                into workspace.

        Args:
             merge_key_list (:obj:`list` of :obj:`str`): The archives of the list will be deleted from storage
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_field_list(self) -> List[str]:
        """ To be implemented public function

        This function will get all field of workspace

        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_list_by_field_name(self, field_name: str) -> list:
        raise NotImplementedError  # pragma: no cover

    def describe_single_field(self, field_name: str) -> dict:
        """Public function

                This function will describe the data of a single field of the current workspace

        Args:
            field_name (:obj:`str`): field information stored in the table header

        Return:
            Field description struction:
                type: full, number or c_<n> (The first n characters)
                value: Not None value ordered by frequency
                ratio: present ratio
        """
        descriptor = {}
        field_data = self._get_list_by_field_name(field_name)
        total_nb = len(field_data)
        if not field_data:
            return descriptor
        field_types = dict(Counter([type(field_item) for field_item in field_data]))
        for key, value in field_types.items():
            if key not in [type(None), str, int, float]:
                return {}
        descriptor['none_ratio'] = field_types.get(type(None), 0) / total_nb
        field_types.pop(type(None), None)
        field_dist = dict(Counter(field_data))
        field_dist.pop(None, None)
        descriptor['distinct_nb'] = len(field_dist)
        descriptor['total_nb'] = len(field_data)
        descriptor['min_value'] = min(list(field_dist))
        descriptor['max_value'] = max(list(field_dist))
        samples = set()
        for i in range(8):
           samples.add(random.choice(field_data))
        descriptor['samples'] = list(samples)
        if len(field_dist) <= 88:
            descriptor['type'] = 'full'
            descriptor['value'] = [k for k, v in sorted(field_dist.items(), key=lambda x: x[1], reverse=True)]
            descriptor['ratio'] = [v / total_nb for k, v in sorted(field_dist.items(), key=lambda x: x[1],
                                                                   reverse=True)]
        elif len(field_types) == 1:
            if int in field_types or float in field_types:
                field_dist = dict(Counter([self.func_map['number'](item) for item in field_data if item is not None]))
                if len(field_dist) <= 88:
                    descriptor['type'] = 'number'
                    descriptor['value'] = [k for k, v in sorted(field_dist.items(), key=lambda x: x[1], reverse=True)]
                    descriptor['ratio'] = [v / total_nb for k, v in sorted(field_dist.items(), key=lambda x: x[1],
                                                                           reverse=True)]
            elif str in field_types:
                field_dist, old_field_dist = {}, {}
                for i in range(1, 22):
                    old_field_dist = field_dist
                    mapped_field = [self.func_map['c_' + str(i)](item) for item in field_data if item is not None]
                    field_dist = dict(Counter(mapped_field ))
                    if len(field_dist) > 88 and len(old_field_dist) > 0:
                        descriptor['type'] = 'c_' + str(i-1)
                        descriptor['value'] = [k for k, v in sorted(old_field_dist.items(), key=lambda x: x[1],
                                                                    reverse=True)]
                        descriptor['ratio'] = [v / total_nb for k, v in
                                               sorted(old_field_dist.items(), key=lambda x: x[1], reverse=True)]
                        break
        return descriptor

    def describe_relation(self, field1_name, field1_desc: dict, field2_name, field2_desc: dict, rank: int = 1) -> dict:
        result = {}
        field1_rank = min(rank, len(field1_desc['value']))
        field2_rank = min(rank, len(field2_desc['value']))
        field1_type = field1_desc['type']
        field2_type = field2_desc['type']
        field1_list = self._get_list_by_field_name(field1_name)
        field2_list = self._get_list_by_field_name(field2_name)
        field1_mapped_list = [self.func_map[field1_type](item) for item in field1_list if item is not None]
        field2_mapped_list = [self.func_map[field2_type](item) for item in field2_list if item is not None]
        total_nb = len(field1_list)
        for cur_comb in itertools.product(range(field1_rank), range(field2_rank)):
            f1_v = field1_desc['value'][cur_comb[0]]
            f2_v = field2_desc['value'][cur_comb[1]]
            ok_items = [(v1, v2) for v1, v2 in zip(field1_mapped_list, field2_mapped_list) if f1_v == v1 and f2_v == v2]
            result[cur_comb] = len(ok_items) / total_nb
        return result

class ListArchiver(Archiver):
    data_encode = 'blob'
    data_format = 'zst'
    zero_data = dict()

    def record_to_list(self, record_data: List[dict]) -> Dict[str, list]:
        if not record_data:
            return dict()
        field_list = reduce(lambda a, b: set(a) | set(b), record_data)
        return {k: [x.get(k, None) for x in record_data] for k in field_list}

    def list_to_record(self, list_data: Dict[str, list]) -> List[dict]:
        if not list_data:
            return list()
        vector_size_set = [len(value) for key, value in list_data.items()]
        l_size = vector_size_set[0]
        return [{key: value[i] for key, value in list_data.items() if value[i] is not None} for i in range(l_size)]

    def _merge_workspace(self):
        field_list = reduce(lambda a, b: set(a) | set(b), self.workspace)
        self.workspace[:] = [{key: [u for i in self.workspace for u in i.get(key, [])] for key in field_list}]

    def add_data(self, data: List[dict]):
        list_data = self.record_to_list(data)
        self.workspace_size += len(json.dumps(list_data))
        self.workspace.append(list_data)

    def _get_data(self):
        return self.list_to_record(self.workspace[0])

    def _get_list_by_field_name(self, field_name: str):
        if len(self.workspace) > 1:
            self._merge_workspace()
        return self.workspace[0].get(field_name, [])

    def get_field_list(self):
        result_set = set()
        for workspace in self.workspace:
            result_set |= set(workspace)
        return list(result_set)