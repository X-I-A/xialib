import abc
import json
import logging
from typing import List, Dict
from functools import reduce

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

