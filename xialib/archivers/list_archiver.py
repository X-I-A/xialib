import os
import json
import base64
import zipfile
from typing import List, Dict
from functools import reduce
from xialib.archiver import Archiver


class ListArchiver(Archiver):
    """List archivers conver json data to columns and save each column as a seprated file of a zip-archive
    """
    data_encode = 'blob'
    data_format = 'zst'
    data_store = 'file'
    zero_data = dict()

    def __init__(self, archive_path, **kwargs):
        super().__init__()
        if os.path.exists(archive_path):
            self.archive_path = archive_path
        else:
            self.logger.error("{} does not exist".format(archive_path), extra=self.log_context)
            raise ValueError("XIA-000012")

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

    def _set_current_topic_table(self, topic_id: str, table_id: str):
        self.topic_path = os.path.join(self.archive_path, self.topic_id)
        self.table_path = os.path.join(self.topic_path, self.table_id)
        if not os.path.exists(self.topic_path):
            os.makedirs(self.topic_path)
        if not os.path.exists(self.table_path):
            os.makedirs(self.table_path)

    def add_data(self, data: List[dict]):
        list_data = self.record_to_list(data)
        self.workspace_size += len(json.dumps(list_data))
        self.workspace.append(list_data)

    def _get_data(self):
        return self.list_to_record(self.workspace[0])

    def _archive_data(self):
        archive_file_name = os.path.join(self.table_path, self.merge_key + '.zst')
        with zipfile.ZipFile(archive_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as f:
            for key, value in self.workspace[0].items():
                item_name = base64.b32encode(key.encode()).decode()
                f.writestr(item_name, json.dumps(value, ensure_ascii=False))
        return archive_file_name

    def append_archive(self, append_merge_key: str, fields: List[str] = None):
        field_list = fields
        archive_file_name = os.path.join(self.table_path, append_merge_key + '.zst')
        with zipfile.ZipFile(archive_file_name) as f:
            load_list = [item for item in f.infolist() if base64.b32decode(item.filename).decode() in field_list]
            list_data = {base64.b32decode(im.filename).decode(): json.loads(f.read(im).decode()) for im in load_list}
            list_size = sum([item.file_size for item in load_list])
            self.workspace.append(list_data)
            self.workspace_size += list_size

    def remove_archives(self, merge_key_list: List[str]):
        for merge_key in merge_key_list:
            os.remove(os.path.join(self.table_path, merge_key + '.zst'))