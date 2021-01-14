import io
import os
import json
import base64
import zipfile
import hashlib
from typing import List, Dict
from functools import reduce
from xialib.archiver import ListArchiver
from xialib.storer import RWStorer

class IOListArchiver(ListArchiver):
    """Basic List archiver use local file system to save archive data
    """
    def __init__(self, fs: RWStorer, **kwargs):
        super().__init__(**kwargs)
        if not isinstance(fs, RWStorer):
            self.logger.error("storer must be type of RWStorer", extra=self.log_context)
            raise TypeError("XIA-000018")
        self.storer = fs
        self.data_store = fs.store_types[0]
        if "archive_path" not in kwargs:
            self.archive_path = self._get_default_archive_path()
        else:
            if not self.storer.exists(kwargs['archive_path']):
                self.logger.error("{} does not exist".format(kwargs['archive_path']), extra=self.log_context)
                raise ValueError("XIA-000012")
            self.archive_path = kwargs['archive_path']

    def _get_default_archive_path(self):
        archive_path = os.path.join('.', 'archiver')
        if not os.path.exists(archive_path):
            os.mkdir(archive_path)  # pragma: no cover
        return archive_path

    def _get_filename(self, merge_key):
        return hashlib.md5(merge_key.encode()).hexdigest()[:4] + '-' + merge_key + '.zst'

    def _set_current_topic_table(self, topic_id: str, table_id: str):
        self.topic_path = self.storer.join(self.archive_path, self.topic_id)
        self.table_path = self.storer.join(self.topic_path, self.table_id)
        if not self.storer.exists(self.topic_path):
            self.storer.mkdir(self.topic_path)
        if not self.storer.exists(self.table_path):
            self.storer.mkdir(self.table_path)

    def _archive_data(self):
        archive_file_name = self.storer.join(self.table_path, self._get_filename(self.merge_key))
        write_io = io.BytesIO()
        with zipfile.ZipFile(write_io, 'w', compression=zipfile.ZIP_DEFLATED) as f:
            for key, value in self.workspace[0].items():
                item_name = base64.b32encode(key.encode()).decode()
                f.writestr(item_name, json.dumps(value, ensure_ascii=False))
        write_io.flush()
        self.storer.write(write_io, archive_file_name)
        # for write_io in self.storer.get_io_wb_stream(archive_file_name):
        return archive_file_name

    def append_archive(self, append_merge_key: str, fields: List[str] = None):
        field_list = fields
        archive_file_name = self.storer.join(self.table_path, self._get_filename(append_merge_key))
        for read_io in self.storer.get_io_stream(archive_file_name):
            with zipfile.ZipFile(read_io) as f:
                fd_list = [item for item in f.infolist() if base64.b32decode(item.filename).decode() in field_list]
                list_data = {base64.b32decode(im.filename).decode(): json.loads(f.read(im).decode()) for im in fd_list}
                list_size = sum([item.file_size for item in fd_list])
                self.workspace.append(list_data)
                self.workspace_size += list_size

    def remove_archives(self, merge_key_list: List[str]):
        for merge_key in merge_key_list:
            self.storer.remove(self.storer.join(self.table_path, self._get_filename(merge_key)))
