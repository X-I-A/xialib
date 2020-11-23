import os
import json
import base64
import gzip
from typing import List, Dict, Any, Union, Generator
from xialib.depositor import Depositor


class FileDepositor(Depositor):
    data_encode = 'b64g'
    size_limit = 2 ** 20
    file_type = {'initial': '.initial', 'merged': '.merged', 'packaged': '.packaged'}

    def __init__(self, deposit_path, **kwargs):
        super().__init__()
        if os.path.exists(deposit_path):
            self.archive_path = deposit_path
        else:
            self.logger.error("{} does not exist".format(deposit_path), extra=self.log_context)
            raise ValueError("XIA-000015")
        self.deposit_path = deposit_path

    def _get_ref_from_filename(self, filename):
        file = filename.split('.')[0]
        if os.path.exists(os.path.join(self.table_path, file + '.initial')):
            return file+'.initial'
        elif os.path.exists(os.path.join(self.table_path, file + '.merged')):
            return file+'.merged'
        elif os.path.exists(os.path.join(self.table_path, file + '.header')):
            return file + '.header'
        else:
            return file + '.packaged'

    def _set_current_topic_table(self, topic_id: str, table_id: str):
        self.topic_path = os.path.join(self.deposit_path, self.topic_id)
        self.table_path = os.path.join(self.topic_path, self.table_id)
        if not os.path.exists(self.topic_path):
            os.makedirs(self.topic_path)
        if not os.path.exists(self.table_path):
            os.makedirs(self.table_path)

    def _add_document(self, header: dict, data: bytes) -> dict:
        if header['merge_status'] == 'header':
            doc_ref = header['sort_key'] + '.header'
        else:
            doc_ref = header['sort_key'] + '-' + header['merge_key'] + self.file_type.get(header['merge_status'])
        old_file = os.path.join(self.table_path, self._get_ref_from_filename(doc_ref))
        if os.path.exists(old_file):
            os.remove(old_file)
        doc_content = header.copy()
        doc_content['data'] = base64.b64encode(data).decode()
        doc_content['data_size'] = len(doc_content['data'])
        with open(os.path.join(self.table_path, doc_ref), 'w') as f:
            f.write(json.dumps(doc_content, ensure_ascii=False))
        return doc_content

    def _update_document(self, ref: Any, header: dict, data: bytes):
        ori_filename = os.path.join(self.table_path, self._get_ref_from_filename(ref))
        tar_filename = os.path.join(self.table_path, '.'.join([ref.split('.')[0], header['merge_status']]))
        doc_content = header.copy()
        doc_content['data'] = base64.b64encode(data).decode()
        doc_content['data_size'] = len(doc_content['data'])
        with open(tar_filename, 'w') as f:
            f.write(json.dumps(doc_content, ensure_ascii=False))
        if ori_filename != tar_filename:
            os.remove(ori_filename)
        return doc_content

    def _update_header(self, ref: Any, header: dict):
        ori_filename = os.path.join(self.table_path, self._get_ref_from_filename(ref))
        if 'merge_status' in header:
            tar_filename = os.path.join(self.table_path, '.'.join([ref.split('.')[0], header['merge_status']]))
        else:
            tar_filename = ori_filename
        with open(ori_filename, 'rb') as f:
            doc_content = json.loads(f.read().decode())
        for key, value in header.items():
            if key not in doc_content and value != self.DELETE:
                doc_content[key] = value
            elif value == self.DELETE:
                doc_content.pop(key, None)
            else:
                doc_content[key] = value
        with open(tar_filename, 'w') as f:
            f.write(json.dumps(doc_content, ensure_ascii=False))
        if ori_filename != tar_filename:
            os.remove(ori_filename)
        return doc_content

    def delete_documents(self, ref_list):
        for file_to_delete in ref_list:
            os.remove(os.path.join(self.table_path, self._get_ref_from_filename(file_to_delete)))
        return True

    def get_header_from_ref(self, doc_ref: Any):
        with open(os.path.join(self.table_path, self._get_ref_from_filename(doc_ref)), 'rb') as f:
            return json.loads(f.read().decode())

    def get_data_from_header(self, header: dict):
        return json.loads(gzip.decompress(base64.b64decode(header['data'])).decode())

    def get_ref_by_merge_key(self, merge_key):
        based_doc_query = [fn for fn in os.listdir(self.table_path) if ('-' + merge_key + '.') in fn]
        if based_doc_query:
            return self._get_ref_from_filename(max(based_doc_query))

    def get_stream_by_sort_key(self,
                               status_list: List[str] = None,
                               le_ge_key: str = None,
                               reverse: bool = False,
                               min_merge_level: int = 0,
                               equal: bool = True):
        if not status_list:
            status_list = ['header', 'initial', 'merged', 'packaged']
        if reverse:
            if not le_ge_key:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list))], reverse=True)
            elif equal:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] <= le_ge_key], reverse=True)
            else:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] < le_ge_key], reverse=True)
        else:
            if not le_ge_key:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list))])
            elif equal:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] >= le_ge_key])
            else:
                doc_list = sorted([f for f in os.listdir(self.table_path)
                                   if f.endswith(tuple(status_list)) and f[:20] > le_ge_key])
        # Merge Level Check
        for doc in doc_list:
            if not doc.endswith('.header') and self.calc_merge_level((doc.split('.')[0])[-20:]) < min_merge_level:
                continue
            yield self._get_ref_from_filename(doc)

    def get_table_header(self) -> Any:
        header_list = [f for f in os.listdir(self.table_path) if f.endswith(('.header'))]
        if not header_list:
            return None
        else:
            return max(header_list)

    def inc_table_header(self, **kwargs):
        header_ref = self.get_table_header()
        header_dict = self.get_header_from_ref(header_ref)
        header_dict.pop('data', None)
        for key, value in kwargs.items():
            header_dict[key] = header_dict.get(key, 0) + value
        self.update_document(header_ref, header_dict)
        return header_dict
