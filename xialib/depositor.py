import abc
import io
import gzip
import json
import hashlib
import datetime
import logging
from functools import reduce
from typing import List, Dict, Any, Union, Generator

__all__ = ['Depositor']


class Depositor(metaclass=abc.ABCMeta):
    """
    Attributes:
        DELETE (:obj:`object`): Delete symbol for document field delete
        data_encode (:obj:`str`): Each depositor subclass should has its pre-defined data encode
        size_limit (:obj:`int`): Each depositor will have its limit of document size.

    Note:
        It is forbidden to create dependency among XIA work units, each depositor must implement its encoder
    """
    DELETE = object()
    data_encode = None
    size_limit = 2 ** 20

    def __init__(self, **kwargs):
        """
        Attributes:
            topic_id (:obj:`str`): Topic ID
            table_id (:obj:`str`): Table ID
        """
        self.topic_id = None
        self.table_id = None
        self.logger = logging.getLogger("XIA.Depositor")
        self.log_context = {'context': ''}
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    @classmethod
    def calc_merge_level(cls, merge_key):
        hex_dict = {'2': 1, '4': 2, '6': 1, '8': 3, 'a': 1, 'c': 2, 'e': 1}
        prove = hashlib.md5(merge_key.encode()).hexdigest()
        zero_count = (len(prove) - len(prove.rstrip('0'))) * 4 + hex_dict.get(prove[0], 0)
        mlvl_dict = {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 2, 6: 2, 7: 3, 8: 3, 9: 4, 10: 5, 11: 6}
        return mlvl_dict.get(zero_count, 7)

    @classmethod
    def get_current_timestamp(cls):
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')

    def set_current_topic_table(self, topic_id: str, table_id: str):
        """Public function

        This function will prepare the depositor instance to work with another topic / table

        Args:
            topic_id (:obj:`str`): Topic ID
            table_id (:obj:`str`): Table ID
        """
        self.topic_id = topic_id
        self.table_id = table_id
        self.log_context['context'] = topic_id + '-' + table_id
        self._set_current_topic_table(topic_id, table_id)

    @abc.abstractmethod
    def _set_current_topic_table(self, topic_id: str, table_id: str):
        """ To be implemented function

        This function will make spectifc changes related to topic/table changes

        Args:
            topic_id (:obj:`str`): Topic ID
            table_id (:obj:`str`): Table ID
        """
        raise NotImplementedError  # pragma: no cover

    def _get_aged_data_chunk(self, header: dict, input_data: List[dict]) -> Generator[dict, None, None]:
        if len(input_data) == 0:
            zero_data = gzip.compress(json.dumps([]).encode())
            yield {'header': header, 'data': zero_data, 'line_nb': 0}
            return
        chunk_size = self.size_limit // 8
        chunk_number, raw_size, cur_age, line_no, nb, data_io, zipped_size, zipped_io = 0, 0, None, 0, 0, None, 0, None
        for line in input_data:
            cur_age = line['_AGE'] if cur_age is None else cur_age
            line['_AGE'] = cur_age
            if '_NO' in line:
                line['_NO'] = line_no
                line_no += 1
            json_line = json.dumps(line, ensure_ascii=False)
            if data_io is None:
                data_io = io.BytesIO()
                zipped_io = gzip.GzipFile(mode='wb', fileobj=data_io)
                zipped_io.write(('[' + json_line).encode())
            else:
                zipped_io.write((',' + json_line).encode())
            raw_size += (len(json_line) + 1)
            nb += 1

            cur_chunk_number = raw_size // chunk_size
            if cur_chunk_number != chunk_number:
                chunk_number = cur_chunk_number
                zipped_io.flush()
                zipped_size = data_io.getbuffer().nbytes
                if zipped_size >= self.size_limit // 2:
                    zipped_io.write(']'.encode())
                    zipped_io.close()
                    chunk_header = header.copy()
                    if cur_age > header.get('end_age', header['age']):
                        self.logger.error("Not enough age ranged defined for an aged flow", extra=self.log_context)
                        raise ValueError('XIA-000016')
                    chunk_header['age'] = cur_age
                    chunk_header.pop('end_age', None)
                    chunk_data = data_io.getvalue()
                    yield {'header': chunk_header, 'data': chunk_data, 'line_nb': nb}
                    cur_age += 1
                    chunk_number, raw_size, line_no, nb, data_io, zipped_size, zipped_io = 0, 0, 0, 0, None, 0, None
        if raw_size > 0 or cur_age != header.get('end_age', cur_age):
            if raw_size > 0:
                zipped_io.write(']'.encode())
                zipped_io.close()
                chunk_data = data_io.getvalue()
            else:
                chunk_data = gzip.compress(b'[]')
            chunk_header = header.copy()
            if cur_age > header.get('end_age', header['age']):
                self.logger.error("Not enough age ranged defined for an aged flow", extra=self.log_context)
                raise ValueError('XIA-000016')
            chunk_header['age'] = cur_age
            chunk_header['end_age'] = header.get('end_age', cur_age)
            yield {'header': chunk_header, 'data': chunk_data, 'line_nb': nb}

    def _get_normal_data_chunk(self, header: dict, input_data: List[dict]) -> Generator[dict, None, None]:
        if len(input_data) == 0:
            zero_data = gzip.compress(json.dumps([]).encode())
            yield {'header': header, 'data': zero_data, 'line_nb': 0}
            return
        chunk_size = self.size_limit // 8
        chunk_number, raw_size, cur_seq, line_no, nb, data_io, zipped_size, zipped_io = 0, 0, None, 0, 0, None, 0, None
        for line in input_data:
            cur_seq = line['_SEQ'] if cur_seq is None or line['_SEQ'] > cur_seq else cur_seq
            line['_SEQ'] = cur_seq
            if '_NO' in line:
                line['_NO'] = line_no
                line_no += 1
            json_line = json.dumps(line, ensure_ascii=False)
            if data_io is None:
                data_io = io.BytesIO()
                zipped_io = gzip.GzipFile(mode='wb', fileobj=data_io)
                zipped_io.write(('[' + json_line).encode())
            else:
                zipped_io.write((',' + json_line).encode())
            nb += 1
            raw_size += (len(json_line) + 1)

            cur_chunk_number = raw_size // chunk_size
            if cur_chunk_number != chunk_number:
                chunk_number = cur_chunk_number
                zipped_io.flush()
                zipped_size = data_io.getbuffer().nbytes
                if zipped_size >= self.size_limit // 2:
                    zipped_io.write(']'.encode())
                    zipped_io.close()
                    chunk_header = header.copy()
                    chunk_header['start_seq'] = cur_seq
                    yield {'header': chunk_header, 'data': data_io.getvalue(), 'line_nb': nb}
                    cur_seq = str(int(cur_seq) + 1)
                    chunk_number, raw_size, line_no, nb, data_io, zipped_size, zipped_io = 0, 0, 0, 0, None, 0, None
        if raw_size > 0:
            zipped_io.write(']'.encode())
            zipped_io.close()
            chunk_header = header.copy()
            chunk_header['start_seq'] = cur_seq
            yield {'header': chunk_header, 'data': data_io.getvalue(), 'line_nb': nb}

    def add_document(self, header: dict, data: List[dict]) -> List[dict]:
        """ Public function

        This function will add a document to depositor. The following properties:
        ``aged``, ``merge_status``, ``merge_key``, ``merge_level``, ``deposit_at``, ``sort_key``
        might be added to header

        Args:
            header (:obj:`dict`): Document Header
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format

        Returns:
            :obj:`list` of :obj:`dict`: List of added document header
        """
        self.set_current_topic_table(header['topic_id'], header['table_id'])
        content = header.copy()
        content.pop('merged_level', None)
        content.pop('segment_start_time', None)
        content.pop('segment_start_age', None)
        # Case 1 : Header
        if int(content.get('age', 0)) == 1:
            content['age'] = 1
            content['aged'] = (content.get('aged', '').lower() == 'true')
            content['merge_status'] = 'header'
            content['merge_level'] = 9
            content['sort_key'] = content['start_seq']
            content['line_nb'] = len(data)
            content['deposit_at'] = self.get_current_timestamp()
            for key in ['merged_size', 'merged_lines', 'packaged_size', 'packaged_lines']:
                content.pop(key, None)
            return [self._add_document(content, gzip.compress(json.dumps(data, ensure_ascii=False).encode()))]
        # Case 2 : Aged Document
        elif 'age' in content:
            for key in [k for k in ['age', 'end_age'] if k in content]:
                content[key] = int(content[key])
            data = sorted(data, key=lambda a: (a.get('_AGE', 0), a.get('_NO', 0)))
            result_headers = list()
            content['merge_status'] = 'initial'
            for result in self._get_aged_data_chunk(content, data):
                chunk_h = result['header']
                chunk_h['merge_key'] = str(int(chunk_h['start_seq']) + chunk_h.get('end_age', chunk_h['age']))
                chunk_h['sort_key'] = chunk_h['merge_key']
                chunk_h['merge_level'] = self.calc_merge_level(chunk_h['merge_key'])
                chunk_h['line_nb'] = result['line_nb']
                chunk_h['deposit_at'] = self.get_current_timestamp()
                result_headers.append(self._add_document(chunk_h, result['data']))
            return result_headers
        # Case 3 : Normal Document
        else:
            data = sorted(data, key=lambda a: (a.get('_SEQ', ''), a.get('_NO', 0)))
            result_headers = list()
            content['merge_status'] = 'initial'
            for result in self._get_normal_data_chunk(content, data):
                chunk_h = result['header']
                chunk_h['merge_key'] = chunk_h['start_seq']
                chunk_h['merge_level'] = self.calc_merge_level(chunk_h['merge_key'])
                chunk_h['deposit_at'] = self.get_current_timestamp()
                chunk_h['sort_key'] = chunk_h['deposit_at']
                chunk_h['line_nb'] = result['line_nb']
                result_headers.append(self._add_document(chunk_h, result['data']))
            return result_headers

    @abc.abstractmethod
    def _add_document(self, header: dict, data: bytes) -> dict:
        """ To be implemented function

        This function will add a header-prepared document to depositor

        Args:
            header (:obj:`str`): Document Header
            data (:obj:`bytes`): gzipped bytes objects

        Returns:
            :obj:`dict`: Last chunked Added document header
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _update_document(self, ref: Any, header: dict, data: bytes) -> dict:
        """ To be implemented function

        This function will update document and its data

        Args:
            ref (:obj:`Any`): Document reference
            header (:obj:`str`): Document Header
            data (:obj:`bytes`): gzipped bytes objects

        Returns:
            :obj:`dict`: Last chunked Modified document header
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _update_header(self, ref: Any, header: dict) -> dict:
        """ To be implemented function

        This function will update document and its data

        Args:
            ref (:obj:`Any`): Document reference
            header (:obj:`str`): Document Header, only the field presented will be updated

        Returns:
            :obj:`dict`: Modified document header
        """
        raise NotImplementedError  # pragma: no cover

    def update_document(self, ref: Any, header: dict, data: List[dict] = None) -> dict:
        """ Public function

        This function will update document and its data

        Args:
            ref (:obj:`Any`): Document reference
            header (:obj:`str`): Document Header
            data (:obj:`str`): Data in Python dictioany list format, when it is not specified,
                Only specified field of header will be updated (if possible)

        Returns:
            :obj:`dict`: Last chunked Modified document header
        """
        if data is not None:
            data = sorted(data, key=lambda a: (a.get('_AGE', 0), a.get('_SEQ', ''), a.get('_NO', 0)))
            header['line_nb'] = len(data)
            gzipped_data = gzip.compress(json.dumps(data, ensure_ascii=False).encode())
            if len(gzipped_data) >= self.size_limit:
                self.logger.error("Updated data is oversized", extra=self.log_context)
                raise ValueError("XIA-000017")
            return self._update_document(ref, header, gzipped_data)
        else:
            return self._update_header(ref, header)

    @abc.abstractmethod
    def delete_documents(self, ref_list) -> bool:
        """ To be implemented public function

        This function will delete all document whose reference is in the list

        Args:
            ref_list (:obj:`list` of :obj:`Any`): List of document reference

        Returns:
            True if successful, False otherwise.
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_ref_by_merge_key(self, merge_key: str) -> Any:
        """ To be implemented public function

        This function will get the document reference by the specified document key

        Args:
            merge_key (:obj:`str`): Merge key

        Returns:
            :obj:`Any`: Document reference
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_table_header(self) -> Any:
        """ To be implemented public function

        This function will get the document reference of the current topic_id / table_id

        Returns:
            :obj:`Any`: Document reference
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def inc_table_header(self, **kwargs) -> dict:
        """ To be implemented public function

        This function will increment some field of table header document

        Args:
            field1 (:obj:`str`): increment value 1
            field2 (:obj:`str`): increment value 2

        Returns:
            :obj:`dict`: Document after update in Python dictioany format
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_header_from_ref(self, ref: Any) -> dict:
        """ To be implemented public function

        This function will get the document content (data unpacked) by reference

        Args:
            ref (:obj:`Any`): Document reference

        Returns:
            :obj:`dict`: Document (header + unpacked data) in Python dictioany format
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_data_from_header(self, header: dict) -> List[dict]:
        """ To be implemented public function

        This function will get data from stored body data

        Args:
            header (:obj:`dict`): Document header

        Returns:
            (:obj:`list` of :obj:`dict`): Data in Python dictioany list format
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_stream_by_sort_key(self,
                               status_list: List[str] = None,
                               le_ge_key: str = None,
                               reverse: bool = False,
                               min_merge_level: int = 0,
                               equal: bool = True) -> Generator[Any, None, None]:
        """ To be implemented public function

        This function will yield document reference one by one by using the given parameters

        Args:
            status_list (:obj:`list` of :obj:`str`):
                combinaison of ``header``, ``packaged``, ``merged``, ``initial``. Default None means all status
            le_ge_key (:obj:`str`): ``sort_key`` to be compared. Default None means all scope search
            reverse (:obj:`bool`): lower than or great than. Default False means start by the le_ge_key
            min_merge_level (:obj:`int`): Only showing the document with at least the given merge_level.
                Default 0 means all
            equal (:obj:`bool`): Whether the document with given sort_key should be included in the output.

        Yields:
            :obj:`Any`: Document reference
        """
        raise NotImplementedError  # pragma: no cover

    def merge_documents(self, merge_key: str, target_merge_level: int) -> bool:
        """ Public function

        Merge all of the document under the control of leader document.

        Args:
            merge_key (:obj:`str`): Leader document merge_key
            target_merge_level (:obj:`int`): Target merge level

        Returns:
            True if successful, False otherwise.
        """
        self.log_context['context'] = self.topic_id + '-' + self.table_id + '-' \
                                      + merge_key + '(' + str(target_merge_level) + ')'
        base_doc = self.get_ref_by_merge_key(merge_key)
        if not base_doc:
            self.logger.error("Can not get base doc by Merge Key", extra=self.log_context)
            return False
        base_doc_header = self.get_header_from_ref(base_doc)
        if base_doc_header.get('merged_level', 0) < target_merge_level - 1:
            # Not really possible to happen because the higher level merge could only be triggered by lower level
            self.logger.warning("Lower level merge has not yet finished", extra=self.log_context)  # pragma: no cover
            return False # pragma: no cover
        elif base_doc_header.get('merged_level', 0) >= target_merge_level:
            self.logger.warning("This level has already been merged", extra=self.log_context)
            return True
        if 'age' in base_doc_header:
            aged_merge_task = self._get_aged_merge_task(base_doc, base_doc_header, target_merge_level)
            if not aged_merge_task:
                return False
            return self._aged_merge(aged_merge_task, target_merge_level)
        else:
            normal_merge_task = self._get_normal_merge_task(base_doc, base_doc_header, target_merge_level)
            if not normal_merge_task:
                return False
            return self._normal_merge(normal_merge_task, target_merge_level)

    def _get_aged_merge_task(self, leader_doc_ref: Any, leader_doc_dict: dict, target_merge_level: int) -> List[dict]:
        doc_header = {}
        current_start_age = leader_doc_dict.get('segment_start_age', leader_doc_dict['age'])
        segment_end_age = leader_doc_dict.get('end_age', leader_doc_dict['age'])
        task_list = [{'ref': leader_doc_ref, 'start_age': current_start_age, 'end_age': segment_end_age,
                      'task_start_age': current_start_age, 'task_end_age': segment_end_age,
                      'merged': leader_doc_dict['merge_status'] == 'merged', 'size': leader_doc_dict['data_size']}]
        for doc_ref in self.get_stream_by_sort_key(le_ge_key=leader_doc_dict['sort_key'],
                                                   reverse=True,
                                                   min_merge_level=(target_merge_level - 1),
                                                   equal=False):
            doc_header = self.get_header_from_ref(doc_ref)

            if doc_header['start_seq'] < leader_doc_dict['start_seq']:
                self.logger.warning("Old data reached without meeting header", extra=self.log_context)
                return list()
            if doc_header['merge_level'] >= target_merge_level:
                self.logger.info("End of scope: Higher merge level reached", extra=self.log_context)
                break

            if doc_header.get('merged_level', 0) < target_merge_level - 1:
                self.logger.warning("{}({}) not merged yet".format(doc_header['merge_key'],
                                                                   doc_header.get('merged_level', 0)),
                                    extra=self.log_context)
                return list()

            if doc_header.get('segment_start_age', doc_header['age']) >= current_start_age:
                self.logger.warning("Obsolete task found", extra=self.log_context)
                task_list.append({'ref': doc_ref,
                                  'start_age': doc_header.get('segment_start_age', doc_header['age']),
                                  'end_age': doc_header.get('end_age', doc_header['age']),
                                  'task_start_age': doc_header.get('segment_start_age', doc_header['age']),
                                  'task_end_age': current_start_age - 1,
                                  'merged': doc_header['merge_status'] == 'merged', 'size': doc_header['data_size']})
                continue
            current_end_age = doc_header.get('end_age', doc_header['age'])
            last_start_age = current_start_age
            current_start_age = doc_header.get('segment_start_age', doc_header['age'])
            if current_end_age < last_start_age - 1:
                self.logger.warning("GAP: doc-{} can't reach {}".format(doc_header['merge_key'],
                                                                        last_start_age), extra=self.log_context)
                return list()
            task_list.append({'ref': doc_ref, 'start_age': current_start_age, 'end_age': current_end_age,
                              'task_start_age': current_start_age, 'task_end_age': last_start_age - 1,
                              'merged': doc_header['merge_status'] == 'merged', 'size': doc_header['data_size']})

        if doc_header.get('merge_level', 0) < target_merge_level:
            self.logger.warning("No package data or header meeting", extra=self.log_context)
            return list()
        if doc_header.get('end_age', doc_header['age']) < current_start_age - 1:
            self.logger.warning("GAP: doc-{} can't reach {}".format(doc_header.get('merge_key', 'header'),
                                                                    current_start_age), extra=self.log_context)
            return list()
        return task_list

    def _get_normal_merge_task(self, leader_doc_ref: Any, leader_doc_dict: dict, target_merge_level: int) -> List[dict]:
        doc_header = {}
        current_start_time = leader_doc_dict.get('segment_start_time',
            leader_doc_dict.get('start_time', leader_doc_dict['deposit_at']))
        segment_end_time = leader_doc_dict['deposit_at']
        task_list = [{'ref': leader_doc_ref, 'start_time': current_start_time, 'end_time': segment_end_time,
                      'merged': leader_doc_dict['merge_status'] == 'merged', 'size': leader_doc_dict['data_size']}]
        for doc_ref in self.get_stream_by_sort_key(le_ge_key=leader_doc_dict['sort_key'],
                                                   reverse=True,
                                                   min_merge_level=(target_merge_level - 1),
                                                   equal=False):
            doc_header = self.get_header_from_ref(doc_ref)

            if doc_header['merge_level'] >= target_merge_level:
                self.logger.info("End of scope: Higher merge level reached", extra=self.log_context)
                break

            if doc_header.get('merged_level', 0) < target_merge_level - 1:
                self.logger.warning("{}({}) not merged yet".format(
                    doc_header['merge_key'], doc_header.get('merged_level', 0)), extra=self.log_context)
                return list()

            current_start_time = doc_header.get('segment_start_time',
                                                doc_header.get('start_time', doc_header['deposit_at']))
            current_end_time = doc_header['deposit_at']
            task_list.append({'ref': doc_ref, 'start_time': current_start_time, 'end_time': current_end_time,
                              'merged': doc_header['merge_status'] == 'merged', 'size': doc_header['data_size']})

        if doc_header.get('merge_level', 0) < target_merge_level:
            self.logger.warning("No package data or header meeting", extra=self.log_context)
            return list()
        return task_list

    def _aged_merge(self, merge_task: List[dict], target_merge_level: int) -> bool:
        segment_start_age = min([task['start_age'] for task in merge_task])
        over_size_flag = True if sum([task['size'] for task in merge_task]) >= self.size_limit else False
        has_merged_flag = True if any([task['merged'] for task in merge_task]) else False
        merge_flag = True if over_size_flag or has_merged_flag or target_merge_level == 7 else False
        task_list = sorted(merge_task, key=lambda x: x['task_end_age'], reverse=True)
        del_list = list()
        # Case 1: Simple no merge at all
        if not merge_flag:
            lead_doc = task_list[0]['ref']
            header = self.get_header_from_ref(lead_doc)
            header['age'] = segment_start_age
            header['merged_level'] = target_merge_level
            body_data = list()
            for task in task_list:
                if task['ref'] != lead_doc:
                    del_list.append(task['ref'])
                task_header = self.get_header_from_ref(task['ref'])
                task_data = self.get_data_from_header(task_header)
                body_data.extend([item for item in task_data
                                  if task['task_start_age'] <= item['_AGE'] <= task['task_end_age']])
            self.update_document(lead_doc, header, body_data)
            self.delete_documents(del_list)
            return True
        # Case 2 - Step 1: Merge everything who is self-oversized
        for task in [task for task in task_list if task['size'] >= (self.size_limit // 2) and not task['merged']]:
            header = self.get_header_from_ref(task['ref'])
            body_data = [item for item in self.get_data_from_header(header)
                              if task['task_start_age'] <= item['_AGE'] <= task['task_end_age']]
            header['merge_status'] = 'merged'
            header['age'] = task['task_start_age']
            header['end_age'] = task['task_end_age']
            task['merged'] = True
            updated_header = self.update_document(task['ref'], header, body_data)
            self.inc_table_header(merged_size=updated_header['data_size'], merged_lines=updated_header['line_nb'])
        # Case 2 - Step 2: Merge documents
        base_doc, total_size, header, body_data = None, 0, dict(), list()
        for task in task_list:
            if (total_size + task['size'] >= self.size_limit or task['merged']) and total_size > 0:
                updated_header = self.update_document(base_doc, header, body_data)
                self.inc_table_header(merged_size=updated_header['data_size'], merged_lines=updated_header['line_nb'])
                base_doc, total_size, header, body_data = None, 0, dict(), list()
            if task['merged']:
                continue
            if task['task_start_age'] > task['task_end_age']:
                del_list.append(task['ref'])
                continue
            task_header = self.get_header_from_ref(task['ref'])
            task_data = self.get_data_from_header(task_header)
            body_data.extend([item for item in task_data
                              if task['task_start_age'] <= item['_AGE'] <= task['task_end_age']])
            total_size += task['size']
            if base_doc is None:
                base_doc = task['ref']
                header = task_header.copy()
                header['merge_status'] = 'merged'
                header['end_age'] = task['task_end_age']
            else:
                del_list.append(task['ref'])
            header['age'] = task['task_start_age']
        if total_size > 0:
            updated_header = self.update_document(base_doc, header, body_data)
            self.inc_table_header(merged_size=updated_header['data_size'], merged_lines=updated_header['line_nb'])
        self.delete_documents(del_list)
        # Case 2 - Step 3: Update Lead Document
        lead_doc = task_list[0]['ref']
        header = {'segment_start_age': segment_start_age, 'merged_level': target_merge_level}
        self.update_document(lead_doc, header)
        return True

    def _normal_merge(self, merge_task: List[dict], target_merge_level: int) -> bool:
        segment_start_time = min([task['start_time'] for task in merge_task])
        over_size_flag = True if sum([task['size'] for task in merge_task]) >= self.size_limit else False
        has_merged_flag = True if any([task['merged'] for task in merge_task]) else False
        merge_flag = True if over_size_flag or has_merged_flag or target_merge_level == 7 else False
        task_list = sorted(merge_task, key=lambda x: x['end_time'], reverse=True)
        del_list = list()
        # Case 1: Simple no merge at all
        if not merge_flag:
            lead_doc = task_list[0]['ref']
            header = self.get_header_from_ref(lead_doc)
            header['start_time'] = segment_start_time
            header['merged_level'] = target_merge_level
            body_data = list()
            for task in task_list:
                if task['ref'] != lead_doc:
                    del_list.append(task['ref'])
                task_header = self.get_header_from_ref(task['ref'])
                task_data = self.get_data_from_header(task_header)
                body_data.extend(task_data)
            self.update_document(lead_doc, header, body_data)
            self.delete_documents(del_list)
            return True
        # Case 2 - Step 1: Merge everything who is self-oversized
        for task in [task for task in task_list if task['size'] >= (self.size_limit // 2) and not task['merged']]:
            header = self.get_header_from_ref(task['ref'])
            body_data = self.get_data_from_header(header)
            header['merge_status'] = 'merged'
            task['merged'] = True
            updated_header = self.update_document(task['ref'], header, body_data)
            self.inc_table_header(merged_size=updated_header['data_size'], merged_lines=updated_header['line_nb'])
        # Case 2 - Step 2: Merge documents
        base_doc, total_size, header, body_data = None, 0, dict(), list()
        for task in task_list:
            if (total_size + task['size'] >= self.size_limit or task['merged']) and total_size > 0:
                updated_header = self.update_document(base_doc, header, body_data)
                self.inc_table_header(merged_size=updated_header['data_size'], merged_lines=updated_header['line_nb'])
                base_doc, total_size, header, body_data = None, 0, dict(), list()
            if task['merged']:
                continue
            task_header = self.get_header_from_ref(task['ref'])
            task_data = self.get_data_from_header(task_header)
            body_data.extend(task_data)
            total_size += task['size']
            if base_doc is None:
                base_doc = task['ref']
                header = task_header.copy()
                header['merge_status'] = 'merged'
            else:
                del_list.append(task['ref'])
            header['start_time'] = task['start_time']
        if total_size > 0:
            updated_header = self.update_document(base_doc, header, body_data)
            self.inc_table_header(merged_size=updated_header['data_size'], merged_lines=updated_header['line_nb'])
        self.delete_documents(del_list)
        # Case 2 - Step 3: Update Lead Document
        lead_doc = task_list[0]['ref']
        header = {'segment_start_time': segment_start_time, 'merged_level': target_merge_level}
        self.update_document(lead_doc, header)
        return True
