import json
from functools import reduce
from typing import List, Union
from xialib.adaptor import FileAdaptor


class JsonAdaptor(FileAdaptor):
    """Adaptor for exporting json files

    Notes:
        Each dataset is ordered by sequence and will be seperated into two files: Delete File and Insert File.
        The correction reconstruction order is: Seq-1-D -> Seq-1-I -> Seq-2-D -> Seq-2-I

    """
    def get_ctrl_info(self, table_id: str, segment_id: str = ''):
        return {'TABLE_ID': table_id, 'SEGMENT_ID': segment_id, "LOG_TABLE_ID": table_id}

    def insert_raw_data(self, log_table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        check_d, check_i = dict(), dict()
        file_name = self._get_file_name(data)
        if field_data:
            key_list = [item['field_name'] for item in field_data if item['key_flag']]
        else:
            all_fields = reduce(lambda x, y: x | y, [set(line) for line in data])
            key_list = [field for field in all_fields if not field.startswith("_")]
        log_table_path = [i if i else "default" for i in log_table_id.split(".")]
        if not self.storer.exists(self.storer.join(self.location, *log_table_path)):
            self.storer.mkdir(self.storer.join(self.location, *log_table_path))  # pragma: no cover
        data = sorted(data,
                      key = lambda k: (k.get('_AGE', None), k.get('_SEQ', None), k.get('_NO', None)),
                      reverse=True)
        for i in range(len(data)):
            line = data.pop()
            key_descr = self._get_key_from_line(key_list, line)
            if line.get('_OP', 'I') == 'I':
                line.pop('_OP', None)
                line.pop('_SEQ', None)
                line.pop('_AGE', None)
                line.pop('_NO', None)
                check_i[key_descr] = line
                check_d.pop(key_descr, None)
            elif line.get('_OP', 'I') == 'D':
                check_d[key_descr] = ''
                check_i.pop(key_descr, None)
            elif line.get('_OP', 'I') == 'U':
                line.pop('_OP', None)
                line.pop('_SEQ', None)
                line.pop('_AGE', None)
                line.pop('_NO', None)
                check_i[key_descr] = line
                check_d[key_descr] = ''
        if check_d:
            d_file_name = self.storer.join(self.location, *log_table_path, file_name + '-D.json')
            d_content = [{key: value for key, value in zip(key_list, line)} for line in check_d]
            self.storer.write(json.dumps(d_content, ensure_ascii=False).encode(), d_file_name)
        if check_i:
            i_file_name = self.storer.join(self.location, *log_table_path, file_name + '-I.json')
            i_content = json.dumps([value for key, value in check_i.items()], ensure_ascii=False).encode()
            self.storer.write(i_content, i_file_name)
        return True
