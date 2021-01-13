import json
from typing import List, Union
from xialib.adaptor import FileAdaptor


class JsonAdaptor(FileAdaptor):
    """Adaptor for exporting json files

    Notes:
        Each dataset is ordered by sequence and will be seperated into two files: Delete File and Insert File.
        The correction reconstruction order is: Seq-1-D -> Seq-1-I -> Seq-2-D -> Seq-2-I

    """
    def insert_raw_data(self, log_table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        check_d, check_i = dict(), dict()
        file_name = self._get_file_name(data)
        key_list = [item['field_name'] for item in field_data if item['key_flag']]
        if not self.storer.exists(self.storer.join(self.location, log_table_id)):
            self.storer.mkdir(self.storer.join(self.location, log_table_id))  # pragma: no cover
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
            d_file_name = self.storer.join(self.location, log_table_id, file_name + '-D.json')
            d_content = [{key: value for key, value in zip(key_list, line)} for line in check_d]
            self.storer.write(json.dumps(d_content, ensure_ascii=False).encode(), d_file_name)
        if check_i:
            i_file_name = self.storer.join(self.location, log_table_id, file_name + '-I.json')
            i_content = json.dumps([value for key, value in check_i.items()], ensure_ascii=False).encode()
            self.storer.write(i_content, i_file_name)
        return True
