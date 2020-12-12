import abc
import json
import os
import base64
import gzip
import logging
from collections import Counter
from typing import List, Dict, Tuple, Union

__all__ = ['Adaptor']


class Adaptor(metaclass=abc.ABCMeta):
    # Constant Definition
    FLEXIBLE_FIELDS = [{}]

    # Standard field definition
    _age_field = {'field_name': '_AGE', 'key_flag': True, 'type_chain': ['int', 'ui_8'],
                  'format': None, 'encode': None, 'default': 0}
    _seq_field = {'field_name': '_SEQ', 'key_flag': True, 'type_chain': ['char', 'c_20'],
                  'format': None, 'encode': None, 'default': '0'*20}
    _no_field = {'field_name': '_NO', 'key_flag': True, 'type_chain': ['int', 'ui_8'],
                 'format': None, 'encode': None, 'default': 0}
    _op_field = {'field_name': '_OP', 'key_flag': False, 'type_chain': ['char', 'c_1'],
                 'format': None, 'encode': None, 'default': ''}

    # Ctrl Table definition
    _ctrl_table_id = '...X_I_A_C_T_R_L'
    _ctrl_table = [
        {'field_name': 'SOURCE_ID', 'key_flag': True, 'type_chain': ['char', 'c_255']},
        {'field_name': 'START_SEQ', 'key_flag': False, 'type_chain': ['char', 'c_20']},
        {'field_name': 'TABLE_ID', 'key_flag': False, 'type_chain': ['char', 'c_255']},
        {'field_name': 'LOG_TABLE_ID', 'key_flag': False, 'type_chain': ['char', 'c_255']},
        {'field_name': 'LOADED_KEY', 'key_flag': False, 'type_chain': ['char', 'c_20']},
        {'field_name': 'SAFE_END_FLAG', 'key_flag': False, 'type_chain': ['int', 'bool']},
        {'field_name': 'META_DATA', 'key_flag': False, 'type_chain': ['char', 'c_5000']},
        {'field_name': 'AGE_LIST', 'key_flag': False, 'type_chain': ['char', 'c_1000000']},
        {'field_name': 'FIELD_LIST', 'key_flag': False, 'type_chain': ['char', 'c_1000000']},
    ]

    def __init__(self, **kwargs):
        """Adaptor for loading databases

        """
        self.logger = logging.getLogger("XIA.Adaptor")
        self.log_context = {'context': ''}
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          '%(context)s:%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def _meta_data_to_string(self, meta_data: dict) -> str:
        meta_str = base64.b64encode(gzip.compress(json.dumps(meta_data, ensure_ascii=False).encode())).decode()
        return meta_str

    def _string_to_meta_data(self, meta_str: str) -> dict:
        meta_data = json.loads(gzip.decompress(base64.b64decode(meta_str.encode())).decode())
        return meta_data

    def _field_data_to_string(self, field_data: List[dict]) -> str:
        xia_f = [{key: value for key, value in field.items() if not key.startswith('_')} for field in field_data]
        field_str = base64.b64encode(gzip.compress(json.dumps(xia_f, ensure_ascii=False).encode())).decode()
        return field_str

    def _string_to_field_data(self, field_str: str) -> List[dict]:
        field_data = json.loads(gzip.decompress(base64.b64decode(field_str.encode())).decode())
        return field_data

    @abc.abstractmethod
    def get_ctrl_info(self, source_id) -> dict:
        """ To be implemented Public function

        This function will get the ctrl table information

        Args:
            source_id (:obj:`str`): Data source ID

        Return:
            Ctrl line dictionary
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def set_ctrl_info(self, source_id: str, **kwargs) -> bool:
        """ To be implemented Public function

        This function will set the ctrl table information

        Warning:
            The update must be synchronous to the current node.

        Args:
            source_id (:obj:`str`): Data source ID

        Return:
            Ctrl line dictionary
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def get_log_table_id(self, source_id: str) -> str:
        """ To be implemented Public function

        This function will return the log table id of a given source_id. Should be unique and doesn't exist yet

        Args:
            source_id (:obj:`str`): Data source ID

        Return:
            log table id
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def insert_raw_data(self, log_table_id: str, field_data: List[dict], data: List[dict], **kwargs) -> bool:
        """ To be implemented Public function

        This function will insert x-i-a spec data into the database without any modification. Two main usage:
        1. log insert. 2. Fast table load

        Args:
            table_id (:obj:`str`): Log Table ID
            field_data (:obj:`list` of `dict`): Table field description
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format (spec x-i-a)

        Return:
            True if successful False in the other case

        Warning:
            Same entry might be sent more than once, implementation must take this point into account
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def load_log_data(self, source_id: str, start_age: int = None, end_age: int = None) -> bool:
        """ Public function

        This function will load the data saved in raw table (log usage) into target table

        Args:
            source_id (:obj:`str`): Source Table ID
            start_age (:obj:`int`): Start Age
            end_age (:obj:`int`): End Age

        Return:
            True if successful False in the other case

        Notes:

       """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def upsert_data(self,
                    table_id: str,
                    field_data: List[dict],
                    data: List[dict],
                    replay_safe: bool = False,
                    **kwargs) -> bool:
        """ Public function

        This function will get the pushed data and save it to the target database

        Args:
            table_id (:obj:`str`): Table ID
            field_data (:obj:`list` of `dict`): Table field description
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format (spec x-i-a)
            replay_safe (:obj:`bool`): Try to delete everything before


        Return:
            True if successful False in the other case
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def create_table(self, source_id: str, start_seq: str, meta_data: dict, field_data: List[dict],
                     raw_flag: bool = False, table_id: str = None) -> bool:
        """Public Function

        Create a table with information provided by header message with specification x-i-a

        Args:
            source_id (:obj:`str`): Source Table ID with format sysid.dbid.schema.table
            start_seq (:obj:`str`): Start sequence ID
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table
            meta_data (:obj:`dict`): Table related meta-data, such as Table description
            field_data (:obj:`list` of `dict`): Table field description
            raw_flag (:obj:`bool`): If the table contains internal fields (_AGE, _SEG, _NO, _OP)

        Return:
            True if successful False in the other case
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def drop_table(self, source_id: str) -> bool:
        """Public Function

        Drop table

        Args:
            source_id (:obj:`str`): Source Table ID with format sysid.dbid.schema.table

        Return:
            True if successful False in the other case
       """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def rename_table(self, source_id: str, new_table_id: str):
        """Public Function

        Rename table from old name into new table

        Args:
            source_id (:obj:`str`): Source Table ID with format sysid.dbid.schema.table
            new_table_id (:obj:`str`): New Table ID with format sysid.dbid.schema.table

        Return:
            True if successful False in the other case
       """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def alter_column(self, table_id: str, field_line: dict ) -> bool:
        """Public Function

        Changing size of an existed column. (if supported by database)

        Args:
            table_id (:obj:`str`): Table ID
            field_line (:obj:`dict`): field to be extended

        Return:
            True if success False in the other case

        Notes:
            If the database support dynamic column size, just implement the methode with pass keyword
        """
        return False # pragma: no cover

class DbapiAdaptor(Adaptor):
    """Adaptor for databases supporting PEP249

    Attributes:
        type_dict (:obj:`dict`): field type translator
        create_sql_template (:obj:`str`): create table
        drop_sql_template (:obj:`str`): drop table
        insert_sql_template (:obj:`str`): insert table
        delete_sql_template (:obj:`str`): delete table
        connection (:obj:`Connection`): Connection object defined in PEP249
    """

    type_dict = {}

    # Variable Name: @table_name@, @field_types@, @key_list@
    create_sql_template = "CREATE TABLE {} ( {}, PRIMARY KEY( {} ))"
    # Variable Name: @table_name@
    drop_sql_template = "DROP TABLE {}"
    # Variable Name: @table_name@, @value_holders@
    insert_sql_template = "INSERT INTO {} VALUES ( {} )"
    # Variable Name: @table_name@, @where_key_holders@
    delete_sql_template = "DELETE FROM {} WHERE {}"
    # Variable Name: @old_table_name@, @new_table_name@
    rename_sql_template = "ALTER TABLE {} RENAME TO {}"
    obs_insert_sql = ("(IFNULL(r._SEQ, '00000000000000000000') || "
                      "SUBSTR('0000000000' || IFNULL(r._AGE, ''), -10, 10) || "
                      "SUBSTR('0000000000' || IFNULL(r._NO, ''), -10, 10)) > "
                      "(IFNULL(t._SEQ, '00000000000000000000') || "
                      "SUBSTR('0000000000' || IFNULL(t._AGE, ''), -10, 10) || "
                      "SUBSTR('0000000000' || IFNULL(t._NO, ''), -10, 10)) "
                      "AND r._OP in ('U', 'D')")
    # Variable Name: @ori_table_name@, @raw_table_name@, @key_eq_key@, @age_range
    load_del_sql_template = ("DELETE FROM {} WHERE EXISTS ( "
                             "SELECT * FROM {} WHERE {} AND {} AND _OP in ( 'U', 'D' ) )")
    # Variable Name: @ori_table_name@,
    # @field_list@, @raw_table_name@,
    # @raw_table_name@, @obs_insert_sql@, @key_eq_key@, @age_range
    load_ins_sql_template = ("INSERT INTO {} "
                             "SELECT {} FROM {} t WHERE NOT EXISTS ( "
                             "SELECT * FROM {} r WHERE {} AND {} ) AND {} AND t._OP != 'D'")
    # Variable Name: @raw_table_name@, @old_age_range
    remove_old_raw_sql_template = "DELETE FROM {} WHERE {}"
    # Variable Name: @ctrl_table_name@, @table_name@
    select_from_ctrl_template = ("SELECT * FROM {} WHERE TABLE_ID = {}")

    def __init__(self, connection, **kwargs):
        super().__init__(**kwargs)
        # Duck type check
        if any([not hasattr(connection, method) for method in ['cursor', 'close', 'commit']]):
            self.logger.error("connection must an Connection defined by PEP249", extra=self.log_context)
            raise TypeError("XIA-000019")
        else:
            self.connection = connection

    def _sql_safe(self, input: str) -> str:
        return input.replace(';', '')

    def get_ctrl_info(self, source_id):
        cur = self.connection.cursor()
        sql = self._get_ctrl_info_sql(self._ctrl_table_id)
        try:
            cur.execute(sql, (source_id, ))
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return dict()  # pragma: no cover
        return_line = {'SOURCE_ID': source_id, 'TABLE_ID': source_id,}
        fetch_result = cur.fetchone()
        if fetch_result is not None:
            sql_result  = list(fetch_result)
            key_list = [item['field_name'] for item in self._ctrl_table]
            return_line = {key: value for key, value in zip(key_list, sql_result)}
        if return_line['SOURCE_ID'] != source_id:  # pragma: no cover
            self.logger.error("Ctrl Table: {} != {}".format(return_line['SOURCE_ID'],
                                                            source_id), extra=self.log_context)  # pragma: no cover
            raise ValueError("XIA-000021")  # pragma: no cover
        if return_line.get('META_DATA', None) is not None:
            return_line['META_DATA'] = self._string_to_meta_data(return_line.get('META_DATA'))
        if return_line.get('FIELD_LIST', None) is not None:
            return_line['FIELD_LIST'] = self._string_to_field_data(return_line['FIELD_LIST'])
        return return_line

    def set_ctrl_info(self, source_id, **kwargs) -> bool:
        old_ctrl_info = self.get_ctrl_info(source_id)
        new_ctrl_info = old_ctrl_info.copy()
        if new_ctrl_info.get('META_DATA', None) is not None:
            new_ctrl_info['META_DATA'] = self._meta_data_to_string(new_ctrl_info['META_DATA'])
        if new_ctrl_info.get('FIELD_LIST', None) is not None:
            new_ctrl_info['FIELD_LIST'] = self._field_data_to_string(new_ctrl_info['FIELD_LIST'])
        key_list = [item['field_name'] for item in self._ctrl_table if item['field_name'].lower() in kwargs]
        for key in key_list:
            if key == 'META_DATA':
                new_ctrl_info[key] = self._meta_data_to_string(kwargs[key.lower()])
            elif key == 'FIELD_LIST':
                new_ctrl_info[key] = self._field_data_to_string(kwargs[key.lower()])
            elif key != 'SOURCE_ID':
                new_ctrl_info[key] = kwargs[key.lower()]
        return self.upsert_data(self._ctrl_table_id, self._ctrl_table, [new_ctrl_info], True)

    def drop_table(self, source_id: str):
        table_info = self.get_ctrl_info(source_id)
        table_id = table_info['TABLE_ID']
        log_table_id = table_info.get('LOG_TABLE_ID', '')
        cur = self.connection.cursor()
        sql = self._get_drop_sql(table_id)
        try:
            if log_table_id:
                log_drop_sql = self._get_drop_sql(log_table_id)
                cur.execute(log_drop_sql)
            cur.execute(sql)
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover
        return self.upsert_data(self._ctrl_table_id, self._ctrl_table, [{'SOURCE_ID': source_id}], True)

    def create_table(self, source_id: str, start_seq: str, meta_data: dict, field_data: List[dict],
                     raw_flag: bool = False, table_id: str = None):
        field_list = field_data.copy()
        if table_id is None:
            table_id = source_id
        cur = self.connection.cursor()
        sql = self._get_create_sql(table_id, meta_data, field_list, raw_flag)
        try:
            cur.execute(sql)
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

        if raw_flag or table_id == self._ctrl_table_id:
            return True

        log_table_id = self.get_log_table_id(source_id)
        # If no need to create log table, the adaptor can return a None object
        if log_table_id is None:
            return True  # pragma: no cover
        if not self.create_table(source_id, start_seq, meta_data, field_data, True, log_table_id):
            self.logger.error("Log table creation Error: {}".format(log_table_id),
                              extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover
        return self.set_ctrl_info(source_id, table_id=table_id, meta_data=meta_data, field_list=field_data,
                                  start_seq=start_seq, log_table_id=log_table_id)

    def rename_table(self, source_id: str, new_table_id: str):
        table_info = self.get_ctrl_info(source_id)
        table_param = {key.lower(): value for key, value in table_info.items() if key.lower() != 'source_id'}
        old_table_id = table_param['table_id']
        sql = self.rename_sql_template.format(self._sql_safe(self._get_table_name(old_table_id)),
                                              self._sql_safe(self._get_table_name(new_table_id)))
        cur = self.connection.cursor()
        try:
            cur.execute(sql)
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e))  # pragma: no cover
            return False  # pragma: no cover
        table_param['TABLE_ID'] = new_table_id
        return self.set_ctrl_info(source_id, **table_param)

    def insert_raw_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        raw_field = field_data.copy()
        raw_field.append(self._age_field)
        raw_field.append(self._seq_field)
        raw_field.append(self._no_field)
        raw_field.append(self._op_field)
        cur = self.connection.cursor()
        sql = self._get_insert_sql(table_id, raw_field)
        values = self._get_value_tuples(data, raw_field)
        try:
            cur.executemany(sql, values)
            self.connection.commit()
            return True
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

    def upsert_data(self,
                    table_id: str,
                    field_data: List[dict],
                    data: List[dict],
                    replay_safe: bool = False,
                    **kwargs):
        cur = self.connection.cursor()
        key_list = [item for item in field_data if item['key_flag']]
        del_sql = self._get_delete_sql(table_id, key_list)
        ins_sql = self._get_insert_sql(table_id, field_data)
        del_data = [item for item in data if item.get('_OP', '') in ['U', 'D']]
        # Everything is equal to : Delete + Insert: Delete at first
        del_vals = self._get_value_tuples(data, key_list) if replay_safe else self._get_value_tuples(del_data, key_list)
        if len(del_vals) > 0:
            try:
                cur.executemany(del_sql, del_vals)
                self.connection.commit()
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        # Insert Mode : Case simple : Append mode
        if len(del_data) == 0:
            ins_values = self._get_value_tuples(data, field_data)
            try:
                cur.executemany(ins_sql, ins_values)
                self.connection.commit()
                return True
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        # Case standard: mark obsoleted inserts and than insert
        else:
            cur_del_set = set()
            for line in reversed(data):
                key_tuple = tuple([line.get(field['field_name'], None) for field in key_list])
                if key_tuple in cur_del_set:
                    line['_OP'] = 'D'
                elif line.get('_OP', '') in ['U', 'D']:
                    cur_del_set.add(key_tuple)
            ins_values = self._get_value_tuples([item for item in data if item.get('_OP', '') != 'D'], field_data)
            try:
                cur.executemany(ins_sql, ins_values)
                self.connection.commit()
                return True
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover

    def load_log_data(self, source_id: str, start_age: int = None, end_age: int = None):
        table_info = self.get_ctrl_info(source_id)
        raw_table_id = table_info['LOG_TABLE_ID']
        tar_table_id = table_info['TABLE_ID']
        field_data = table_info['FIELD_LIST']

        cur = self.connection.cursor()
        load_del_sql = self._get_load_del_sql(raw_table_id, tar_table_id, field_data, start_age, end_age)
        try:
            cur.execute(load_del_sql)
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover
        load_ins_sql = self._get_load_ins_sql(raw_table_id, tar_table_id, field_data, start_age, end_age)
        try:
            cur.execute(load_ins_sql)
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover
        remove_old_raw_sql = self._get_remove_old_raw_sql(raw_table_id, start_age, end_age)
        try:
            cur.execute(remove_old_raw_sql)
            self.connection.commit()
            return True
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

    def get_log_table_id(self, source_id: str):
        sysid, db, schema, table = source_id.split('.')
        return '.'.join([sysid, db, schema, "XIA_" + table])

    def _get_key_list(self, field_data: List[dict]) -> str:
        key_list = ", ".join(['"' + field['field_name'] + '"' for field in field_data if field['key_flag']])
        return key_list

    def _get_field_list(self, field_data: List[dict]) -> str:
        field_list = ", ".join(['"' + field['field_name'] + '"' for field in field_data])
        return field_list

    def _get_table_name(self, table_id: str) -> str:
        sysid, db_name, schema, table_name = table_id.split('.')
        table_name = '"' + schema + '"."' + table_name + '"' if schema else '"' + table_name + '"'
        return table_name

    @abc.abstractmethod
    def _get_field_type(self, type_chain: list):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_field_types(self, field_data: List[dict]) -> str:
        """To be implemented Function

        Create table fields definitions,

        Args:
            field_data (:obj:`list` of `dict`): Table field description

        Returns:
            field defintion string: FIELD_A Integer, FIELD_B String, ...
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_value_holders(self, field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_where_key_holders(self, field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_key_eq_key(self, field_data: List[dict], alias1: str, alias2: str):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_value_tuples(self, data_list: List[dict], field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    def _get_create_sql(self, table_id: str, meta_data: dict, field_data: List[dict], raw_flag: bool):
        field_list =field_data.copy()
        if raw_flag:
            field_list.append(self._age_field)
            field_list.append(self._seq_field)
            field_list.append(self._no_field)
            field_list.append(self._op_field)
        return self.create_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_field_types(field_list)),
                                               self._sql_safe(self._get_key_list(field_list)))

    def _get_drop_sql(self, table_id: str):
        return self.drop_sql_template.format(self._sql_safe(self._get_table_name(table_id)))

    @abc.abstractmethod
    def _get_insert_sql(self, table_id: str, field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_delete_sql(self, table_id: str, key_field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_ctrl_info_sql(self, source_id: str):
        raise NotImplementedError  # pragma: no cover

    def _get_age_range_condition(self, start_age: int = None, end_age: int = None) -> str:
        if start_age is None and end_age is None:
            return "1 = 1"
        elif end_age is None:
            return "_AGE >= {}".format(start_age)
        elif start_age is None:
            return "_AGE <= {}".format(end_age)
        else:
            return "_AGE >= {} AND _AGE <= {}".format(start_age, end_age)

    def _get_old_age_condition(self, end_age: int = None) -> str:
        return "1 > 1" if end_age is None else "_AGE <= {}".format(end_age)

    def _get_load_del_sql(self, raw_table_id: str, tar_table_id: str, field_data: List[dict],
                          start_age: int = None, end_age: int = None):
        raw_table_name = self._get_table_name(raw_table_id)
        tar_table_name = self._get_table_name(tar_table_id)
        return self.load_del_sql_template.format(self._sql_safe(tar_table_name),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(self._get_key_eq_key(field_data,
                                                                                     tar_table_name,
                                                                                     raw_table_name)),
                                                 self._sql_safe(self._get_age_range_condition(start_age,
                                                                                              end_age)))

    def _get_load_ins_sql(self, raw_table_id: str, tar_table_id: str, field_data: List[dict],
                          start_age: int = None, end_age: int = None):
        raw_table_name = self._get_table_name(raw_table_id)
        tar_table_name = self._get_table_name(tar_table_id)
        return self.load_ins_sql_template.format(self._sql_safe(tar_table_name),
                                                 self._sql_safe(self._get_field_list(field_data)),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(self.obs_insert_sql),
                                                 self._sql_safe(self._get_key_eq_key(field_data,
                                                                                     't',
                                                                                     'r')),
                                                 self._sql_safe(self._get_age_range_condition(start_age,
                                                                                              end_age)))

    def _get_remove_old_raw_sql(self, raw_table_id: str, start_age: int = None, end_age: int = None):
        raw_table_name = self._get_table_name(raw_table_id)
        return self.remove_old_raw_sql_template.format(self._sql_safe(raw_table_name),
                                                       self._sql_safe(self._get_old_age_condition(end_age)))

class DbapiQmarkAdaptor(DbapiAdaptor):
    """Adaptor for databases supporting PEP 249 with paramstyple qmark

    """
    def _get_value_holders(self, field_data: List[dict]):
        value_holders = ', '.join(['?' for field in field_data])
        return value_holders

    def _get_where_key_holders(self, field_data: List[dict]):
        where_key_holders = ' AND '.join(['"' + field['field_name'] + '" = ?' for field in field_data])
        return where_key_holders

    def _get_key_eq_key(self, field_data: List[dict], alias1: str, alias2: str):
        where_key_holders = ' AND '.join([alias1 + '."' + field['field_name'] + '" = ' +
                                          alias2 + '."' + field['field_name'] + '"'
                                            for field in field_data if field['key_flag']])
        return where_key_holders

    def _get_value_tuples(self, data_list: List[dict], field_data: List[dict]):
        value_tuples = list()
        for line in data_list:
            value_tuples.append(tuple([line.get(field['field_name'], field.get('default', None))
                                       for field in field_data]))
        return value_tuples

    def _get_insert_sql(self, table_id: str, field_data: List[dict]):
        return self.insert_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_value_holders(field_data)))

    def _get_delete_sql(self, table_id: str, key_field_data: List[dict]):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_where_key_holders(key_field_data)))

    def _get_ctrl_info_sql(self, source_id: str):
        return self.select_from_ctrl_template.format(self._sql_safe(self._get_table_name(self._ctrl_table_id)),
                                                     '?')