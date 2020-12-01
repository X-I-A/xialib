import abc
import json
import os
import base64
import logging
from collections import Counter
from typing import List, Dict, Tuple, Union

__all__ = ['Adaptor']


class Adaptor(metaclass=abc.ABCMeta):
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

    @abc.abstractmethod
    def insert_raw_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs) -> bool:
        """ To be implemented Public function

        This function will insert x-i-a spec data into the database without any modification

        Args:
            table_id (:obj:`str`): Table ID
            field_data (:obj:`list` of `dict`): Table field description
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format (spec x-i-a)

        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def load_raw_data(self, raw_table_id: str, tar_table_id: str, field_data: List[dict]):
        """ Public function

        This function will load the data saved in raw table into target table

        Args:
            raw_table_id (:obj:`str`): Raw Table ID
            tar_table_id (:obj:`str`): Raw Table ID
            field_data (:obj:`list` of `dict`): Table field description
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

        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def create_table(self, table_id: str, meta_data: dict, field_data: List[dict]):
        """Public Function

        Create a table with information provided by header message with specification x-i-a

        Args:
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table
            meta_data (:obj:`dict`): Table related meta-data, such as Table description
            field_data (:obj:`list` of `dict`): Table field description
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def drop_table(self, table_id: str):
        """Public Function

        Drop table

        Args:
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def rename_table(self, old_table_id: str, new_table_id: str):
        """Public Function

        Rename table from old name into new table

        Args:
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table
        """
        raise NotImplementedError  # pragma: no cover

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
    obs_insert_sql = ("(IFNULL(r._SEQ, '') || IFNULL(r._AGE, '') || IFNULL(r._SEQ, '')) > "
                      "(IFNULL(t._SEQ, '') || IFNULL(t._AGE, '') || IFNULL(t._SEQ, '')) "
                      "AND r._OP in ('U', 'D')")
    # Variable Name: @ori_table_name@, @raw_table_name@, @key_eq_key@
    load_del_sql_template = ("DELETE FROM {} WHERE EXISTS ( "
                             "SELECT * FROM {} WHERE {} AND _OP in ( 'U', 'D' ) )")
    # Variable Name: @ori_table_name@, @field_list@, @raw_table_name@, @raw_table_name@, @obs_insert_sql@
    load_ins_sql_template = ("INSERT INTO {} "
                             "SELECT {} FROM {} t WHERE NOT EXISTS ( "
                             "SELECT * FROM {} r WHERE {} )")


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

    def drop_table(self, table_id: str):
        cur = self.connection.cursor()
        sql = self._get_drop_sql(table_id)
        cur.execute(sql)

    def create_table(self, table_id: str, meta_data: dict, field_data: List[dict], raw_flag: bool = False):
        field_list = field_data.copy()
        if raw_flag:
            field_list.append({'field_name': '_AGE', 'key_flag': False, 'type_chain': ['int', 'ui_8'],
                              'format': None, 'encode': None})
            field_list.append({'field_name': '_SEQ', 'key_flag': False, 'type_chain': ['char', 'c_8'],
                              'format': None, 'encode': None})
            field_list.append({'field_name': '_NO', 'key_flag': False, 'type_chain': ['int', 'ui_8'],
                              'format': None, 'encode': None})
            field_list.append({'field_name': '_OP', 'key_flag': False, 'type_chain': ['char', 'c_1'],
                              'format': None, 'encode': None})
        cur = self.connection.cursor()
        sql = self._get_create_sql(table_id, meta_data, field_list)
        cur.execute(sql)

    def rename_table(self, old_table_id: str, new_table_id: str):
        rename_sql = self.rename_sql_template.format(self._sql_safe(self._get_table_name(old_table_id)),
                                                     self._sql_safe(self._get_table_name(new_table_id)))
        cur = self.connection.cursor()
        cur.execute(rename_sql)

    def insert_raw_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        raw_field = field_data.copy()
        raw_field.append({'field_name': '_AGE'})
        raw_field.append({'field_name': '_SEQ'})
        raw_field.append({'field_name': '_NO'})
        raw_field.append({'field_name': '_OP'})
        cur = self.connection.cursor()
        sql = self._get_insert_sql(table_id, raw_field)
        values = self._get_value_tuples(data, raw_field)
        cur.executemany(sql, values)

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
        cur.executemany(del_sql, del_vals)
        # Insert Mode : Case simple : Append mode
        if len(del_data) == 0:
            ins_values = self._get_value_tuples(data, field_data)
            cur.executemany(ins_sql, ins_values)
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
            cur.executemany(ins_sql, ins_values)

    def load_raw_data(self, raw_table_id: str, tar_table_id: str, field_data: List[dict]):
        cur = self.connection.cursor()
        load_del_sql = self._get_load_del_sql(raw_table_id, tar_table_id, field_data)
        cur.execute(load_del_sql)
        load_ins_sql = self._get_load_ins_sql(raw_table_id, tar_table_id, field_data)
        cur.execute(load_ins_sql)

    def _get_key_list(self, field_data: List[dict]) -> str:
        key_list = ", ".join(['"' + field['field_name'] + '"' for field in field_data if field['key_flag']])
        return key_list

    def _get_field_list(self, field_data: List[dict]) -> str:
        field_list = ", ".join(['"' + field['field_name'] + '"' for field in field_data])
        return field_list

    def _get_table_name(self, table_id: str) -> str:
        sysid, db_name, schema, table_name = table_id.split('.')
        table_name = schema + '.' + table_name if schema else table_name
        return table_name

    def _get_field_type(self, type_chain: list):
        for type in reversed(type_chain):
            for key, value in self.type_dict.items():
                if type in value:
                    return key
        return 'OBJECT'  # pragma: no cover

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

    def _get_create_sql(self, table_id: str, meta_data: dict, field_data: List[dict]):
        return self.create_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_field_types(field_data)),
                                               self._sql_safe(self._get_key_list(field_data)))

    def _get_drop_sql(self, table_id: str):
        return self.drop_sql_template.format(self._sql_safe(self._get_table_name(table_id)))

    @abc.abstractmethod
    def _get_insert_sql(self, table_id: str, field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_delete_sql(self, table_id: str, key_field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    def _get_load_del_sql(self, raw_table_id: str, tar_table_id: str, field_data: List[dict]):
        raw_table_name = self._get_table_name(raw_table_id)
        tar_table_name = self._get_table_name(tar_table_id)
        return self.load_del_sql_template.format(self._sql_safe(tar_table_name),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(self._get_key_eq_key(field_data,
                                                                                     tar_table_name,
                                                                                     raw_table_name)))

    def _get_load_ins_sql(self, raw_table_id: str, tar_table_id: str, field_data: List[dict]):
        raw_table_name = self._get_table_name(raw_table_id)
        tar_table_name = self._get_table_name(tar_table_id)
        return self.load_ins_sql_template.format(self._sql_safe(tar_table_name),
                                                 self._sql_safe(self._get_field_list(field_data)),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(self.obs_insert_sql))

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
            value_tuples.append(tuple([line.get(field['field_name'], None) for field in field_data]))
        return value_tuples

    def _get_insert_sql(self, table_id: str, field_data: List[dict]):
        return self.insert_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_value_holders(field_data)))

    def _get_delete_sql(self, table_id: str, key_field_data: List[dict]):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_where_key_holders(key_field_data)))
