import abc
import json
import os
import uuid
import base64
import gzip
import logging
from collections import Counter
from xialib.flowers.segment_flower import SegmentFlower
from xialib.storer import RWStorer
from typing import List, Dict, Tuple, Union

__all__ = ['Adaptor', 'DbapiAdaptor', 'DbapiQmarkAdaptor', 'FileAdaptor']


class Adaptor(metaclass=abc.ABCMeta):
    # Constant Definition
    FLEXIBLE_FIELDS = [{}]

    # Some basic configuration
    support_add_column = True
    support_alter_column = True

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
        {'field_name': 'TABLE_ID', 'key_flag': True, 'type_chain': ['char', 'c_255']},
        {'field_name': 'SEGMENT_ID', 'key_flag': True, 'type_chain': ['char', 'c_255']},
        {'field_name': 'SOURCE_ID', 'key_flag': False, 'type_chain': ['char', 'c_255']},
        {'field_name': 'START_SEQ', 'key_flag': False, 'type_chain': ['char', 'c_20']},
        {'field_name': 'LOG_TABLE_ID', 'key_flag': False, 'type_chain': ['char', 'c_255']},
        {'field_name': 'META_DATA', 'key_flag': False, 'type_chain': ['char', 'c_5000']},
        {'field_name': 'FIELD_LIST', 'key_flag': False, 'type_chain': ['char', 'c_1000000']},
    ]

    # Log Table defintion
    _ctrl_log_id = '...X_I_A_L_O_G'
    _ctrl_log_table = [
        {'field_name': 'TABLE_ID', 'key_flag': True, 'type_chain': ['char', 'c_255']},
        {'field_name': 'SEGMENT_ID', 'key_flag': True, 'type_chain': ['char', 'c_255']},
        {'field_name': 'START_AGE', 'key_flag': True, 'type_chain': ['int', 'i_8']},
        {'field_name': 'END_AGE', 'key_flag': True, 'type_chain': ['int', 'i_8']},
        {'field_name': 'LOADED_FLAG', 'key_flag': False, 'type_chain': ['char', 'c_1']},
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
    def get_ctrl_info_list(self, table_id: str) -> List[dict]:
        """ To be implemented Public function

        This function will get the ctrl table information. Could return multiple line in the case of segment

        Args:
            table_id (:obj:`str`): Table ID

        Return:
            List of Ctrl line dictionary
        """
        raise NotImplementedError  # pragma: no cover

    def get_ctrl_info(self, table_id: str, segment_id: str = '') -> dict:
        """ To be implemented Public function

        This function will get the ctrl table information. Could return multiple line in the case of segment

        Args:
            table_id (:obj:`str`): Table ID

        Return:
            List of Ctrl line dictionary
        """
        ctrl_info_list = [ctr for ctr in self.get_ctrl_info_list(table_id) if ctr.get('SEGMENT_ID', '') == segment_id]
        if not ctrl_info_list:
            ctrl_info_line = {'TABLE_ID': table_id, 'SEGMENT_ID': segment_id}
        else:
            ctrl_info_line = ctrl_info_list[0]
        return ctrl_info_line

    @abc.abstractmethod
    def set_ctrl_info(self, table_id: str, segment_id: str, **kwargs) -> bool:
        """ To be implemented Public function

        This function will set the ctrl table information

        Warning:
            The update must be synchronous to the current node.

        Args:
            table_id (:obj:`str`): Table ID
            segment_id (:obj:`str`): Table Segment ID

        Return:
            True if operation is successful False otherwise
        """
        raise NotImplementedError  # pragma: no cover

    def get_log_table_id(self, table_id: str, segment_id: str) -> str:
        """ Public function

        This function will return the log table id of a given source_id. Should be unique and doesn't exist yet

        Args:
            table_id (:obj:`str`): Table ID
            segment_id (:obj:`str`): Table Segment ID

        Return:
            log table id
        """
        sysid, db, schema, table = table_id.split('.')
        segment_prefix = segment_id + '_' if segment_id else ''
        return '.'.join([sysid, db, schema, "XIA_" + segment_prefix + table])

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
    def get_log_info(self, table_id: str, segment_id: str) -> List[dict]:
        """Public function

        This function will return the related table entry of log_info_table

        Args:
            table_id (:obj:`str`): Table ID
            segment_id (:obj:`str`): Table Segment ID

        Return:
            :obj:`list` of :obj:`dict` with fields
            ``TABLE_ID``, ``SEGMENT_ID``, ``START_AGE``, ``END_AGE``, ``LOADED_FLAG``

        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def load_log_data(self, table_id: str, segment_id: str, start_age: int = None, end_age: int = None) -> bool:
        """ Public function

        This function will load the data saved in raw table (log usage) into target table

        Args:
            table_id (:obj:`str`): Table ID
            segment_id (:obj:`str`): Table Segment ID
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
                    **kwargs) -> bool:
        """ Public function

        This function will get the pushed data and save it to the target database

        Args:
            table_id (:obj:`str`): Table ID
            field_data (:obj:`list` of `dict`): Table field description
            data (:obj:`list` of :obj:`dict`): Data in Python dictioany list format (spec x-i-a)

        Return:
            True if successful False in the other case

        Warning:
            By the name of the definition,
            upsert data is replay safe which means one data could be imported more than once.
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def create_table(self, table_id: str, start_seq: str, meta_data: dict, field_data: List[dict],
                     raw_flag: bool, source_id: str) -> bool:
        """Public Function

        Create a table with information provided by header message with specification x-i-a

        Args:
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table
            start_seq (:obj:`str`): Start sequence ID
            source_id (:obj:`str`): Source Table ID with format sysid.dbid.schema.table
            meta_data (:obj:`dict`): Table related meta-data, such as Table description
            field_data (:obj:`list` of `dict`): Table field description
            raw_flag (:obj:`bool`): If the table contains internal fields (_AGE, _SEG, _NO, _OP)

        Return:
            True if successful False in the other case
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def drop_table(self, table_id: str) -> bool:
        """Public Function

        Drop table

        Warning:
            Drop Table without ANY Check !!! Only useful in the extreme cases !!!

        Args:
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table

        Return:
            True if successful False in the other case
       """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def purge_table(self, table_id: str, segment_config: Union[dict, None]) -> bool:
        """Public Function

        Empty related table data

        Args:
            table_id (:obj:`str`): Table ID with format sysid.dbid.schema.table
            segment_config (:obj:`dict`): Table Segment Configuration

        Return:
            True if successful False in the other case
       """
        raise NotImplementedError  # pragma: no cover

    def alter_column(self, table_id: str, old_field_line: dict, new_field_line: dict) -> bool:
        """Public Function

        Changing size of an existed column. (if supported by database)

        Args:
            table_id (:obj:`str`): Table ID
            old_field_line (:obj:`dict`): old field
            new_field_line (:obj:`dict`): new field

        Return:
            True if success False in the other case

        Notes:
            If the database support dynamic column size, just return True
        """
        return False  # pragma: no cover

    def add_column(self, table_id: str, new_field_line: dict) -> bool:
        """Public Function

        Add a column to target database. (if supported by database)

        Args:
            table_id (:obj:`str`): Table ID
            new_field_line (:obj:`dict`): new field

        Return:
            True if success False in the other case

        Notes:
            If the database support dynamic column, just return True
        """
        return False  # pragma: no cover


class DbapiAdaptor(Adaptor):
    """Adaptor for databases supporting PEP249

    Attributes:
        type_dict (:obj:`dict`): field type translator
        create_sql_template (:obj:`str`): create table
        drop_sql_template (:obj:`str`): drop table
        insert_sql_template (:obj:`str`): insert table
        delete_sql_template (:obj:`str`): delete table
        connection (:obj:`Connection`): Connection object defined in PEP249

    Warning:
        The default SQL template is based on SQLite specification.
    """

    type_dict = {}

    # Variable Name: @table_name@, @field_types@, @key_list@
    create_sql_template = "CREATE TABLE {} ( {}, PRIMARY KEY( {} ))"
    # Variable Name: @table_name@
    drop_sql_template = "DROP TABLE {}"
    # Varuabke Bale; @table_name@, @field_type@
    add_column_sql_template = "ALTER TABLE {} ADD ( {} )"
    # Variable Name: @table_name@, @fields@, @value_holders@
    upsert_sql_template = "INSERT OR REPLACE INTO {} ({}) VALUES ( {} )"
    # Variable Name: @table_name@, @where_key_holders@
    delete_sql_template = "DELETE FROM {} WHERE {}"

    # Variable Name: @ctrl_table_name@, @table_name@
    select_from_ctrl_template = ("SELECT * FROM {} WHERE TABLE_ID = {}")


    # ==== Below is only for aged flow scenario====
    # Variable Name: @ori_table_name@, @raw_table_name@, @key_eq_key@, @age_range
    load_del_sql_template = ("DELETE FROM {} WHERE EXISTS ( "
                             "SELECT * FROM {} WHERE {} AND {} AND _OP in ( 'U', 'D' ) )")
    # Variable Name: @ori_table_name@,
    # @field_list@, @raw_table_name@,
    # @raw_table_name@, @obs_insert_sql@, @key_eq_key@, @age_range
    load_ins_sql_template = ("INSERT OR IGNORE INTO {} ({})"
                             "SELECT {} FROM {} t WHERE NOT EXISTS ( "
                             "SELECT * FROM {} r WHERE {} AND {} ) AND {} AND t._OP != 'D'")
    obs_insert_sql = ("(IFNULL(r._SEQ, '00000000000000000000') || "
                      "SUBSTR('0000000000' || IFNULL(r._AGE, ''), -10, 10) || "
                      "SUBSTR('0000000000' || IFNULL(r._NO, ''), -10, 10)) > "
                      "(IFNULL(t._SEQ, '00000000000000000000') || "
                      "SUBSTR('0000000000' || IFNULL(t._AGE, ''), -10, 10) || "
                      "SUBSTR('0000000000' || IFNULL(t._NO, ''), -10, 10)) "
                      "AND r._OP in ('U', 'D')")
    # Variable Name: @log_table_name@, @table_name@, @segment_id@
    select_from_log_template = ("SELECT * FROM {} WHERE TABLE_ID = {} AND SEGMENT_ID = {}")
    # Variable Name: @log_table_name@, @merged@, @old_age_range, @table_name@, @segment_id@
    update_log_table_sql_template = "UPDATE {} SET LOADED_FLAG = {} WHERE {} AND TABLE_ID = {} AND SEGMENT_ID = {}"
    # Variable Name: @log_table_name@, @segment_id@, @merged@
    # @log_table_name@, @table_name@, @segment_id@, @merged@
    remove_old_log_sql_template = ("DELETE FROM {} WHERE TABLE_ID = {} AND SEGMENT_ID = {} AND END_AGE < "
                                   "( SELECT MAX(END_AGE) "
                                   "FROM {} WHERE TABLE_ID = {} AND SEGMENT_ID = {} AND LOADED_FLAG = {} ) ")
    # Variable Name: @raw_table_name@,
    # @log_table_name@, @table_name@, @segment_id@, @merged@
    remove_old_raw_sql_template = ("DELETE FROM {} WHERE _AGE <= "
                                   "( SELECT MAX(END_AGE) "
                                   "FROM {} WHERE TABLE_ID = {} AND SEGMENT_ID = {} AND LOADED_FLAG = {} ) ")

    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        # Duck type check
        if any([not hasattr(db, method) for method in ['cursor', 'close', 'commit']]):
            self.logger.error("connection must an Connection defined by PEP249", extra=self.log_context)
            raise TypeError("XIA-000019")
        else:
            self.connection = db

    def _sql_safe(self, input: str) -> str:
        return input.replace(';', '')

    def get_ctrl_info_list(self, table_id: str):
        cur = self.connection.cursor()
        sql = self._get_ctrl_info_sql()
        try:
            cur.execute(sql, (table_id, ))
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return dict()  # pragma: no cover
        # return_line = {'TABLE_ID': table_id, 'SEGMENT_ID': segment_id}
        records = cur.fetchall()
        return_list = list()
        for record in records:
            sql_result = list(record)
            key_list = [item['field_name'] for item in self._ctrl_table]
            return_line = {key: value for key, value in zip(key_list, sql_result)}
            if return_line.get('META_DATA', None) is not None:
                return_line['META_DATA'] = self._string_to_meta_data(return_line.get('META_DATA'))
            if return_line.get('FIELD_LIST', None) is not None:
                return_line['FIELD_LIST'] = self._string_to_field_data(return_line['FIELD_LIST'])
            return_list.append(return_line)
        return return_list

    def set_ctrl_info(self, table_id: str, segment_id: str, **kwargs):
        old_ctrl_info = self.get_ctrl_info(table_id, segment_id)
        new_ctrl_info = old_ctrl_info.copy()
        if new_ctrl_info.get('META_DATA', None) is not None:
            new_ctrl_info['META_DATA'] = self._meta_data_to_string(new_ctrl_info['META_DATA'])
        if new_ctrl_info.get('FIELD_LIST', None) is not None:
            new_ctrl_info['FIELD_LIST'] = self._field_data_to_string(new_ctrl_info['FIELD_LIST'])
        key_list = [item['field_name'] for item in self._ctrl_table if item['field_name'].lower() in kwargs]
        for key in key_list:
            if key == 'META_DATA':
                new_ctrl_info[key] = self._meta_data_to_string(kwargs [key.lower()])
            elif key == 'FIELD_LIST':
                new_ctrl_info[key] = self._field_data_to_string(kwargs[key.lower()])
            elif key != 'TABLE_ID' and key != 'SEGMENT_ID':
                new_ctrl_info[key] = kwargs[key.lower()]
        return self.upsert_data(self._ctrl_table_id, self._ctrl_table, [new_ctrl_info])

    def drop_table(self, table_id: str, raw_flag: bool = False):
        cur = self.connection.cursor()
        sql = self._get_drop_sql(table_id)
        try:
            cur.execute(sql)
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

        if raw_flag or table_id in [self._ctrl_table_id, self._ctrl_log_id]:
            return True

        ctrl_info_list = self.get_ctrl_info_list(table_id)
        for ctrl_line in ctrl_info_list:
            if ctrl_line['LOG_TABLE_ID'] != table_id:
                self.drop_table(ctrl_line['LOG_TABLE_ID'], True)
        purge_profil = {'field_name': 'TABLE_ID', 'list': [table_id]}
        self.purge_table(self._ctrl_table_id, purge_profil)
        self.purge_table(self._ctrl_log_id, purge_profil)
        return True

    def _create_table_if_not_exists_check(self, table_id: str):
        return True

    def _get_largest_field_dict(self, ctrl_info_list: List[dict]) -> dict:
        return_dict = dict()
        all_existed_fields = [field for ctr in ctrl_info_list for field in ctr['FIELD_LIST']]
        field_names = [line['field_name'] for line in all_existed_fields]
        for field_name in field_names:
            type_chain = list()
            for field in all_existed_fields:
                if field['field_name'] == field_name:
                    if not type_chain:
                        type_chain = field['type_chain']
                    elif len(type_chain) > len(field['type_chain']):
                        type_chain = field['type_chain']
                    elif type_chain[-1] < field['type_chain'][-1]:
                        type_chain = field['type_chain']
            return_dict[field_name] = type_chain
        return return_dict

    def _adapte_fields(self, table_id: str, log_table_id: str, field_data: List[dict]):
        ctrl_info_list = self.get_ctrl_info_list(table_id)
        old_field_dict = self._get_largest_field_dict(ctrl_info_list)
        # Field adaptation
        for field in field_data:
            # Check 2.1: Add field
            if field['field_name'] not in old_field_dict:
                if not self.add_column(table_id, field) or \
                        (log_table_id != table_id and log_table_id is not None and
                         not self.add_column(log_table_id, field)):
                    return False  # pragma: no cover
            # Check 2.2: Adapte field
            elif len(old_field_dict[field['field_name']]) > len(field['type_chain']):
                old_field = {'field_name': field['field_name'],
                             'type_chain': old_field_dict[field['field_name']]}
                if not self.alter_column(table_id, old_field, field) or \
                        (log_table_id != table_id and log_table_id is not None and
                         not self.alter_column(log_table_id, old_field, field)):
                    return False  # pragma: no cover
            elif old_field_dict[field['field_name']][-1] < field['type_chain'][-1]:
                old_field = {'field_name': field['field_name'],
                             'type_chain': old_field_dict[field['field_name']]}
                if not self.alter_column(table_id, old_field, field) or \
                        (log_table_id != table_id and log_table_id is not None and
                         not self.alter_column(log_table_id, old_field, field)):
                    return False  # pragma: no cover
        return True

    def create_table(self, table_id: str, start_seq: str, meta_data: dict, field_data: List[dict],
                     raw_flag: bool, source_id: str):
        field_list = field_data.copy()
        cur = self.connection.cursor()
        if self._create_table_if_not_exists_check(table_id):
            sql = self._get_create_sql(table_id, meta_data, field_list, raw_flag)
            try:
                cur.execute(sql)
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover

        if raw_flag or table_id in [self._ctrl_table_id, self._ctrl_log_id]:
            return True

        # Segment Control Flow
        cur_segment_config = meta_data.get('segment', None)
        segment_id = '' if cur_segment_config is None else cur_segment_config['id']
        ctrl_info_list = self.get_ctrl_info_list(table_id)
        ctrl_info_tmp = [ctr for ctr in ctrl_info_list if ctr.get('SEGMENT_ID', '') == segment_id]
        if len(ctrl_info_tmp) > 0:
            # Case 1: Segment already exists, should check fields
            cur_ctrl_info = ctrl_info_tmp[0]
            log_table_id = cur_ctrl_info['LOG_TABLE_ID']
            # Prepare : Segment related data cannot be changed
            if 'segment' in meta_data:
                meta_data['segement'] = cur_ctrl_info['META_DATA'].get('segement', None)

            if cur_ctrl_info['START_SEQ'] > start_seq:
                # Smaller start seq means an obsoleted data story
                self.logger.warning("Receiving an obsolated Header", extra=self.log_context)
                return True
            elif cur_ctrl_info['START_SEQ'] < start_seq:
                # Bigger start seq means a new data story
                self.logger.info("Starting new data story", extra=self.log_context)
                self.purge_table(table_id, cur_ctrl_info['META_DATA'].get('segement', None))
                if log_table_id != table_id:
                    self.purge_table(log_table_id)
            adapte_log_table_id = log_table_id
        else:
            # Case 2: Segment doesn't exist, should check if it is possible to create a new segment
            cur_segment_flow = SegmentFlower(cur_segment_config)
            for config_line in ctrl_info_list:
                if not cur_segment_flow.check_compatible(config_line.get('META_DATA', {}).get('segment', None)):
                    self.logger.error("Segment not compatible with existed ones", extra=self.log_context)
                    return False
            # log table drop and creation because there is no risk
            log_table_id = self.get_log_table_id(table_id, segment_id)
            assert isinstance(cur_segment_config, (dict, type(None)))
            self.drop_table(log_table_id)
            if not self.create_table(log_table_id, start_seq, meta_data, field_data, True, None):
                self.logger.error("Log table creation Error: {}".format(log_table_id),
                                  extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
            adapte_log_table_id = None

        if ctrl_info_list:
            self._adapte_fields(table_id, adapte_log_table_id, field_data)

        self.set_ctrl_info(table_id=table_id, source_id=source_id, segment_id=segment_id, meta_data=meta_data,
                           field_list=field_data, start_seq=start_seq, log_table_id=log_table_id)
        return True

    def add_column(self, table_id: str, new_field_line: dict):
        cur = self.connection.cursor()
        if self._create_table_if_not_exists_check(table_id):
            sql = self._get_add_column_sql(table_id, new_field_line)
            try:
                cur.execute(sql)
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        return True

    def purge_table(self, table_id: str, segment_config: Union[dict, None] = None):
        cur = self.connection.cursor()
        if segment_config is None:
            sql = self._get_purge_all_sql(table_id)
            try:
                cur.execute(sql)
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        elif 'null' in segment_config or segment_config.get('default', '') is None:
            sql = self._get_purge_null_sql(table_id, segment_config['field_name'])
            try:
                cur.execute(sql)
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        elif segment_config.get('list', None):
            sql = self._get_purge_list_sql(table_id, segment_config['field_name'])
            purge_list = segment_config['list'].copy()
            values = [(value, ) for value in purge_list]
            try:
                cur.executemany(sql, values)
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        elif 'min' in segment_config or segment_config.get('default', '') is not None:
            min_value = segment_config['min'] if 'min' in segment_config else segment_config['default']
            max_value = segment_config['max'] if 'max' in segment_config else segment_config['default']
            sql = self._get_purge_range_sql(table_id, segment_config['field_name'])
            try:
                cur.execute(sql, (min_value, max_value))
            except Exception as e:  # pragma: no cover
                self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
                return False  # pragma: no cover
        log_sql = self._get_purge_log_sql()
        segment_id = '' if segment_config is None else segment_config.get('id', '')
        try:
            cur.execute(log_sql, (table_id, segment_id))
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover
        self.connection.commit()
        return True

    def insert_raw_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        raw_field = field_data.copy()
        raw_field.append(self._age_field)
        raw_field.append(self._seq_field)
        raw_field.append(self._no_field)
        raw_field.append(self._op_field)
        cur = self.connection.cursor()
        ins_sql = self._get_upsert_sql(table_id, raw_field)
        ins_values = self._get_value_tuples(data, raw_field)
        try:
            cur.executemany(ins_sql, ins_values)
            self.connection.commit()
            return True
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

    def upsert_data(self,
                    table_id: str,
                    field_data: List[dict],
                    data: List[dict],
                    **kwargs):
        cur = self.connection.cursor()
        key_list = [item for item in field_data if item['key_flag']]
        del_sql = self._get_delete_sql(table_id, key_list)
        ins_sql = self._get_upsert_sql(table_id, field_data)
        del_data = [item for item in data if item.get('_OP', '') in ['U', 'D']]
        del_vals = self._get_value_tuples(del_data, key_list)
        if len(del_vals) > 0:
            try:
                cur.executemany(del_sql, del_vals)
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

    def get_log_info(self, table_id: str, segment_id: str):
        cur = self.connection.cursor()
        sql = self._get_log_info_sql()
        try:
            cur.execute(sql, (table_id, segment_id))
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return list()  # pragma: no cover
        records = cur.fetchall()
        return_list = list()
        for record in records:
            sql_result = list(record)
            key_list = [item['field_name'] for item in self._ctrl_log_table]
            return_line = {key: value for key, value in zip(key_list, sql_result)}
            return_list.append(return_line)
        return return_list

    def load_log_data(self, table_id: str, segment_id: str, start_age: int = None, end_age: int = None):
        table_info = self.get_ctrl_info(table_id, segment_id)
        raw_table_id = table_info['LOG_TABLE_ID']
        field_data = table_info['FIELD_LIST']
        cur = self.connection.cursor()
        load_del_sql = self._get_load_del_sql(raw_table_id, table_id, field_data, start_age, end_age)
        load_ins_sql = self._get_load_ins_sql(raw_table_id, table_id, field_data, start_age, end_age)
        update_log_sql = self._get_update_log_sql(end_age)
        remove_old_raw_sql = self._get_remove_old_raw_sql(raw_table_id)
        remove_old_log_sql = self._get_remove_old_log_sql()
        try:
            cur.execute(load_del_sql)
            cur.execute(load_ins_sql)
            cur.execute(update_log_sql, ('X', table_id, segment_id))
            cur.execute(remove_old_raw_sql, (table_id, segment_id, 'X'))
            cur.execute(remove_old_log_sql, (table_id, segment_id, table_id, segment_id, 'X'))
            self.connection.commit()
            return True
        except Exception as e:  # pragma: no cover
            self.logger.error("SQL Error: {}".format(e), extra=self.log_context)  # pragma: no cover
            return False  # pragma: no cover

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
        field_list = field_data.copy()
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

    def _get_add_column_sql(self, table_id: str, field_line: dict):
        field_list = [field_line]
        return self.add_column_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                                   self._sql_safe(self._get_field_types(field_list)))

    @abc.abstractmethod
    def _get_upsert_sql(self, table_id: str, field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_delete_sql(self, table_id: str, key_field_data: List[dict]):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_purge_log_sql(self):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_purge_all_sql(self, table_id: str):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_purge_null_sql(self, table_id: str, field_name: str):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_purge_list_sql(self, table_id: str, field_name: str):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_purge_range_sql(self, table_id: str, field_name: str):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_ctrl_info_sql(self):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_log_info_sql(self):
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

    def _get_log_age_condition(self, end_age: int = None) -> str:
        return "1 > 1"  if end_age is None else "END_AGE <= {}".format(end_age)

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
                                                 self._sql_safe(self._get_field_list(field_data)),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(raw_table_name),
                                                 self._sql_safe(self.obs_insert_sql),
                                                 self._sql_safe(self._get_key_eq_key(field_data,
                                                                                     't',
                                                                                     'r')),
                                                 self._sql_safe(self._get_age_range_condition(start_age,
                                                                                              end_age)))

    @abc.abstractmethod
    def _get_update_log_sql(self, end_age: int = None):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_remove_old_log_sql(self):
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def _get_remove_old_raw_sql(self, raw_table_id: str):
        raise NotImplementedError  # pragma: no cover

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

    def _get_upsert_sql(self, table_id: str, field_data: List[dict]):
        return self.upsert_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_field_list(field_data)),
                                               self._sql_safe(self._get_value_holders(field_data)))

    def _get_delete_sql(self, table_id: str, key_field_data: List[dict]):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(self._get_where_key_holders(key_field_data)))

    def _get_purge_log_sql(self):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(self._ctrl_log_id)),
                                               self._sql_safe('TABLE_ID = ? AND SEGMENT_ID = ?'))

    def _get_purge_all_sql(self, table_id: str):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               '1=1')

    def _get_purge_null_sql(self, table_id: str, field_name: str):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(field_name + ' IS NULL'))

    def _get_purge_list_sql(self, table_id: str, field_name: str):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(field_name + ' = ?'))

    def _get_purge_range_sql(self, table_id: str, field_name: str):
        return self.delete_sql_template.format(self._sql_safe(self._get_table_name(table_id)),
                                               self._sql_safe(field_name + ' >= ?' + ' AND ' +
                                                              field_name + ' <= ?'))

    def _get_ctrl_info_sql(self):
        return self.select_from_ctrl_template.format(self._sql_safe(self._get_table_name(self._ctrl_table_id)),
                                                     '?')

    def _get_log_info_sql(self):
        return self.select_from_log_template.format(self._sql_safe(self._get_table_name(self._ctrl_log_id)),
                                                    '?', '?')

    def _get_update_log_sql(self, end_age: int = None):
        log_ctrl_table_name = self._get_table_name(self._ctrl_log_id)
        return self.update_log_table_sql_template.format(self._sql_safe(log_ctrl_table_name),
                                                         '?',
                                                         self._sql_safe(self._get_log_age_condition(end_age)),
                                                         '?', '?')

    def _get_remove_old_log_sql(self):
        log_ctrl_table_name = self._get_table_name(self._ctrl_log_id)
        return self.remove_old_log_sql_template.format(self._sql_safe(log_ctrl_table_name),
                                                       '?', '?',
                                                       self._sql_safe(log_ctrl_table_name),
                                                       '?', '?',
                                                       '?')

    def _get_remove_old_raw_sql(self, raw_table_id: str):
        log_ctrl_table_name = self._get_table_name(self._ctrl_log_id)
        raw_table_name = self._get_table_name(raw_table_id)
        return self.remove_old_raw_sql_template.format(self._sql_safe(raw_table_name),
                                                       self._sql_safe(log_ctrl_table_name),
                                                       '?', '?',
                                                       '?')

class FileAdaptor(Adaptor):
    """Adaptor for exporting data to files

    """
    def __init__(self, fs: RWStorer, location: str, **kwargs):
        super().__init__(**kwargs)
        if not isinstance(fs, RWStorer):
            self.logger.error("File Adapter needs a RWStorer", extra=self.log_context)
            raise TypeError("XIA-000030")
        if not fs.exists(location):
            self.logger.error("Location does not exists", extra=self.log_context)
            raise TypeError("XIA-000031")
        else:
            self.storer = fs
            self.location = location

    def _get_file_name(self, data: List[dict]):
        min_age, min_seq = min([(int(line.get('_AGE', 0)), line.get('_SEQ', '')) for line in data])
        return str(min_age).zfill(20) if min_age > 0 else min_seq

    def _get_key_from_line(self, field_list: List[str], line: dict) -> Tuple:
        return_list = list()
        for key in field_list:
            if key in line:
                return_list.append(line[key])
            else:  # pragma: no cover
                return_list.append(None)  # pragma: no cover
        return tuple(return_list)

    def upsert_data(self, table_id: str, field_data: List[dict], data: List[dict], **kwargs):
        return self.insert_raw_data(table_id, field_data, data)  # pragma: no cover

    def create_table(self, table_id: str, start_seq: str, meta_data: dict, field_data: List[dict],
                     raw_flag: bool, source_id: str):
        return True  # pragma: no cover

    def drop_table(self, table_id: str):
        return True  # pragma: no cover

    def get_ctrl_info_list(self, table_id: str):
        return list()  # pragma: no cover

    def set_ctrl_info(self, table_id: str, segment_id: str, **kwargs):
        return True  # pragma: no cover

    def get_log_info(self, table_id: str, segment_id: str):
        return list()  # pragma: no cover

    def purge_table(self, table_id: str, segment_config: Union[dict, None]):
        return True  # pragma: no cover

    def load_log_data(self, table_id: str, segment_id: str, start_age: int = None, end_age: int = None):
        return True  # pragma: no cover
