import os
import json
import sqlite3
from typing import List
from xialib.adaptor import DbapiQmarkAdaptor

class SQLiteAdaptor(DbapiQmarkAdaptor):
    type_dict = {
        'NULL': ['null'],
        'INTEGER': ['int'],
        'REAL': ['real'],
        'TEXT': ['char'],
        'BLOB': ['blob']
    }

    def _get_field_type(self, type_chain: list):
        for type in reversed(type_chain):
            for key, value in self.type_dict.items():
                if type in value:
                    return key
        self.logger.error("{} Not supported".format(json.dumps(type_chain)), extra=self.log_context)  # pragma: no cover
        raise TypeError("XIA-000020")  # pragma: no cover

    def _get_field_types(self, field_data: List[dict]):
        field_types = list()
        for field in field_data:
            field_line_list = ['"' + self._sql_safe(field['field_name']) + '"']
            field_line_list.append(self._get_field_type(field['type_chain']))
            if field.get('default', None) is not None:
                if isinstance(field['default'], str):
                    field_line_list.append("DEFAULT '" + self._sql_safe(field['default']) + "'")
                elif isinstance(field['default'], (int, float)):
                    field_line_list.append("DEFAULT " + str(field['default']))
            field_line = ' '.join(field_line_list)
            field_types.append(field_line)
        return ",\n ".join(field_types)

    def alter_column(self, table_id: str, field_line: dict) -> bool:
        return True