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

    def _get_field_types(self, field_data: List[dict]):
        field_types = ['"' + field['field_name'] + '" ' + self._get_field_type(field['type_chain'])
                       for field in field_data]
        return ",\n ".join(field_types)
