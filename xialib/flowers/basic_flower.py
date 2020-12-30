from typing import Tuple, List
from collections import deque
from xialib.flower import Flower

def xia_eq(a, b):
    return a is not None and a == b

def xia_ge(a, b):
    return a is not None and a >= b

def xia_gt(a, b):
    return a is not None and a > b

def xia_le(a, b):
    return a is not None and a <= b

def xia_lt(a, b):
    return a is not None and a < b

def xia_ne(a, b):
    return a is not None and a != b


class BasicFlower(Flower):
    """Basic Flower: get needed fields with needed criteria

    """
    XIA_FIELDS = ['_AGE', '_SEQ', '_NO', '_OP']
    ALL_FIELDS = list()
    NO_FILTER = list(list())
    # Operation Dictionary:
    oper = {'=': xia_eq,
            '>=': xia_ge,
            '>': xia_gt,
            '<=': xia_le,
            '<': xia_lt,
            '!=': xia_ne,
            '<>': xia_ne}

    def __init__(self, field_list: list = ALL_FIELDS, filters: List[List[list]] = NO_FILTER):
        super().__init__(field_list=field_list, filters=filters)
        self.check_params(field_list, filters)
        self.field_list = field_list
        self.filters = filters

    # disjunctive normal form filters (DNF)
    @classmethod
    def _filter_dnf(cls, line: dict, ndf_filters):
        return any([all([cls.oper.get(l2[1])(line.get(l2[0], None), l2[2]) for l2 in l1 if len(l2) > 0])
                    for l1 in ndf_filters])

    # retrieve list of keys from
    @classmethod
    def _filter_column(cls, line: dict, field_list):
        return {key: value for key, value in line.items() if key in field_list}

    # Get dnf filter field set
    @classmethod
    def get_fields_from_filter(cls, ndf_filters: List[List[list]]):
        return set([x[0] for l1 in ndf_filters for x in l1 if len(x) > 0])

    @classmethod
    def get_minimum_fields(cls, field_list, ndf_filters):
        filter_fields = cls.get_fields_from_filter(ndf_filters)
        return list(set(filter_fields) | set(field_list) | set(cls.XIA_FIELDS))

    @classmethod
    def filter_table_dnf(cls, dict_list, ndf_filters):
        return [line for line in dict_list if cls._filter_dnf(line, ndf_filters)]

    @classmethod
    def filter_table_column(cls, dict_list: list, field_list):
        if field_list:
            field_list.extend(cls.XIA_FIELDS)
        return [cls._filter_column(line, field_list) for line in dict_list]

    @classmethod
    def filter_table(cls, dict_list: list, field_list=ALL_FIELDS, filter_list=NO_FILTER):
        if (not filter_list or filter_list == cls.NO_FILTER) and (not field_list or field_list == cls.ALL_FIELDS):
            return dict_list
        elif not filter_list or filter_list == cls.NO_FILTER:
            return cls.filter_table_column(dict_list, field_list)
        elif not field_list or field_list == cls.ALL_FIELDS:
            return cls.filter_table_dnf(dict_list, filter_list)
        else:
            return cls.filter_table_column(cls.filter_table_dnf(dict_list, filter_list), field_list)

    def set_params(self, field_list: list, filters: List[List[list]]):
        self.check_params(field_list, filters)
        self.field_list = field_list
        self.filters = filters

    def check_params(self, field_list: list, filters: List[List[list]]):
        if field_list is not None and not isinstance(field_list, list):
            self.logger.error("Wrong Field List of Basic FLower")
            raise ValueError("XIA-000026")
        if filters is not None and not isinstance(filters, list):
            self.logger.error("Wrong Filters of Basic FLower")
            raise ValueError("XIA-000027")
        else:
            for or_filter in filters:
                if not isinstance(or_filter, list):
                    self.logger.error("Wrong Filters of Basic FLower")
                    raise ValueError("XIA-000027")
                else:
                    for and_filter in or_filter:
                        if not isinstance(and_filter, list) or len(and_filter) != 3:
                            self.logger.error("Wrong Filters of Basic FLower")
                            raise ValueError("XIA-000027")

    def _header_flow(self, data_header: dict, data_body: List[dict]):
        if (not self.field_list or self.field_list == self.ALL_FIELDS):
            return data_header, data_body
        new_data_body = [line for line in data_body if line['field_name'] in self.field_list]
        return data_header, new_data_body

    def _body_flow(self, data_header: dict, data_body: List[dict]):
        return data_header, self.filter_table(data_body, self.field_list, self.filters)
