import io
import json
from ..formatter import Formatter
from ..exceptions import XIAFormatError


class BasicFormatter(Formatter):
    def __init__(self):
        super().__init__()
        self.support_formats = ['list', 'record']

    def list_to_record(self, data: dict):
        if not data:
            return list()  # pragma: no cover
        line_nbs = [len(value) for key, value in data.items()]
        if len(set(line_nbs)) > 1:
            self.logger.error("list must have identical line numbers")
            raise XIAFormatError("XIA-000008")
        return [{key: value[i] for key, value in data.items() if value[i] is not None} for i in range(line_nbs[0])]

    def _format_to_record(self, data_or_io, from_format, **kwargs):
        if isinstance(data_or_io, bytes):
            if from_format == 'record':
                yield json.loads(data_or_io.decode())
            elif from_format == 'list':
                list_data = json.loads(data_or_io.decode())
                record_data = self.list_to_record(list_data)
                yield record_data
        # IO Termination
        elif isinstance(data_or_io, io.BufferedIOBase):
            raw_data = data_or_io.read().decode()
            if from_format == 'record':
                yield json.loads(raw_data)
            elif from_format == 'list':
                yield self.list_to_record(json.loads(raw_data))
