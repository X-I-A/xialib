import io
import json
import zipfile
import base64
from xialib.formatter import Formatter


class ZstFormatter(Formatter):
    """
    Supported data formats: ``zst``
    """
    support_formats = ['zst']


    def list_to_record(self, data: dict):
        if not data:
            return list()  # pragma: no cover
        line_nbs = [len(value) for key, value in data.items()]
        if len(set(line_nbs)) > 1:
            self.logger.error("list must have identical line numbers")  # pragma: no cover
            raise ValueError("XIA-000006")  # pragma: no cover
        return [{key: value[i] for key, value in data.items() if value[i] is not None} for i in range(line_nbs[0])]

    def _format_to_record(self, data_or_io, from_format, **kwargs):
        if isinstance(data_or_io, io.IOBase):
            raw_data_io = data_or_io
        else:
            raw_data_io = io.BytesIO(data_or_io)
        with zipfile.ZipFile(raw_data_io) as f:
            load_list = [item for item in f.infolist()]
            list_data = {base64.b32decode(im.filename).decode(): json.loads(f.read(im).decode()) for im in load_list}
            yield self.list_to_record(list_data)
