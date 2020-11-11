import io
import csv
import codecs
from xialib.exceptions import XIAFormatError
from xialib.formatter import Formatter


class CSVFormatter(Formatter):
    """
    Supported data format: ``csv``
    """
    def __init__(self):
        super().__init__()
        self.support_formats = ['csv']

    def _format_to_record(self, data_or_io, from_format, **kwargs):
        if isinstance(data_or_io, io.BufferedIOBase):
            StreamReader = codecs.getreader('utf-8')
            reader_io = StreamReader(data_or_io)
        elif isinstance(data_or_io, bytes):
            reader_io = io.StringIO(data_or_io.decode())
        else:
            raise XIAFormatError("XIA-000010")  # pragma: no cover
        counter, chunk = 0, list()
        dialect = csv.Sniffer().sniff(reader_io.read(4096))
        reader_io.seek(0)
        reader = csv.DictReader(reader_io, dialect=dialect)
        for row in reader:
            counter += 1
            chunk.append(dict(row))
            if counter % 64 == 0:
                yield chunk
                chunk = list()
        yield chunk
