import io
import os
from xialib.storer import IOStorer

class BasicStorer(IOStorer):
    """Local file system based storer
    """
    store_types = ['file']

    def __init__(self):
        super().__init__()

    def exists(self, location: str):
        if os.path.exists(location):
            return True
        else:
            return False

    def join(self, *args):
        return os.path.join(*args)

    def read(self, location: str) -> bytes:
        with open(location, 'rb') as fp:
            return fp.read()

    def write(self, data_or_io, location: str) -> str:
        if isinstance(data_or_io, io.IOBase):
            with open(location, 'wb') as fp:
                data_or_io.seek(0)
                chunk = data_or_io.read(2 ** 20)
                while chunk:
                    fp.write(chunk)
                    chunk = data_or_io.read(2 ** 20)
        elif isinstance(data_or_io, bytes):
            with open(location, 'wb') as fp:
                fp.write(data_or_io)
        return location

    def remove(self, location: str) -> bool:
        if os.path.exists(location):
            os.remove(location)
            return True
        else:
            return False

    def mkdir(self, path: str):
        os.mkdir(path)

    def get_io_stream(self, location: str):
        with open(location, 'rb') as fp:
            yield fp

    def get_io_wb_stream(self, location: str):
        with open(location, 'wb') as fp:
            yield fp