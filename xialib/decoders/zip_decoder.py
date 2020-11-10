import io
import zipfile
from xialib.exceptions import XIADecodeError
from ..decoder import Decoder


class ZipDecoder(Decoder):
    def __init__(self):
        super().__init__()
        self.support_encodes = ['zip']

    def _encode_to_blob(self, data_or_io, from_encode, **kwargs):
        # IO to IO
        if isinstance(data_or_io, io.BufferedIOBase):
            archive = zipfile.ZipFile(data_or_io)
            for file in archive.namelist():
                with archive.open(file) as f:
                    yield f
        # Blob to Blob
        elif isinstance(data_or_io, bytes):
            archive = zipfile.ZipFile(io.BytesIO(data_or_io))
            for file in archive.namelist():
                yield archive.read(file)
        else:
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XIADecodeError("XED-000007")
