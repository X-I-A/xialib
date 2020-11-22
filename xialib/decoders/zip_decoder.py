import io
import zipfile
from xialib.decoder import Decoder


class ZipDecoder(Decoder):
    """
    Supported data encode: ``zip``
    """
    supported_encodes = ['zip']

    def __init__(self):
        super().__init__()

    def _encode_to_blob(self, data_or_io, from_encode, **kwargs):
        # IO to IO
        if isinstance(data_or_io, io.IOBase):
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
            raise TypeError("XIA-000002")
