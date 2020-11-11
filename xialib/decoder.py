import abc
import io
import gzip
import base64
import logging
from xialib.exceptions import XIADecodeError

__all__ = ['Decoder']


class Decoder(metaclass=abc.ABCMeta):
    def __init__(self):
        self.supported_encodes = []
        self.logger = logging.getLogger("Xeed.Decoder")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def basic_encoder(self, data, from_encode, to_encode):
        assert from_encode in ['blob', 'flat', 'gzip', 'b64g']  # pragma: no cover
        assert to_encode in ['blob', 'flat', 'gzip', 'b64g']  # pragma: no cover

        if from_encode == to_encode:
            yield data
        if from_encode == 'gzip' and to_encode == 'flat':
            yield gzip.decompress(data).decode()
        elif from_encode == 'b64g' and to_encode == 'flat':
            yield gzip.decompress(base64.b64decode(data.encode())).decode()
        elif from_encode == 'blob' and to_encode == 'flat':
            yield data.decode()
        elif from_encode == 'flat' and to_encode == 'b64g':
            yield base64.b64encode(gzip.compress(data.encode())).decode()
        elif from_encode == 'gzip' and to_encode == 'b64g':
            yield base64.b64encode(data).decode()
        elif from_encode == 'blob' and to_encode == 'b64g':
            yield base64.b64encode(gzip.compress(data)).decode()
        elif from_encode == 'flat' and to_encode == 'gzip':
            yield gzip.compress(data.encode())
        elif from_encode == 'b64g' and to_encode == 'gzip':
            yield base64.b64decode(data.encode())
        elif from_encode == 'blob' and to_encode == 'gzip':
            yield gzip.compress(data)
        elif from_encode == 'flat' and to_encode == 'blob':
            yield data.encode()
        elif from_encode == 'gzip' and to_encode == 'blob':
            yield gzip.decompress(data)
        elif from_encode == 'b64g' and to_encode == 'blob':
            yield gzip.decompress(base64.b64decode(data.encode()))

    @abc.abstractmethod
    def _encode_to_blob(self, data_or_io, from_encode, **kwargs):
        """ To be implemented function

        The function to be implemented by customized encoder.

        Args:
            data_or_io (:obj:`io.BufferedIOBase` or :obj:`bytes`): data to be decoded
            from_encode (str): source encode

        Returns:
            :obj:`io.BufferedIOBase` or :obj:`bytes`

        Note:
            Use :obj:`io.BufferedIOBase` if it is possible
        """
        raise NotImplementedError  # pragma: no cover

    def decoder(self, data_or_io, from_encode, to_encode, **kwargs):
        """ Public function

        This function can decode data or io flow into requested encode.
        If to_encode is ``blob``, deocder will try to keep the :obj:`io.BufferedIOBase` as output.

        Args:
            data_or_io (:obj:`io.BufferedIOBase` or :obj:`str` or :obj:`bytes`): data to be decoded
            from_encode (str): source encode
            to_encode (str): target encode

        Returns:
            data (:obj:`io.BufferedIOBase` or :obj:`bytes`)
        """
        if len(self.supported_encodes) == 0:
            raise NotImplementedError  # pragma: no cover

        if from_encode not in self.supported_encodes:
            self.logger.error("Decoder of {} not found at {}".format(from_encode, self.__class__.__name__))
            raise XIADecodeError("XIA-000007")

        if not isinstance(data_or_io, (str, bytes, io.BufferedIOBase)):
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise XIADecodeError("XIA-000007")

        if to_encode not in ['blob', 'flat', 'gzip', 'b64g']:
            self.logger.error("Cannot decoder to {}".format(to_encode))
            raise XIADecodeError("XIA-000006")

        # Blob type
        if from_encode in ['blob', 'flat', 'gzip', 'b64g']:
            if to_encode == 'blob':
                for output in self._encode_to_blob(data_or_io, from_encode):
                    yield output
            else:
                # Terminating
                if isinstance(data_or_io, io.BufferedIOBase):
                    if from_encode in ['flat', 'b64g']:
                        data_or_io = data_or_io.read().decode()
                    else:
                        data_or_io = data_or_io.read()
                for output in self.basic_encoder(data_or_io, from_encode, to_encode):
                    yield output
        else:
            for output in self._encode_to_blob(data_or_io, from_encode, **kwargs):
                if to_encode == 'blob':
                    yield output
                elif isinstance(output, io.BufferedIOBase):
                    output = output.read()
                    for output in self.basic_encoder(output, 'blob', to_encode):
                        yield output
                else:
                    for output in self.basic_encoder(output, 'blob', to_encode):
                        yield output