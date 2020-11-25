import abc
import io
import gzip
import base64
import logging
from typing import Union

__all__ = ['Decoder']


class Decoder(metaclass=abc.ABCMeta):
    """
    Attributes:
        supported_encodes (:obj:`list`): encodes supported by Decoder
    """
    supported_encodes = list()

    def __init__(self, **kwargs):
        self.logger = logging.getLogger("XIA.Decoder")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def basic_encoder(self, data: Union[bytes, str], from_encode: str, to_encode: str) -> Union[bytes, str]:
        assert from_encode in ['blob', 'flat', 'gzip', 'b64g']  # pragma: no cover
        assert to_encode in ['blob', 'flat', 'gzip', 'b64g']  # pragma: no cover

        if from_encode == to_encode:
            yield data
        elif to_encode == 'blob':
            if from_encode == 'flat':
                yield data.encode()
            elif from_encode == 'gzip':
                yield gzip.decompress(data)
            elif from_encode == 'b64g':
                yield gzip.decompress(base64.b64decode(data.encode()))
        elif to_encode == 'flat':
            if from_encode == 'gzip':
                yield gzip.decompress(data).decode()
            elif from_encode == 'b64g':
                yield gzip.decompress(base64.b64decode(data.encode())).decode()
            elif from_encode == 'blob':
                yield data.decode()
        elif to_encode == 'b64g':
            if from_encode == 'flat':
                yield base64.b64encode(gzip.compress(data.encode())).decode()
            elif from_encode == 'gzip':
                yield base64.b64encode(data).decode()
            elif from_encode == 'blob':
                yield base64.b64encode(gzip.compress(data)).decode()
        elif to_encode == 'gzip':
            if from_encode == 'flat':
                yield gzip.compress(data.encode())
            elif from_encode == 'b64g':
                yield base64.b64decode(data.encode())
            elif from_encode == 'blob':
                yield gzip.compress(data)

    @abc.abstractmethod
    def _encode_to_blob(self, data_or_io: Union[io.IOBase, bytes],
                        from_encode: str, **kwargs) -> Union[io.IOBase, bytes]:
        """ To be implemented function

        The function to be implemented by customized encoder.

        Args:
            data_or_io (:obj:`io.IOBase` or :obj:`bytes`): data to be decoded
            from_encode (`str`): source encode

        Returns:
            :obj:`io.IOBase` or :obj:`bytes`

        Notes:
            Use :obj:`io.IOBase` if it is possible

        Notes:
            If the customized data encode is based on `str`, please do the decode in the implemented method

        """
        raise NotImplementedError  # pragma: no cover

    def decoder(self, data_or_io:  Union[io.IOBase, bytes, str],
                from_encode: str, to_encode: str, **kwargs) -> Union[io.IOBase, bytes]:
        """ Public function

        This function can decode data or io flow into requested encode.
        If to_encode is ``blob``, deocder will try to keep the :obj:`io.IOBase` as output.

        Args:
            data_or_io (:obj:`io.IOBase` or :obj:`str` or :obj:`bytes`): data to be decoded
            from_encode (str): source encode
            to_encode (str): target encode

        Returns:
            data (:obj:`io.IOBase` or :obj:`bytes`)
        """
        if len(self.supported_encodes) == 0:
            raise NotImplementedError  # pragma: no cover

        if from_encode not in self.supported_encodes:
            self.logger.error("Decoder of {} not found at {}".format(from_encode, self.__class__.__name__))
            raise TypeError("XIA-000001")

        if not isinstance(data_or_io, (str, bytes, io.IOBase)):
            self.logger.error("Data type {} not supported".format(data_or_io.__class__.__name__))
            raise TypeError("XIA-000002")

        if to_encode not in ['blob', 'flat', 'gzip', 'b64g']:
            self.logger.error("Cannot decoder to {}".format(to_encode))
            raise ValueError("XIA-000003")

        # Blob type
        if from_encode in ['blob', 'flat', 'gzip', 'b64g']:
            if from_encode in ['flat', 'b64g'] and isinstance(data_or_io, bytes):
                data_or_io = data_or_io.decode()
            if to_encode == 'blob':
                for output in self._encode_to_blob(data_or_io, from_encode):
                    yield output
            else:
                # Terminating
                if isinstance(data_or_io, io.IOBase):
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
                elif isinstance(output, io.IOBase):
                    output = output.read()
                    for output in self.basic_encoder(output, 'blob', to_encode):
                        yield output
                else:
                    for output in self.basic_encoder(output, 'blob', to_encode):
                        yield output