import abc
import json
import base64
import logging
from typing import Union
from xialib.exceptions import XIADataSpecError

__all__ = ['Publisher']

class Publisher(metaclass=abc.ABCMeta):
    def __init__(self):
        self.store_types = []
        self.blob_support = True
        self.logger = logging.getLogger("XIA.Publisher")
        formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                      ':%(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _preapare(self, header: dict, data: Union[str, bytes]):
        # Convert All Header to String-Like Objects
        for key, value in header.items():
            if isinstance(value, (int, float, bool)):
                header[key] = str(value)

        if not self.blob_support:
            data = base64.b64encode(data).decode()

        return header, data

    @abc.abstractmethod
    def _send(self, destination: str, topic_id: str, header: dict, data: Union[str, bytes]) -> str:
        """ To be implemented function

        This function should be implemented by customized publisher to publish message

        Args:
            destination (:obj:`str`): Destination
            topic_id (:obj:`str`): Topic ID
            header (:obj:`dict`): Header Attributes
            body (:obj:`bytes` or :obj:`str`): `Data-type` content will be sent gzipped. If `blob_support` is `False`
                the gzipped content will be base64 encoded. `URI-type` content will be sent as is.

        Returns:
            :obj:`str` : Message ID
        """
        raise NotImplementedError  # pragma: no cover

    def publish(self, destination: str, topic_id: str, header: dict, data: Union[str, bytes]) -> str:
        """ Public function

        This function will publish message to specified destination. In X-I-A, the published data must be in 'x-i-a'
        data_spec, 'record' data format and 'gzip' data encode

        Args:
            destination (:obj:`str`): Destination
            topic_id (:obj:`str`): Topic ID
            header (:obj:`dict`): Header Attributes
            body (:obj:`bytes` or :obj:`str`): `Data-type` content will be sent gzipped. If `blob_support` is `False`
                the gzipped content will be base64 encoded. `URI-type` content will be sent as is.

        Returns:
            :obj:`str` : Message ID
        """
        if header.get('data_spec', '') != 'x-i-a' \
                or header.get('data_format', '') != 'record' \
                or header.get('data_encode', '') != 'gzip':
            raise XIADataSpecError("XIA-000002")
        header, data = self._preapare(header, data)
        return self._send(destination, topic_id, header, data)