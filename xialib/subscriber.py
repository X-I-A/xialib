import abc
import asyncio
import logging
from typing import Tuple, Union, Generator, Callable

__all__ = ['Subscriber']


class Subscriber(metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):
        self.logger = logging.getLogger("XIA.Subscriber")
        if len(self.logger.handlers) == 0:
            formatter = logging.Formatter('%(asctime)s-%(process)d-%(thread)d-%(module)s-%(funcName)s-%(levelname)s-'
                                          ':%(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def unpack_message(self, message: dict) -> Tuple[dict, Union[str, bytes], str]:
        """ Public function

        This function unpack the pulled message without unpacking data part

        Args:
            message (:obj:`TypedDict` : header-:obj:`dict`, data-:obj:`str` or :obj:`bytes`, id-:obj:`str`):

        Returns:
            :obj:`dict`: Message header
            :obj:`str` or :obj:`bytes`: Message data (un-packed)
            :obj:`str`: Message ID
        """
        if any([key not in message for key in ['header', 'data', 'id']]):
            self.logger.error("Not a validate message")
            raise ValueError("XIA-000010")
        return message['header'], message['data'], message['id']

    @abc.abstractmethod
    async def stream(self, source: str, subscription_id: str, callback: Callable, timeout: int = None):
        """ To be implemented public function

        This function stream the message reception in async mode

        Args:
            source (:obj:`str`): source name
            subscription_id (:obj:`str`): subscription id
            callback (:obj:`Callable`): callback(message of the same type of method pull)
            timeout (:obj:`str`): time out in seconds

        Examples:
            Normally, a program should make a response to multiple streams.

            >>>loop = asyncio.get_event_loop()

            >>>tasks = [self.stream('s1', 't1', callback=callback), self.stream('s1', 't2', callback=callback)]

            >>>loop.run_until_complete(asyncio.wait(tasks))

            >>>loop.close()
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def pull(self, source: str, subscription_id: str) -> Generator[dict, None, None]:
        """ To be implemented public function

        This function pull the message

        Args:
            source (:obj:`str`): source name
            subscription_id (:obj:`str`): subscription id
            message_id (:obj:`str`): message id

        Yields:
            :obj:`TypedDict` : header-:obj:`dict`, data-:obj:`str` or :obj:`bytes`, id-:obj:`str`
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def ack(self, source: str, subscription_id: str, message_id: str) -> bool:
        """ To be implemented public function

        This function acknowlegde the message reception.

        Args:
            source (:obj:`str`): source name
            subscription_id (:obj:`str`): subscription id
            message_id (:obj:`str`): message id

        Returns:
            True if successful, False otherwise.
        """
        raise NotImplementedError  # pragma: no cover

    def nack(self, source: str, subscription_id: str, message_id: str) -> bool:
        """ Pubblic function

        This function no-acknowlegde the message reception.

        Args:
            source (:obj:`str`): source name
            subscription_id (:obj:`str`): subscription id
            message_id (:obj:`str`): message id

        Returns:
            True if successful, False otherwise.

        Notes:
            If message servers recognize no difference between a reception without ack and an explict uack,
            just leave this function as is
        """
        return True  # pragma: no cover