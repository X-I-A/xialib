import os
import json
import asyncio
from datetime import datetime, timedelta
from xialib.subscriber import Subscriber
from typing import Callable

class BasicSubscriber(Subscriber):
    """A local file system based subscriber

    Data is pulled from file in the path of (source->topic_id).
    Ack message equals to delete file
    """
    def pull(self, source: str, subscription_id: str):
        subscription_path = os.path.join(source, subscription_id)
        if not os.path.exists(subscription_path):
            self.logger.error("Subscription path {} does not exist".format(subscription_path))
            raise ValueError("XIA-000011")
        current_time_str = datetime.now().strftime('%Y%m%d%H%M%S%f')
        msg_list = sorted([p for p in os.listdir(subscription_path) if p < current_time_str])
        for msg_id in msg_list:
            with open(os.path.join(subscription_path, msg_id), 'rb') as fp:
                header = json.loads(fp.read().decode())
                data = header.pop('data')
                message = {'header': header, 'data': data, 'id': msg_id}
            yield message

    async def stream(self, source: str, subscription_id: str, callback: Callable, timeout: int = None):
        subscription_path = os.path.join(source, subscription_id)
        if not os.path.exists(subscription_path):
            self.logger.error("Subscription path {} does not exist".format(subscription_path))
            return
        self.logger.info("Streaming subscirption {}".format(subscription_path))
        msg_list, idle_time = set(), 0
        while True:
            current_time_str = datetime.now().strftime('%Y%m%d%H%M%S%f')
            new_msg_list = set(sorted([p for p in os.listdir(subscription_path) if p < current_time_str]))
            for msg_id in new_msg_list - msg_list:
                idle_time = 0
                with open(os.path.join(subscription_path, msg_id), 'rb') as fp:
                    header = json.loads(fp.read().decode())
                    data = header.pop('data')
                    message = {'header': header, 'data': data, 'id': msg_id}
                if message:
                    callback(self, message, source, subscription_id)
            await asyncio.sleep(1)
            idle_time += 1
            msg_list = new_msg_list
            if timeout is not None and idle_time >= timeout:
                self.logger.info("Timeout vault reached, exit")
                return

    def ack(self, source: str, subscription_id: str, message_id: str):
        message_location = os.path.join(source, subscription_id, message_id)
        if not os.path.exists(message_location):
            self.logger.warning("Message {} not found".format(message_location))
            return False
        else:
            os.remove(message_location)
            return True

    def nack(self, source: str, subscription_id: str, message_id: str):
        message_location = os.path.join(source, subscription_id, message_id)
        if not os.path.exists(message_location):
            self.logger.warning("Message {} not found".format(message_location))
            return False
        else:
            msg_uuid = message_id.split('-', 1)[1]
            timestamp = (datetime.now() + timedelta(seconds=10)).strftime('%Y%m%d%H%M%S%f')
            new_message_location = os.path.join(source, subscription_id, timestamp + '-' + msg_uuid)
            os.rename(message_location, new_message_location)
            return True