import os
import uuid
import json
from datetime import datetime
from xialib.publisher import Publisher

class BasicPublisher(Publisher):
    """A local file system based publisher

    Data is saved to path of (destination->topic_id->current_timestamp ).
    all content is dumped to a json file. Because the json do not support blob, the data part is base64 encoded
    """
    blob_support = False


    def _get_message_id(self) -> str:
        return datetime.now().strftime('%Y%m%d%H%M%S%f') + '-' + str(uuid.uuid4())

    def check_destination(self, destination: str, topic_id: str):
        if not os.path.exists(destination):
            try:
                os.mkdir(destination)
                return True
            except Exception as e:
                return False
        else:
            return True

    def _send(self, destination: str, topic_id: str, header: dict, data: str):
        file_name = os.path.join(destination, topic_id, self._get_message_id())
        if not os.path.exists(destination):
            os.mkdir(destination)
        if not os.path.exists(os.path.join(destination, topic_id)):
            os.mkdir(os.path.join(destination, topic_id))
        content = header.copy()
        content['data'] = data
        with open(file_name, 'w') as f:
            f.write(json.dumps(content, ensure_ascii=False))
        return file_name
