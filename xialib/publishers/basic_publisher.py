import os
import uuid
import json
from xialib.publisher import Publisher

class BasicPublisher(Publisher):
    """A local file system based publisher

    Data is saved to path of (destination->topic_id->current_timestamp ).
    all content is dumped to a json file. Because the json do not support blob, the data part is base64 encoded
    """
    blob_support = False

    def __init__(self):
        super().__init__()

    def _get_message_id(self) -> str:
        return str(uuid.uuid4())

    def _send(self, destination: str, topic_id: str, header: dict, data):
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
