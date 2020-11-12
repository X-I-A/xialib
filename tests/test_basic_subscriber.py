import os
import json
import base64
import gzip
import asyncio
import pytest
from xialib import BasicSubscriber

source = os.path.join('.', 'input', 'module_specific')
subscription_id = 'subscriber'
channel = os.path.join('.', 'input', 'module_specific', 'subscriber_stream')

def callback(s: BasicSubscriber, message: dict, source, subscription_id):
    header, data, id = s.unpack_message(message)
    assert int(header['age']) == 2


@pytest.fixture(scope='module')
def subscriber():
    subscriber = BasicSubscriber()
    yield subscriber


def test_pull(subscriber):
    for message in subscriber.pull(source, subscription_id):
        header, data, id = subscriber.unpack_message(message)
        assert id in ['simple_header', 'complex_header']
        if id == 'simple_header':
            assert int(header['age']) == 2
            for line in json.loads(gzip.decompress(base64.b64decode(data.encode())).decode()):
                assert 'é' in line['NAME']
        elif id == 'complex_header':
            assert isinstance(header['merged_data'], list)
            assert isinstance(header['encrypted_data'], dict)


def test_ack(subscriber):
    with open(os.path.join(source, subscription_id, 'simple_header')) as f1:
        data = f1.read()
        with open(os.path.join(source, subscription_id, 'ack_test'), 'w') as f2:
            f2.write(data)
    assert subscriber.ack(source, subscription_id, 'ack_test')
    assert not subscriber.ack(source, subscription_id, 'ack_test')


def test_stream(subscriber):
    loop = asyncio.get_event_loop()
    task1 = subscriber.stream(channel, 'stream1', callback=callback, timeout=2)
    task2 = subscriber.stream(channel, 'stream2', callback=callback, timeout=2)
    task3 = subscriber.stream('wrong_path', 'wrong_topic', callback=callback, timeout=2)
    loop.run_until_complete(asyncio.wait([task1, task2, task3]))
    loop.close()


def test_exceptions(subscriber):
    with pytest.raises(ValueError):
        subscriber.unpack_message({'wrong': 'message'})
    with pytest.raises(ValueError):
        for message in subscriber.pull('wrong_path', 'wrong_topic'):
            break  # pragma: no cover
