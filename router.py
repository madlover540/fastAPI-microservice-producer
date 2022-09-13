from fastapi import APIRouter
import logging
from typing import List, Dict
from config import loop, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post('/create_message/')
async def send(message: Dict[str, List[int]]):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        print(f'sending message with values {message}')
        value_json = json.dumps(dict(message)).encode('utf-8')
        response = message

        await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
        return response
    finally:
        await producer.stop()


async def consume():
    consumer = AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,
                                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    await consumer.start()
    data = {}
    try:
        async for msg in consumer:
            # msgs = msg.value.decode("ufd-8")
            # data = json.loads(msg)
            # value = data.value()
            # print(check_list(value))
            print(
                "consumed: ",
                msg
            )

    finally:

        await consumer.stop()


def kafka_json_deserializer(serialized):
    return json.loads(serialized)
