import asyncio


#ENV_VARIABLE

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'kafka'
KAFKA_CONSUMER_GROUP = 'group-id'
loop = asyncio.get_event_loop()