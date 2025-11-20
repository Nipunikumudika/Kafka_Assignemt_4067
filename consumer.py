from confluent_kafka import SerializingProducer, DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from collections import deque
import time
import uuid

# Kafka and Schema Registry configuration
bootstrap_servers = 'localhost:29092'
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Load Avro schema
with open("order.avsc") as f:
    order_schema_str = f.read()

# Avro deserializer for consumer
def order_from_dict(order_dict, ctx):
    return order_dict

order_deserializer = AvroDeserializer(
    schema_str=order_schema_str,
    schema_registry_client=schema_registry_client,
    from_dict=order_from_dict
)

# Avro serializer for DLQ
def order_to_dict(order_obj, ctx):
    return order_obj

order_serializer = AvroSerializer(
    schema_str=order_schema_str,
    schema_registry_client=schema_registry_client,
    to_dict=order_to_dict
)

# Consumer setup
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': order_deserializer,
    'group.id': 'order-group',
    'auto.offset.reset': 'earliest'
}
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['orders'])

# DLQ producer setup
dlq_conf = {
    'bootstrap.servers': bootstrap_servers,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': order_serializer
}
dlq_producer = SerializingProducer(dlq_conf)

# Running average
count = 0
total = 0.0

# Retry queue: (order, retry_count)
retry_queue = deque()

print("Consumer started...")

while True:
    # First process any message in retry queue
    if retry_queue:
        order, retries = retry_queue.popleft()
        order_id = order['orderId']
    else:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                continue
        order = msg.value()
        order_id = order['orderId']
        retries = 0

    try:
        # Simulate temporary failure for expensive items
        if order['price'] > 90:
            raise Exception("Temporary failure")

        # Successfully processed
        count += 1
        total += order['price']
        print(f"Consumed: {order}, Running Average: {total/count:.2f}")

    except Exception as e:
        if retries < 3:
            retries += 1
            print(f"Retrying order {order_id}, attempt {retries}")
            retry_queue.append((order, retries))
            time.sleep(1)
        else:
            print(f"Sending to DLQ: {order}")
            dlq_producer.produce(topic='orders-dlq', key=str(uuid.uuid4()), value=order)
            dlq_producer.flush()
