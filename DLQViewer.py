from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import avro.schema

# Kafka and Schema Registry config
bootstrap_servers = 'localhost:29092'
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Load Avro schema
with open("order.avsc") as f:
    order_schema_str = f.read()

# Avro deserializer
def order_from_dict(order_dict, ctx):
    return order_dict

order_deserializer = AvroDeserializer(
    schema_str=order_schema_str,
    schema_registry_client=schema_registry_client,
    from_dict=order_from_dict
)

# DLQ consumer config
dlq_consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': order_deserializer,
    'group.id': 'dlq-viewer-group',
    'auto.offset.reset': 'earliest'
}

dlq_consumer = DeserializingConsumer(dlq_consumer_conf)
dlq_consumer.subscribe(['orders-dlq'])

print("Consuming messages from DLQ...")

while True:
    msg = dlq_consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            continue

    order = msg.value()
    print(f"DLQ Message: {order}")
