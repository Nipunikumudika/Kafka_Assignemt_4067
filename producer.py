import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Load schema
with open("order.avsc", "r") as f:
    order_schema_str = f.read()

# Schema Registry
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro serializer
def order_to_dict(order, ctx):
    return order

order_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=order_schema_str,
    to_dict=order_to_dict
)

# Producer config
producer_conf = {
    "bootstrap.servers": "localhost:29092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": order_serializer
}

producer = SerializingProducer(producer_conf)

products = ["Item1", "Item2", "Item3"]

# Global counter
order_counter = 1

def produce_order():
    global order_counter

    order = {
        "orderId": f"Order{order_counter}",
        "product": random.choice(products),
        "price": round(random.uniform(50, 100), 2)
    }

    producer.produce(
        topic="orders",
        key=order["orderId"],
        value=order
    )

    producer.flush()
    print(f"Produced: {order}")

    order_counter += 1  # increase ID

if __name__ == "__main__":
    for _ in range(10):
        produce_order()
