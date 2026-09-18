import random
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from common import BOOTSTRAP_SERVERS, ORDERS_TOPIC, SCHEMA_REGISTRY_URL, load_schema


def delivery_report(err, msg):
    if err is not None:
        print(f"[PRODUCER] Delivery failed: {err}")
    else:
        print(
            f"[PRODUCER] Sent -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=load_schema(),
    )

    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": lambda key, ctx: key.encode("utf-8"),
            "value.serializer": avro_serializer,
        }
    )

    products = ["Laptop", "Monitor", "Keyboard", "Mouse", "Server"]
    print("[PRODUCER] Producing 8 Avro order messages...")

    for number in range(1001, 1009):
        order = {
            "orderId": str(number),
            "product": random.choice(products),
            "price": round(random.uniform(50.0, 500.0), 2),
        }

        producer.produce(
            topic=ORDERS_TOPIC,
            key=order["orderId"],
            value=order,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        print(f"[PRODUCER] Order {order}")
        time.sleep(0.4)

    producer.flush()
    print("[PRODUCER] Finished.")


if __name__ == "__main__":
    main()
