from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from common import BOOTSTRAP_SERVERS, ORDERS_TOPIC, SCHEMA_REGISTRY_URL, load_schema


def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = AvroSerializer(schema_registry, load_schema())

    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": lambda key, ctx: key.encode("utf-8"),
            "value.serializer": serializer,
        }
    )

    # 9001 triggers a temporary failure once, then succeeds on retry.
    temporary_order = {
        "orderId": "9001",
        "product": "TemporaryFailureDemo",
        "price": 250.0,
    }

    # Negative price is allowed by the Avro float schema, but our business
    # validation marks it as permanently invalid and sends it to the DLQ.
    permanent_order = {
        "orderId": "9002",
        "product": "PermanentFailureDemo",
        "price": -100.0,
    }

    for order in (temporary_order, permanent_order):
        producer.produce(
            ORDERS_TOPIC,
            key=order["orderId"],
            value=order,
        )
        print(f"[DEMO PRODUCER] Sent {order}")

    producer.flush()


if __name__ == "__main__":
    main()
