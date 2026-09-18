import json
import time
from typing import Dict, Set

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from common import (
    BOOTSTRAP_SERVERS,
    DLQ_TOPIC,
    GROUP_ID,
    ORDERS_TOPIC,
    SCHEMA_REGISTRY_URL,
    load_schema,
    validate_order,
)

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1

# Only for the live demonstration:
# orderId 9001 fails on its first processing attempt, then succeeds.
temporary_failure_seen: Set[str] = set()


def should_simulate_temporary_failure(order: Dict) -> bool:
    order_id = order["orderId"]
    if order_id == "9001" and order_id not in temporary_failure_seen:
        temporary_failure_seen.add(order_id)
        return True
    return False


def process_order(order: Dict) -> None:
    # Permanent/business validation.
    validate_order(order)

    # Controlled temporary failure for demonstration.
    if should_simulate_temporary_failure(order):
        raise ConnectionError("Simulated temporary service failure")


def send_to_dlq(dlq_producer: Producer, original_message, order: Dict, reason: str):
    payload = {
        "order": order,
        "reason": reason,
        "sourceTopic": original_message.topic(),
        "sourcePartition": original_message.partition(),
        "sourceOffset": original_message.offset(),
    }

    dlq_producer.produce(
        DLQ_TOPIC,
        key=order.get("orderId", "unknown"),
        value=json.dumps(payload).encode("utf-8"),
    )
    dlq_producer.flush()
    print(f"[DLQ] Sent order {order.get('orderId')} -> {DLQ_TOPIC}")


def main():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        schema_registry_client=schema_registry,
        schema_str=load_schema(),
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    consumer.subscribe([ORDERS_TOPIC])

    total_price = 0.0
    successful_count = 0

    print("[CONSUMER] Waiting for Avro order messages... Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[CONSUMER] Kafka error: {msg.error()}")
                continue

            try:
                order = deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as exc:
                print(f"[CONSUMER] Deserialization failed: {exc}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            print(f"\n[CONSUMER] Received: {order}")

            processed = False
            permanent_failure = False
            last_error = None

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    process_order(order)
                    processed = True
                    break
                except ValueError as exc:
                    # Validation/business-rule failure is treated as permanent.
                    permanent_failure = True
                    last_error = exc
                    print(f"[CONSUMER] Permanent failure: {exc}")
                    break
                except Exception as exc:
                    # Other processing failures are treated as temporary.
                    last_error = exc
                    print(
                        f"[RETRY] Order {order['orderId']} attempt "
                        f"{attempt}/{MAX_RETRIES} failed: {exc}"
                    )
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY_SECONDS)

            if processed:
                successful_count += 1
                total_price += float(order["price"])
                running_average = total_price / successful_count
                print(
                    f"[SUCCESS] orderId={order['orderId']} "
                    f"price={float(order['price']):.2f} "
                    f"runningAverage={running_average:.2f}"
                )
            else:
                failure_type = "permanent" if permanent_failure else "retry-exhausted"
                reason = f"{failure_type}: {last_error}"
                send_to_dlq(dlq_producer, msg, order, reason)

            # Commit only after success or after safely routing the failed order to DLQ.
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("\n[CONSUMER] Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
