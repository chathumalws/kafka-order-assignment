import json

from confluent_kafka import Consumer

from common import BOOTSTRAP_SERVERS, DLQ_TOPIC


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "dlq-viewer-group",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([DLQ_TOPIC])
    print("[DLQ VIEWER] Waiting for failed messages... Press Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[DLQ VIEWER] Kafka error: {msg.error()}")
                continue

            payload = json.loads(msg.value().decode("utf-8"))
            print("\n[DLQ VIEWER] Permanently failed message:")
            print(json.dumps(payload, indent=2))
    except KeyboardInterrupt:
        print("\n[DLQ VIEWER] Stopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
