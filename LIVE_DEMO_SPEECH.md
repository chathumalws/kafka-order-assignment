# Maximum 5-Minute Live Demonstration Speech

Target duration: about 4 minutes 20 seconds to 4 minutes 50 seconds.

## 0:00-0:30 — Introduction

"Hello. In this demonstration I will present my Kafka-based order processing system.
The system produces and consumes order messages using Apache Kafka. Each order is
serialized using Apache Avro. The system also calculates a real-time running average,
retries temporary failures, and sends permanently failed messages to a Dead Letter Queue."

SHOW:
- Project folder
- `schemas/order.avsc`

## 0:30-1:05 — Explain the Avro schema

"This is my `order.avsc` Avro schema. According to the assignment, every order contains
three fields: `orderId` as a string, `product` as a string, and `price` as a float.
The producer serializes each order using this schema before sending it to the Kafka
`orders` topic, and the consumer deserializes it when it receives the message."

SHOW:
- Open `schemas/order.avsc`
- Briefly point to the three fields

## 1:05-1:35 — Show Kafka environment and consumer

"I am running Kafka and Schema Registry using Docker. Here I can confirm that both
services are running. Now I start my consumer. The consumer subscribes to the `orders`
topic and waits for Avro order messages."

RUN:

```bash
docker compose ps
```

Then in Terminal 1:

```bash
python consumer.py
```

## 1:35-2:25 — Produce normal orders and show running average

"Now I run the producer. It creates sample purchase orders with a unique order ID,
a product name, and a randomized price. As each message arrives, the consumer processes
it immediately. You can see the current order price and the updated running average.
The average is calculated only from successfully processed orders."

RUN in Terminal 2:

```bash
python producer.py
```

SHOW:
- Producer output
- Consumer output
- Point to `runningAverage=...`

## 2:25-3:25 — Demonstrate retry logic

"Next I will demonstrate temporary failure handling. For this live demonstration,
order 9001 is configured to simulate a temporary service failure on its first processing
attempt. I send the demo messages now."

RUN:

```bash
python demo_failure_producer.py
```

Then point to Terminal 1.

"The first processing attempt fails, so the consumer does not immediately discard the
message. It applies the retry logic. On the next attempt, the temporary problem is gone,
the order is processed successfully, and the running average is updated. This demonstrates
the retry requirement."

## 3:25-4:15 — Demonstrate DLQ

"The second demo order, order 9002, has a negative price. The Avro schema permits a
float value, but my application-level validation treats a non-positive price as a permanent
business-rule failure. Because this failure should not be repeatedly processed, the
consumer routes the message to the `orders-dlq` Dead Letter Queue topic."

Open Terminal 3 and run:

```bash
python dlq_consumer.py
```

"The DLQ consumer shows the failed order together with the reason and its original Kafka
topic, partition, and offset. This allows failed records to be inspected later without
blocking normal order processing."

## 4:15-4:45 — Git and conclusion

"Finally, the project is maintained as a Git repository for submission. It contains the
producer, consumer, DLQ consumer, Avro schema, Docker configuration, requirements, and
documentation.

In summary, this system demonstrates Kafka message production and consumption, Avro
serialization, real-time aggregation, temporary-failure retry logic, and a Dead Letter
Queue for permanent failures. Thank you."

SHOW:

```bash
git log --oneline
```

Then briefly show the project files.

## Important recording advice

Keep three terminals ready before starting the recording:

- Terminal 1: main consumer
- Terminal 2: producer commands
- Terminal 3: DLQ consumer

Keep `order.avsc` already open in your editor.

Do not spend time installing packages during the video. Install and test everything
before recording.

Aim for 4:30 total so there is a safety margin under the 5-minute maximum.
