# Kafka Order Processing Assignment

This project implements the assignment requirements:

- Kafka producer and consumer for order messages
- Avro serialization using `order.avsc`
- Real-time running average of successfully processed order prices
- Retry logic for temporary failures
- Dead Letter Queue (DLQ) for permanently failed or retry-exhausted orders
- Git-ready project structure and a short live demonstration flow

## Order schema

Each order contains:

- `orderId` - string
- `product` - string
- `price` - float

The schema is stored in `schemas/order.avsc`.

## Requirements

Install:

1. Docker Desktop
2. Python 3.10+ recommended
3. Git

## 1. Start Kafka and Schema Registry

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

Kafka should be available at `localhost:19092`.

Schema Registry should be available at `http://localhost:8081`.

## 2. Create a Python virtual environment

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Start the main consumer

Open Terminal 1:

```bash
python consumer.py
```

## 4. Produce normal orders

Open Terminal 2:

```bash
python producer.py
```

The consumer prints each received order and updates the running average.

## 5. Demonstrate retry and DLQ

Keep the main consumer running.

Open Terminal 2:

```bash
python demo_failure_producer.py
```

Expected behavior:

- Order `9001` fails once with a simulated temporary error, retries, and then succeeds.
- Order `9002` contains a negative price. It fails permanent validation and is sent to `orders-dlq`.

## 6. View the DLQ

Open Terminal 3:

```bash
python dlq_consumer.py
```

It will display the permanently failed order and the failure reason.

## How the running average works

Only successfully processed orders are included:

```text
running average = total successful order prices / successful order count
```

Example:

```text
100, 200, 300
running averages:
100
150
200
```

## Retry policy used in this implementation

The assignment requires retry logic but does not specify an exact retry count.

This project uses:

- maximum attempts: 3
- delay: 1 second between attempts
- temporary exceptions are retried
- permanent validation failures go directly to the DLQ
- retry-exhausted temporary failures also go to the DLQ

## Demo-specific behavior

To make retry behavior easy to prove during the live demonstration:

- `orderId = 9001` intentionally throws one temporary `ConnectionError`
- its next attempt succeeds
- `orderId = 9002` uses a negative price, which the application validation rejects permanently

These are controlled demonstration rules, not fields required by the assignment schema.

## Git submission

Initialize Git:

```bash
git init
git add .
git commit -m "Initial Kafka Avro order processing assignment"
```

Then create a GitHub repository and push:

```bash
git branch -M main
git remote add origin YOUR_GITHUB_REPOSITORY_URL
git push -u origin main
```

## Suggested repository evidence

Before submission, make sure the repository contains:

- source files
- Avro schema
- Docker Compose file
- requirements file
- README
- meaningful Git commits
