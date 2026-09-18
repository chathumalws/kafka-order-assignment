import json
from pathlib import Path

BOOTSTRAP_SERVERS = "localhost:19092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-consumer-group"

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schemas" / "order.avsc"


def load_schema() -> str:
    return SCHEMA_PATH.read_text(encoding="utf-8")


def validate_order(order: dict) -> None:
    """
    Permanent validation rules used for the assignment demo.
    These are implementation choices because the assignment does not specify
    exact validation rules.
    """
    if not order.get("orderId"):
        raise ValueError("orderId is required")
    if not order.get("product"):
        raise ValueError("product is required")
    if float(order.get("price", 0)) <= 0:
        raise ValueError("price must be greater than 0")


def pretty(order: dict) -> str:
    return json.dumps(order, ensure_ascii=False)
