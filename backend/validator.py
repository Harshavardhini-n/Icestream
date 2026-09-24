"""Flink-like order validation rules used by the consumer."""
from __future__ import annotations
from typing import Any

NULL_ERROR = "NULL_ERROR"
RANGE_ERROR = "RANGE_ERROR"
FORMAT_ERROR = "FORMAT_ERROR"
DUPLICATE_ERROR = "DUPLICATE_ERROR"

class Validator:
    def __init__(self) -> None:
        self.seen_orders: set[str] = set()

    def validate(self, order: dict[str, Any]) -> tuple[bool, str | None]:
        for field in ("tax", "user_id", "order_id"):
            if order.get(field) is None:
                return False, NULL_ERROR
        amount = order.get("amount")
        if not isinstance(amount, (int, float)) or isinstance(amount, bool):
            return False, FORMAT_ERROR
        if amount <= 0 or amount >= 10_000:
            return False, RANGE_ERROR
        user_id = order["user_id"]
        if not isinstance(user_id, (str, int)) or isinstance(user_id, bool) or (isinstance(user_id, str) and not user_id.strip()):
            return False, FORMAT_ERROR
        order_id = order["order_id"]
        if not isinstance(order_id, (str, int)) or not str(order_id).strip():
            return False, FORMAT_ERROR
        identity = str(order_id)
        if identity in self.seen_orders:
            return False, DUPLICATE_ERROR
        self.seen_orders.add(identity)
        return True, None

    def reset(self) -> None:
        self.seen_orders.clear()
