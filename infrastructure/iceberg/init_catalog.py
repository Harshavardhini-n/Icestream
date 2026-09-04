import json
import urllib.request


BASE_URL = "http://localhost:8181"
NAMESPACE = "checkout"
TABLE = "checkout_events"


def request(method, path, payload=None):
    url = f"{BASE_URL}{path}"

    data = None

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request_obj = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )

    with urllib.request.urlopen(request_obj) as response:
        body = response.read().decode("utf-8")
        return response.status, body


def main():
    status, body = request(
        "POST",
        "/v1/namespaces",
        {"namespace": [NAMESPACE]},
    )

    print(f"Namespace response: {status} {body}")

    schema = [
        {"name": "event_id", "type": "string", "required": True},
        {"name": "event_type", "type": "string", "required": False},
        {"name": "event_timestamp", "type": "timestamptz", "required": False},
        {"name": "user_id", "type": "string", "required": False},
        {"name": "product_id", "type": "string", "required": False},
        {"name": "quantity", "type": "int", "required": False},
        {"name": "unit_price", "type": "double", "required": False},
        {"name": "subtotal", "type": "double", "required": False},
        {"name": "discount_amount", "type": "double", "required": False},
        {"name": "shipping_amount", "type": "double", "required": False},
        {"name": "tax_amount", "type": "double", "required": False},
        {"name": "total_amount", "type": "double", "required": False},
        {"name": "tax_was_null", "type": "boolean", "required": False},
        {
            "name": "calculated_total_amount",
            "type": "double",
            "required": False,
        },
        {"name": "amount_difference", "type": "double", "required": False},
        {"name": "amount_consistent", "type": "boolean", "required": False},
        {"name": "has_discount", "type": "boolean", "required": False},
        {"name": "processed", "type": "boolean", "required": False},
        {"name": "processing_stage", "type": "string", "required": False},
        {"name": "processed_at", "type": "timestamptz", "required": False},
    ]

    table_payload = {
        "name": TABLE,
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "id": index + 1,
                    "name": field["name"],
                    "required": field["required"],
                    "type": field["type"],
                }
                for index, field in enumerate(schema)
            ],
        },
    }

    status, body = request(
        "POST",
        f"/v1/namespaces/{NAMESPACE}/tables",
        table_payload,
    )

    print(f"Table response: {status} {body}")


if __name__ == "__main__":
    main()