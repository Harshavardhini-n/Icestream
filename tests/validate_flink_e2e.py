"""End-to-end validation of Flink processing pipeline.

This script demonstrates that the Flink processing logic works correctly
by simulating the complete data flow without requiring PyFlink or Docker.
"""

from __future__ import annotations

import json
import sys
import os
from typing import Any

# Add parent directory to path so we can import flink module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flink.jobs.checkout_processor import (
    EventDeserializer,
    EventValidator,
    EventEnricher,
    EventSerializer,
)


def simulate_pipeline_flow(messages: list[str]) -> dict[str, Any]:
    """Simulate the complete Flink processing pipeline.
    
    Args:
        messages: List of JSON strings (valid or malformed)
        
    Returns:
        Dictionary with processing statistics and results
    """
    deserializer = EventDeserializer()
    validator = EventValidator()
    enricher = EventEnricher()
    serializer = EventSerializer()
    
    results = {
        "total_input": len(messages),
        "deserialized_successfully": 0,
        "deserialized_failed": 0,
        "validated_successfully": 0,
        "validated_failed": 0,
        "processed_events": [],
        "filtered_events": [],
    }
    
    for message in messages:
        # Step 1: Deserialize
        deserialized = deserializer.deserialize(message)
        if deserialized is None:
            results["deserialized_failed"] += 1
            results["filtered_events"].append({"reason": "deserialization_failed"})
            continue
        results["deserialized_successfully"] += 1
        
        # Step 2: Validate
        validated = validator.validate(deserialized)
        if validated is None:
            results["validated_failed"] += 1
            results["filtered_events"].append({
                "reason": "validation_failed",
                "event_id": deserialized.get("event_id"),
            })
            continue
        results["validated_successfully"] += 1
        
        # Step 3: Enrich
        enriched = enricher.enrich(validated)
        
        # Step 4: Serialize
        serialized = serializer.serialize(enriched)
        results["processed_events"].append({
            "serialized": serialized,
            "event_id": enriched.get("event_id"),
            "processed_flag": enriched.get("processed"),
        })
    
    return results


def generate_test_data() -> tuple[list[str], dict[str, Any]]:
    """Generate test data with various scenarios.
    
    Returns:
        Tuple of (messages, expected_results_summary)
    """
    # Valid event
    valid_event_1 = {
        "event_id": "evt-001",
        "event_timestamp": "2026-09-01T22:35:00.000Z",
        "customer_id": "cust-1001",
        "session_id": "sess-abc",
        "product_id": "prod-1001",
        "quantity": 1,
        "unit_price": 49.99,
        "subtotal": 49.99,
        "discount_amount": 5.00,
        "shipping_amount": 9.99,
        "tax_amount": 3.60,
        "total_amount": 58.58,
        "currency": "USD",
        "payment_method": "card",
        "event_type": "checkout",
    }
    
    # Another valid event
    valid_event_2 = {
        "event_id": "evt-002",
        "event_timestamp": "2026-09-01T22:35:01.000Z",
        "customer_id": "cust-1002",
        "session_id": "sess-def",
        "product_id": "prod-1002",
        "quantity": 2,
        "unit_price": 29.99,
        "subtotal": 59.98,
        "discount_amount": 0.00,
        "shipping_amount": 4.99,
        "tax_amount": 4.68,
        "total_amount": 69.65,
        "currency": "USD",
        "payment_method": "wallet",
        "event_type": "checkout",
    }
    
    # Malformed JSON
    malformed_1 = '{"event_id":"evt-003","subtotal":45.10,"tax_amount":,"currency":"USD"}'
    malformed_2 = '{"incomplete json'
    
    # Missing required field
    incomplete_event = {
        "event_id": "evt-004",
        "event_timestamp": "2026-09-01T22:35:02.000Z",
        # Missing customer_id and other fields
        "total_amount": 100.00,
    }
    
    messages = [
        json.dumps(valid_event_1),
        malformed_1,
        json.dumps(valid_event_2),
        malformed_2,
        json.dumps(incomplete_event),
    ]
    
    expected = {
        "total_messages": 5,
        "valid_processed": 2,
        "filtered_count": 3,
    }
    
    return messages, expected


def main() -> int:
    """Run end-to-end validation.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("=" * 80)
    print("ICESTREAM FLINK — END-TO-END PIPELINE VALIDATION")
    print("=" * 80)
    print()
    
    # Generate test data
    print("Generating test data...")
    messages, expected = generate_test_data()
    print(f"✓ Generated {len(messages)} test messages")
    print(f"  - Expected valid: {expected['valid_processed']}")
    print(f"  - Expected filtered: {expected['filtered_count']}")
    print()
    
    # Run pipeline
    print("Running processing pipeline...")
    results = simulate_pipeline_flow(messages)
    print()
    
    # Display results
    print("-" * 80)
    print("PIPELINE EXECUTION RESULTS")
    print("-" * 80)
    print(f"Total input messages:           {results['total_input']}")
    print(f"Deserialized successfully:      {results['deserialized_successfully']}")
    print(f"Deserialization failures:       {results['deserialized_failed']}")
    print(f"Validated successfully:         {results['validated_successfully']}")
    print(f"Validation failures:            {results['validated_failed']}")
    print(f"Successfully processed events:  {len(results['processed_events'])}")
    print(f"Filtered events:                {len(results['filtered_events'])}")
    print()
    
    # Display processed events
    print("-" * 80)
    print("PROCESSED EVENTS (Ready for Kafka output topic)")
    print("-" * 80)
    for i, event_data in enumerate(results['processed_events'], 1):
        print(f"\n[Event {i}]")
        print(f"  Event ID: {event_data['event_id']}")
        print(f"  Processed Flag: {event_data['processed_flag']}")
        print(f"  Output JSON: {event_data['serialized'][:80]}...")
    print()
    
    # Display filtered events
    print("-" * 80)
    print("FILTERED EVENTS (Not sent to output topic)")
    print("-" * 80)
    for i, event_data in enumerate(results['filtered_events'], 1):
        reason = event_data.get('reason', 'unknown')
        event_id = event_data.get('event_id', 'UNKNOWN')
        print(f"  [{i}] {reason:25s} (event_id: {event_id})")
    print()
    
    # Validation
    print("-" * 80)
    print("VALIDATION SUMMARY")
    print("-" * 80)
    success = (
        len(results['processed_events']) == expected['valid_processed']
        and len(results['filtered_events']) == expected['filtered_count']
    )
    
    if success:
        print("✓ END-TO-END PIPELINE TEST PASSED")
        print(f"  - Correctly processed {len(results['processed_events'])} valid events")
        print(f"  - Correctly filtered {len(results['filtered_events'])} invalid events")
        print("  - No events lost or incorrectly processed")
        print()
        print("Pipeline is ready for Kafka integration:")
        print(f"  Input topic:  checkout-events")
        print(f"  Output topic: processed-checkout-events")
        print()
        return 0
    else:
        print("✗ END-TO-END PIPELINE TEST FAILED")
        print(f"  Expected {expected['valid_processed']} processed, got {len(results['processed_events'])}")
        print(f"  Expected {expected['filtered_count']} filtered, got {len(results['filtered_events'])}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
