from __future__ import annotations

import random
import sys
import time
from typing import Any

from .config import Settings
from .generator import apply_anomalies, generate_checkout_event, render_event_payload
from .producer import KafkaCheckoutProducer


def _safe_rate_limit(interval_seconds: float) -> None:
    if interval_seconds <= 0:
        return
    next_deadline = time.perf_counter() + interval_seconds
    while True:
        remaining = next_deadline - time.perf_counter()
        if remaining <= 0:
            return
        time.sleep(min(0.01, remaining))


def run_generator(settings: Settings | None = None) -> int:
    settings = settings or Settings.from_env()
    rng = random.Random(settings.random_seed)

    print("IceStream Transaction Generator")
    print(f"Kafka: {settings.kafka_bootstrap_servers}")
    print(f"Topic: {settings.kafka_checkout_topic}")
    print(f"Rate: {settings.events_per_second} events/sec")
    print(f"Null tax rate: {settings.null_tax_rate * 100:.1f}%")
    print(f"Schema drift rate: {settings.schema_drift_rate * 100:.1f}%")
    print(f"Malformed event rate: {settings.malformed_event_rate * 100:.1f}%")
    print(f"Max events: {settings.max_events if settings.max_events > 0 else 'continuous'}")

    interval_seconds = 1.0 / settings.events_per_second if settings.events_per_second > 0 else 0.0
    generated = 0
    published = 0
    failed = 0
    report_every = max(10, settings.events_per_second)
    producer: KafkaCheckoutProducer | None = None

    try:
        producer = KafkaCheckoutProducer(settings)
        while True:
            if settings.max_events > 0 and generated >= settings.max_events:
                break

            event = generate_checkout_event(rng)
            anomaly_result = apply_anomalies(
                event,
                null_tax_rate=settings.null_tax_rate,
                schema_drift_rate=settings.schema_drift_rate,
                malformed_event_rate=settings.malformed_event_rate,
                rng=rng,
            )
            generated += 1

            try:
                if isinstance(anomaly_result, str):
                    payload = anomaly_result.encode("utf-8")
                    producer.publish(anomaly_result, key=f"malformed-{generated}")
                else:
                    payload = render_event_payload(anomaly_result)
                    producer.publish(anomaly_result, key=anomaly_result.get("event_id", f"evt-{generated}"))
                published += 1
            except Exception as exc:  # pragma: no cover - exercised in runtime only
                failed += 1
                print(f"Kafka publish failed for event {generated}: {exc}", file=sys.stderr)
                raise

            if settings.events_per_second > 0 and (generated % report_every == 0 or generated == settings.max_events):
                rate = published / max(1, generated)
                print(
                    f"Generated: {generated} | Published: {published} | Failed: {failed} | "
                    f"Rate: approx {rate:.2f} events/sec"
                )

            if settings.events_per_second > 0:
                _safe_rate_limit(interval_seconds)

    except KeyboardInterrupt:
        print("\nInterrupted by user; stopping generator.")
    except Exception as exc:
        print(f"Kafka is unavailable or the broker cannot be reached: {exc}", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close()

    print(f"Final totals: generated={generated}, published={published}, failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_generator())
