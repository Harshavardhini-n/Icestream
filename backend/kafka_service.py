"""Kafka producer/consumer with a local-queue resilience fallback."""
from __future__ import annotations
import asyncio, json, importlib
from collections.abc import Awaitable, Callable

# Import aiokafka dynamically to avoid static analysis/import-time errors when
# the package is not installed (e.g. in lightweight demo environments).
_aiokafka = importlib.import_module("aiokafka") if importlib.util.find_spec("aiokafka") else None
if _aiokafka is not None:
    AIOKafkaConsumer = getattr(_aiokafka, "AIOKafkaConsumer", None)
    AIOKafkaProducer = getattr(_aiokafka, "AIOKafkaProducer", None)
else:
    AIOKafkaConsumer = AIOKafkaProducer = None

BOOTSTRAP_SERVERS = "localhost:9092"

class KafkaService:
    def __init__(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        self.handler = handler
        self.producer = None
        self.consumer = None
        self.local_queue: asyncio.Queue[dict] = asyncio.Queue()
        self.tasks: list[asyncio.Task] = []
        self.kafka_connected = False

    async def start(self) -> None:
        if AIOKafkaProducer is None:
            self.tasks.append(asyncio.create_task(self._consume_local()))
            return
        try:
            self.producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            await self.producer.start()
            self.consumer = AIOKafkaConsumer("orders", bootstrap_servers=BOOTSTRAP_SERVERS, group_id="icestream-validator", auto_offset_reset="latest")
            await self.consumer.start()
            self.kafka_connected = True
            self.tasks.append(asyncio.create_task(self._consume_kafka()))
        except Exception:
            self.kafka_connected = False
            if self.producer:
                await self.producer.stop()
            self.producer = None
        self.tasks.append(asyncio.create_task(self._consume_local()))

    async def produce(self, event: dict) -> None:
        if self.producer:
            try:
                await self.producer.send_and_wait("orders", json.dumps(event, default=str).encode())
                return
            except Exception:
                self.kafka_connected = False
        await self.local_queue.put(event)

    async def _consume_kafka(self) -> None:
        assert self.consumer
        async for message in self.consumer:
            await self.handler(json.loads(message.value.decode()))

    async def _consume_local(self) -> None:
        while True:
            await self.handler(await self.local_queue.get())

    async def stop(self) -> None:
        for task in self.tasks: task.cancel()
        if self.consumer: await self.consumer.stop()
        if self.producer: await self.producer.stop()
