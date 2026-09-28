from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fastapi import WebSocket


@dataclass(eq=False)
class WebSocketClient:
    socket: WebSocket
    changed: asyncio.Event


class WebSocketConnectionManager:
    def __init__(self) -> None:
        self._clients: set[WebSocketClient] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False

    async def connect(self, socket: WebSocket) -> WebSocketClient:
        await socket.accept()
        self._loop = asyncio.get_running_loop()
        self._closed = False
        client = WebSocketClient(socket=socket, changed=asyncio.Event())
        self._clients.add(client)
        return client

    def disconnect(self, client: WebSocketClient) -> None:
        self._clients.discard(client)

    def notify_clients(self) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed() and not self._closed:
            loop.call_soon_threadsafe(self._signal_clients)

    def _signal_clients(self) -> None:
        for client in tuple(self._clients):
            client.changed.set()

    async def close(self) -> None:
        self._closed = True
        clients = tuple(self._clients)
        self._clients.clear()
        if clients:
            await asyncio.gather(
                *(client.socket.close(code=1001) for client in clients),
                return_exceptions=True,
            )