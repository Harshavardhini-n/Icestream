import { getWebSocketUrl } from './api';
import type { LiveUpdateMessage } from './types';

const RECONNECT_DELAY_MS = 1500;

function isLiveUpdateMessage(value: unknown): value is LiveUpdateMessage {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const message = value as Partial<LiveUpdateMessage>;
  const statistics = message.statistics;

  return (
    message.type === 'snapshot' &&
    Array.isArray(message.events) &&
    typeof statistics === 'object' &&
    statistics !== null &&
    typeof statistics.total_events === 'number' &&
    typeof statistics.valid_events === 'number' &&
    typeof statistics.malformed_events === 'number' &&
    typeof statistics.consumer_errors === 'number' &&
    typeof statistics.events_in_memory === 'number' &&
    typeof message.health === 'object' &&
    message.health !== null &&
    typeof message.health.status === 'string' &&
    typeof message.health.kafka_connected === 'boolean'
  );
}

export function connectToLiveUpdates(
  onMessage: (message: LiveUpdateMessage) => void,
  onConnectionChange: (connected: boolean) => void,
): () => void {
  let stopped = false;
  let socket: WebSocket | null = null;
  let reconnectTimer: number | undefined;

  const scheduleReconnect = () => {
    if (stopped || reconnectTimer !== undefined) {
      return;
    }

    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = undefined;
      connect();
    }, RECONNECT_DELAY_MS);
  };

  const connect = () => {
    if (stopped || socket !== null) {
      return;
    }

    try {
      const nextSocket = new WebSocket(getWebSocketUrl());
      socket = nextSocket;

      nextSocket.addEventListener('open', () => {
        if (!stopped && socket === nextSocket) {
          onConnectionChange(true);
        }
      });

      nextSocket.addEventListener('message', (event) => {
        if (stopped || socket !== nextSocket) {
          return;
        }

        try {
          const message: unknown = JSON.parse(String(event.data));
          if (isLiveUpdateMessage(message)) {
            onMessage(message);
          }
        } catch {
          // Ignore malformed frames and keep the connection available.
        }
      });

      nextSocket.addEventListener('error', () => {
        if (!stopped && socket === nextSocket) {
          onConnectionChange(false);
          nextSocket.close();
        }
      });

      nextSocket.addEventListener('close', () => {
        if (socket === nextSocket) {
          socket = null;
        }
        if (!stopped) {
          onConnectionChange(false);
          scheduleReconnect();
        }
      });
    } catch {
      socket = null;
      onConnectionChange(false);
      scheduleReconnect();
    }
  };

  connect();

  return () => {
    stopped = true;
    if (reconnectTimer !== undefined) {
      window.clearTimeout(reconnectTimer);
      reconnectTimer = undefined;
    }
    const currentSocket = socket;
    socket = null;
    currentSocket?.close();
  };
}