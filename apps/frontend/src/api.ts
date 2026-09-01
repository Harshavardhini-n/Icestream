import type { EventRecord, HealthStatus, Statistics } from './types';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }

  return (await response.json()) as T;
}

export function getHealth(): Promise<HealthStatus> {
  return request<HealthStatus>('/health');
}

export function getStatistics(): Promise<Statistics> {
  return request<Statistics>('/api/statistics');
}

export function getEvents(limit = 20): Promise<EventRecord[]> {
  return request<EventRecord[]>(`/api/events?limit=${limit}`);
}

export function getEvent(eventId: string): Promise<EventRecord> {
  return request<EventRecord>(`/api/events/${encodeURIComponent(eventId)}`);
}
