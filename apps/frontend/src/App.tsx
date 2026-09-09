import { useEffect, useMemo, useState } from 'react';
import { getEvents, getHealth, getStatistics } from './api';
import ConnectionStatus from './components/ConnectionStatus';
import EventTable from './components/EventTable';
import Header from './components/Header';
import StatCard from './components/StatCard';
import StatusBadge from './components/StatusBadge';
import type { EventRecord, HealthStatus, Statistics } from './types';

const initialStatistics: Statistics = {
  total_events: 0,
  valid_events: 0,
  malformed_events: 0,
  consumer_errors: 0,
  events_in_memory: 0,
};

const initialHealth: HealthStatus = {
  status: 'degraded',
  kafka_connected: false,
  details: 'Connecting to IceStream...',
};

function App() {
  const [statistics, setStatistics] =
    useState<Statistics>(initialStatistics);

  const [events, setEvents] = useState<EventRecord[]>([]);

  const [health, setHealth] =
    useState<HealthStatus>(initialHealth);

  const [apiConnected, setApiConnected] = useState(false);

  const [loading, setLoading] = useState(true);

  const [error, setError] = useState<string | null>(null);

  const [lastUpdated, setLastUpdated] =
    useState('Waiting for first refresh');

  const fetchStatistics = async () => {
    try {
      const nextStatistics = await getStatistics();

      setStatistics(nextStatistics);

      setLastUpdated(
        new Date().toLocaleTimeString([], {
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
        }),
      );

      setError(null);
    } catch {
      setError('Unable to retrieve pipeline statistics.');
    }
  };

  const fetchEvents = async () => {
    try {
      const nextEvents = await getEvents(20);

      setEvents(nextEvents);

      setError(null);
    } catch {
      setError('Unable to retrieve checkout events.');
    }
  };

  const fetchHealth = async () => {
    try {
      const nextHealth = await getHealth();

      setHealth(nextHealth);
      setApiConnected(true);
      setLoading(false);
      setError(null);
    } catch {
      setApiConnected(false);
      setLoading(false);

      setHealth({
        status: 'degraded',
        kafka_connected: false,
        details: 'API unavailable',
      });

      setError('IceStream API is currently unavailable.');
    }
  };

  useEffect(() => {
    const poll = async () => {
      await Promise.allSettled([
        fetchHealth(),
        fetchStatistics(),
        fetchEvents(),
      ]);
    };

    void poll();

    const healthTimer = window.setInterval(() => {
      void fetchHealth();
    }, 5000);

    const metricsTimer = window.setInterval(() => {
      void fetchStatistics();
      void fetchEvents();
    }, 2000);

    return () => {
      window.clearInterval(healthTimer);
      window.clearInterval(metricsTimer);
    };
  }, []);

  const overviewCards = useMemo(
    () => [
      {
        label: 'Total Events',
        value: statistics.total_events,
        hint: 'All messages observed',
      },
      {
        label: 'Valid Events',
        value: statistics.valid_events,
        hint: 'Successfully validated',
      },
      {
        label: 'Malformed Events',
        value: statistics.malformed_events,
        hint: 'Parsing or validation failures',
      },
      {
        label: 'Consumer Errors',
        value: statistics.consumer_errors,
        hint: 'Kafka consumption issues',
      },
    ],
    [statistics],
  );

  const systemOperational =
    apiConnected && health.kafka_connected;

  if (loading && !apiConnected) {
    return (
      <div className="app-shell">
        <div className="loading-state">
          <div>
            <strong>Connecting to IceStream</strong>
            <p>Waiting for the monitoring API...</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <Header
        kafkaConnected={health.kafka_connected}
        lastUpdated={lastUpdated}
      />

      <section className="system-status-bar">
        <div className="system-status-copy">
          <span className="system-status-label">
            Pipeline Status
          </span>

          <strong>
            {systemOperational
              ? 'System Operational'
              : 'System Degraded'}
          </strong>

          <span className="system-status-detail">
            {health.details ?? 'Monitoring pipeline status'}
          </span>
        </div>

        <StatusBadge
          label={systemOperational ? 'OPERATIONAL' : 'DEGRADED'}
          tone={systemOperational ? 'success' : 'warning'}
        />
      </section>

      {error ? (
        <div className="error-banner">
          <StatusBadge
            label="ATTENTION"
            tone="danger"
          />

          <span>{error}</span>
        </div>
      ) : null}

      <section className="stats-grid">
        {overviewCards.map((card) => (
          <StatCard
            key={card.label}
            label={card.label}
            value={card.value}
            hint={card.hint}
          />
        ))}
      </section>

      <section className="summary-row">
        <div className="summary-card">
          <div>
            <span className="summary-label">
              Events in Memory
            </span>

            <small>
              Currently retained by the API consumer
            </small>
          </div>

          <strong>
            {statistics.events_in_memory}
          </strong>
        </div>

        <div className="summary-card">
          <div>
            <span className="summary-label">
              Kafka Connection
            </span>

            <small>
              checkout-events stream
            </small>
          </div>

          <StatusBadge
            label={
              health.kafka_connected
                ? 'CONNECTED'
                : 'DISCONNECTED'
            }
            tone={
              health.kafka_connected
                ? 'success'
                : 'danger'
            }
          />
        </div>
      </section>

      <main className="content-grid">
        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Recent Checkout Events</h2>

              <p className="panel-description">
                Latest events received from the checkout stream
              </p>
            </div>

            <StatusBadge
              label={
                health.kafka_connected
                  ? 'LIVE'
                  : 'OFFLINE'
              }
              tone={
                health.kafka_connected
                  ? 'success'
                  : 'danger'
              }
            />
          </div>

          {events.length > 0 ? (
            <EventTable events={events} />
          ) : (
            <div className="empty-state">
              <div>
                <strong>No checkout events available</strong>
                <p>
                  Events will appear here when the Kafka
                  consumer receives data.
                </p>
              </div>
            </div>
          )}
        </section>

        <aside className="panel overview-panel">
          <div className="panel-header">
            <div>
              <h2>System Overview</h2>

              <p className="panel-description">
                Current pipeline connectivity
              </p>
            </div>
          </div>

          <div className="status-stack">
            <ConnectionStatus
              label="Kafka Status"
              connected={health.kafka_connected}
              healthyLabel="CONNECTED"
            />

            <ConnectionStatus
              label="API Status"
              connected={apiConnected}
              healthyLabel="HEALTHY"
            />

            <div className="status-row">
              <span>Events in memory</span>

              <StatusBadge
                label={String(
                  statistics.events_in_memory,
                )}
                tone="neutral"
              />
            </div>

            <div className="status-row">
              <span>Consumer errors</span>

              <StatusBadge
                label={String(
                  statistics.consumer_errors,
                )}
                tone={
                  statistics.consumer_errors > 0
                    ? 'warning'
                    : 'success'
                }
              />
            </div>

            <div className="status-row">
              <span>Stream</span>

              <StatusBadge
                label="checkout-events"
                tone="neutral"
              />
            </div>
          </div>
        </aside>
      </main>
    </div>
  );
}

export default App;