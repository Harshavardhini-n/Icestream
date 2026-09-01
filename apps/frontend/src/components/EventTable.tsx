import type { EventRecord } from '../types';

type EventTableProps = {
  events: EventRecord[];
};

const currencyFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
});

function formatNumber(value: number | string | null | undefined): string {
  if (value === null || value === undefined || value === '') {
    return '—';
  }

  const numeric = typeof value === 'number' ? value : Number(value);
  if (Number.isNaN(numeric)) {
    return '—';
  }

  return currencyFormatter.format(numeric);
}

export default function EventTable({ events }: EventTableProps) {
  if (events.length === 0) {
    return (
      <div className="empty-state">
        No events received yet. Start the transaction generator to begin streaming.
      </div>
    );
  }

  return (
    <div className="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>Event ID</th>
            <th>Event Type</th>
            <th>Timestamp</th>
            <th>Customer</th>
            <th>Amount</th>
            <th>Tax</th>
            <th>Total</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event) => (
            <tr key={event.event_id ?? `${event.event_timestamp ?? 'unknown'}-${Math.random()}`}>
              <td>{event.event_id ?? '—'}</td>
              <td>{event.event_type ?? '—'}</td>
              <td>{event.event_timestamp ? new Date(event.event_timestamp).toLocaleString() : '—'}</td>
              <td>{event.customer_id ?? '—'}</td>
              <td>{formatNumber(event.total_amount ?? event.subtotal ?? null)}</td>
              <td>{formatNumber(event.tax_amount ?? null)}</td>
              <td>{formatNumber(event.total_amount ?? null)}</td>
              <td>
                <span className="status-badge status-success">VALID</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
