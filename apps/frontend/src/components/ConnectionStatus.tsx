type ConnectionStatusProps = {
  label: string;
  connected: boolean;
  healthyLabel?: string;
};

export default function ConnectionStatus({ label, connected, healthyLabel }: ConnectionStatusProps) {
  return (
    <div className="status-row">
      <span>{label}</span>
      <span className={`status-badge ${connected ? 'status-success' : 'status-danger'}`}>
        {connected ? healthyLabel ?? 'CONNECTED' : 'DISCONNECTED'}
      </span>
    </div>
  );
}
