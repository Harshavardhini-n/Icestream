type HeaderProps = {
  kafkaConnected: boolean;
  lastUpdated: string;
};

export default function Header({ kafkaConnected, lastUpdated }: HeaderProps) {
  return (
    <header className="topbar">
      <div>
        <div className="title-row">
          <h1>IceStream</h1>
          <span className={`connection-pill ${kafkaConnected ? 'connected' : 'disconnected'}`}>
            {kafkaConnected ? 'Kafka Connected' : 'Kafka Disconnected'}
          </span>
        </div>
        <p className="subtitle">Streaming Intelligence Platform</p>
        <p className="meta-line">Real-time transaction monitoring and event intelligence</p>
      </div>

      <div className="header-meta">
        <div className="meta-card">
          <span className="meta-label">API</span>
          <strong>FastAPI</strong>
        </div>
        <div className="meta-card">
          <span className="meta-label">Updated</span>
          <strong>{lastUpdated}</strong>
        </div>
      </div>
    </header>
  );
}
