type StatusBadgeProps = {
  label: string;
  tone: 'success' | 'warning' | 'danger' | 'neutral';
};

export default function StatusBadge({ label, tone }: StatusBadgeProps) {
  return <span className={`status-badge status-${tone}`}>{label}</span>;
}
