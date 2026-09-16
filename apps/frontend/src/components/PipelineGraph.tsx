import { useMemo } from 'react';
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';

type PipelineGraphProps = {
  kafkaConnected: boolean;
  apiConnected: boolean;
};

function PipelineGraph({
  kafkaConnected,
  apiConnected,
}: PipelineGraphProps) {
  const nodes = useMemo<Node[]>(
    () => [
      {
        id: 'events',
        position: { x: 40, y: 170 },
        data: {
          label: 'Checkout Events',
        },
        type: 'default',
      },
      {
        id: 'kafka',
        position: { x: 250, y: 170 },
        data: {
          label: `Kafka\n${
            kafkaConnected ? 'Connected' : 'Disconnected'
          }`,
        },
        type: 'default',
      },
      {
        id: 'flink',
        position: { x: 460, y: 170 },
        data: {
          label: 'Flink',
        },
        type: 'default',
      },
      {
        id: 'quality',
        position: { x: 670, y: 170 },
        data: {
          label: 'Data Quality',
        },
        type: 'default',
      },
      {
        id: 'observability',
        position: { x: 880, y: 170 },
        data: {
          label: 'Observability',
        },
        type: 'default',
      },
      {
        id: 'iceberg',
        position: { x: 670, y: 330 },
        data: {
          label: 'Iceberg',
        },
        type: 'default',
      },
      {
        id: 'minio',
        position: { x: 880, y: 330 },
        data: {
          label: 'MinIO',
        },
        type: 'default',
      },
      {
        id: 'api',
        position: { x: 460, y: 330 },
        data: {
          label: `FastAPI\n${
            apiConnected ? 'Healthy' : 'Unavailable'
          }`,
        },
        type: 'default',
      },
    ],
    [kafkaConnected, apiConnected],
  );

  const edges = useMemo<Edge[]>(
    () => [
      {
        id: 'events-kafka',
        source: 'events',
        target: 'kafka',
        animated: kafkaConnected,
      },
      {
        id: 'kafka-flink',
        source: 'kafka',
        target: 'flink',
        animated: kafkaConnected,
      },
      {
        id: 'flink-quality',
        source: 'flink',
        target: 'quality',
        animated: kafkaConnected,
      },
      {
        id: 'quality-observability',
        source: 'quality',
        target: 'observability',
        animated: apiConnected,
      },
      {
        id: 'quality-iceberg',
        source: 'quality',
        target: 'iceberg',
        animated: kafkaConnected,
      },
      {
        id: 'iceberg-minio',
        source: 'iceberg',
        target: 'minio',
        animated: kafkaConnected,
      },
      {
        id: 'flink-api',
        source: 'flink',
        target: 'api',
        animated: apiConnected,
      },
    ],
    [kafkaConnected, apiConnected],
  );

  return (
    <div className="pipeline-graph">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        fitViewOptions={{
          padding: 0.2,
        }}
        attributionPosition="bottom-left"
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable
        zoomOnScroll
        panOnDrag
      >
        <MiniMap />
        <Controls />
        <Background gap={18} size={1} />
      </ReactFlow>
    </div>
  );
}

export default PipelineGraph;