export type NodeKind = 'question' | 'action' | 'message';

export interface FlowNodeData {
  question?: string;
  check?: string;
  expectedAnswers?: string[];
  action?: string;
  parameters?: Record<string, unknown>;
  message?: string;
  severity?: string;
  metadata?: Record<string, string>;
}

export interface FlowNodePayload {
  id: string;
  type: NodeKind;
  label?: string;
  position?: {
    x: number;
    y: number;
  };
  data: FlowNodeData;
}

export type EdgeStyle = 'default' | 'success' | 'warning' | 'danger';

export interface FlowEdgeData {
  label?: string;
  style?: EdgeStyle;
  metadata?: Record<string, string>;
}

export interface FlowEdgePayload {
  id?: string;
  source: string;
  target: string;
  viaLabel?: string;
  data?: FlowEdgeData;
}

export interface FlowModel {
  id: string;
  name: string;
  nodes: FlowNodePayload[];
  edges: FlowEdgePayload[];
  metadata?: Record<string, unknown>;
  createdAt?: string;
  updatedAt?: string;
}

export interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
  paths: Array<Array<{ nodeId: string; via?: string }>>;
}

export interface ProjectSummary {
  id: string;
  name: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface FlowSummary {
  id: string;
  name: string;
  updatedAt?: string;
}
