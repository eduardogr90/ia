import { create } from 'zustand';
import {
  applyEdgeChanges,
  applyNodeChanges,
  Connection,
  Edge,
  EdgeChange,
  MarkerType,
  Node,
  NodeChange
} from 'reactflow';
import {
  FlowEdgeData,
  FlowEdgePayload,
  FlowModel,
  FlowNodeData,
  FlowNodePayload,
  NodeKind
} from './schema';

export type EditorNode = Node<FlowNodeData>;
export type EditorEdge = Edge<FlowEdgeData>;

interface EditorState {
  flowId?: string;
  name: string;
  metadata: Record<string, unknown>;
  nodes: EditorNode[];
  edges: EditorEdge[];
  dirty: boolean;
  selectedNodeId?: string;
  selectedEdgeId?: string;
  setFlow: (flow: FlowModel) => void;
  setName: (name: string) => void;
  setMetadata: (metadata: Record<string, unknown>) => void;
  applyNodeChanges: (changes: NodeChange[]) => void;
  applyEdgeChanges: (changes: EdgeChange[]) => void;
  addConnection: (connection: Connection) => EditorEdge | null;
  addNode: (type: NodeKind, position: { x: number; y: number }) => EditorNode;
  setNodes: (nodes: EditorNode[]) => void;
  updateNodeData: (id: string, data: Partial<FlowNodeData>) => void;
  updateEdgeData: (id: string, data: Partial<FlowEdgeData>) => void;
  removeEdge: (id: string) => void;
  deleteNode: (id: string) => void;
  setSelection: (nodeId?: string, edgeId?: string) => void;
  getFlowModel: () => FlowModel | null;
  markPristine: () => void;
}

function defaultNodeData(data?: FlowNodeData): FlowNodeData {
  return {
    question: data?.question ?? '',
    check: data?.check ?? '',
    expectedAnswers: data?.expectedAnswers ? [...data.expectedAnswers] : [],
    action: data?.action ?? '',
    parameters: data?.parameters ? { ...data.parameters } : {},
    message: data?.message ?? '',
    severity: data?.severity ?? '',
    metadata: data?.metadata ? { ...data.metadata } : {}
  };
}

function defaultEdgeData(data?: FlowEdgeData): FlowEdgeData {
  return {
    label: data?.label ?? '',
    style: data?.style ?? 'default',
    metadata: data?.metadata ? { ...data.metadata } : {}
  };
}

const colorByStyle: Record<string, string> = {
  default: '#64748b',
  success: '#16a34a',
  warning: '#f59e0b',
  danger: '#dc2626'
};

function makeNodePosition(index: number): { x: number; y: number } {
  const column = index % 4;
  const row = Math.floor(index / 4);
  return { x: column * 280, y: row * 180 };
}

function makeId(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

function toEditorNodes(nodes: FlowNodePayload[]): EditorNode[] {
  return nodes.map((node, index) => ({
    id: node.id,
    type: node.type,
    position: node.position ?? makeNodePosition(index),
    data: defaultNodeData(node.data),
    dragHandle: '.node-drag-handle'
  }));
}

function toEditorEdges(edges: FlowEdgePayload[]): EditorEdge[] {
  return edges.map((edge) => ({
    id: edge.id ?? makeId('edge'),
    source: edge.source,
    target: edge.target,
    type: 'labelled',
    markerEnd: { type: MarkerType.ArrowClosed, width: 24, height: 24, color: colorByStyle[edge.data?.style ?? 'default'] },
    data: defaultEdgeData(edge.data),
    animated: edge.data?.style === 'success'
  }));
}

function serializeNodes(nodes: EditorNode[]): FlowNodePayload[] {
  return nodes.map((node) => ({
    id: node.id,
    type: (node.type as NodeKind) ?? 'question',
    position: node.position,
    data: { ...node.data },
    label: node.data.question || node.data.message || node.data.action || node.id
  }));
}

function serializeEdges(edges: EditorEdge[]): FlowEdgePayload[] {
  return edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    viaLabel: edge.data?.label || undefined,
    data: edge.data ? { ...edge.data } : undefined
  }));
}

export const useEditorStore = create<EditorState>((set, get) => ({
  flowId: undefined,
  name: '',
  metadata: {},
  nodes: [],
  edges: [],
  dirty: false,
  selectedNodeId: undefined,
  selectedEdgeId: undefined,
  setFlow: (flow: FlowModel) => {
    set({
      flowId: flow.id,
      name: flow.name,
      metadata: { ...(flow.metadata ?? {}) },
      nodes: toEditorNodes(flow.nodes ?? []),
      edges: toEditorEdges(flow.edges ?? []),
      dirty: false,
      selectedNodeId: undefined,
      selectedEdgeId: undefined
    });
  },
  setName: (name: string) => set({ name, dirty: true }),
  setMetadata: (metadata: Record<string, unknown>) => set({ metadata: { ...metadata }, dirty: true }),
  applyNodeChanges: (changes: NodeChange[]) =>
    set((state) => ({ nodes: applyNodeChanges(changes, state.nodes), dirty: true })),
  applyEdgeChanges: (changes: EdgeChange[]) =>
    set((state) => ({ edges: applyEdgeChanges(changes, state.edges), dirty: true })),
  addConnection: (connection: Connection) => {
    if (!connection.source || !connection.target) {
      return null;
    }
    const edge: EditorEdge = {
      id: makeId('edge'),
      source: connection.source,
      target: connection.target,
      type: 'labelled',
      data: defaultEdgeData(),
      markerEnd: { type: MarkerType.ArrowClosed, width: 24, height: 24, color: colorByStyle.default }
    };
    set((state) => ({ edges: [...state.edges, edge], dirty: true }));
    return edge;
  },
  addNode: (type: NodeKind, position: { x: number; y: number }) => {
    const node: EditorNode = {
      id: makeId(type),
      type,
      position,
      data: defaultNodeData(),
      dragHandle: '.node-drag-handle'
    };
    set((state) => ({ nodes: [...state.nodes, node], dirty: true, selectedNodeId: node.id, selectedEdgeId: undefined }));
    return node;
  },
  setNodes: (nodes: EditorNode[]) => set({ nodes, dirty: true }),
  updateNodeData: (id: string, data: Partial<FlowNodeData>) => {
    set((state) => ({
      nodes: state.nodes.map((node) =>
        node.id === id ? { ...node, data: { ...node.data, ...data } } : node
      ),
      dirty: true
    }));
  },
  updateEdgeData: (id: string, data: Partial<FlowEdgeData>) => {
    set((state) => ({
      edges: state.edges.map((edge) =>
        edge.id === id
          ? {
              ...edge,
              data: { ...edge.data, ...data },
              markerEnd: {
                type: MarkerType.ArrowClosed,
                width: 24,
                height: 24,
                color: colorByStyle[(data.style ?? edge.data?.style ?? 'default')]
              },
              animated: (data.style ?? edge.data?.style) === 'success'
            }
          : edge
      ),
      dirty: true
    }));
  },
  removeEdge: (id: string) => {
    set((state) => ({ edges: state.edges.filter((edge) => edge.id !== id), dirty: true }));
  },
  deleteNode: (id: string) => {
    set((state) => ({
      nodes: state.nodes.filter((node) => node.id !== id),
      edges: state.edges.filter((edge) => edge.source !== id && edge.target !== id),
      dirty: true,
      selectedNodeId: undefined,
      selectedEdgeId: undefined
    }));
  },
  setSelection: (nodeId?: string, edgeId?: string) => set({ selectedNodeId: nodeId, selectedEdgeId: edgeId }),
  getFlowModel: () => {
    const state = get();
    if (!state.flowId) {
      return null;
    }
    return {
      id: state.flowId,
      name: state.name,
      metadata: { ...state.metadata },
      nodes: serializeNodes(state.nodes),
      edges: serializeEdges(state.edges)
    };
  },
  markPristine: () => set({ dirty: false })
}));
