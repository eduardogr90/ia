import { useCallback } from 'react';
import ReactFlow, {
  Background,
  Connection,
  Controls,
  EdgeTypes,
  MiniMap,
  NodeTypes,
  OnConnect,
  OnEdgesChange,
  OnNodesChange,
  ReactFlowInstance
} from 'reactflow';
import { useEditorStore } from '../../lib/store';
import EdgeWithLabel from './EdgeWithLabel';
import ActionNode from './NodeAction';
import MessageNode from './NodeMessage';
import QuestionNode from './NodeQuestion';

const nodeTypes: NodeTypes = {
  question: QuestionNode,
  action: ActionNode,
  message: MessageNode
};

const edgeTypes: EdgeTypes = {
  labelled: EdgeWithLabel
};

interface FlowCanvasProps {
  onInit: (instance: ReactFlowInstance) => void;
  canvasRef: React.RefObject<HTMLDivElement>;
}

export default function FlowCanvas({ onInit, canvasRef }: FlowCanvasProps) {
  const nodes = useEditorStore((state) => state.nodes);
  const edges = useEditorStore((state) => state.edges);
  const applyNodeChanges = useEditorStore((state) => state.applyNodeChanges);
  const applyEdgeChanges = useEditorStore((state) => state.applyEdgeChanges);
  const addConnection = useEditorStore((state) => state.addConnection);
  const setSelection = useEditorStore((state) => state.setSelection);

  const onNodesChange = useCallback<OnNodesChange>(
    (changes) => applyNodeChanges(changes),
    [applyNodeChanges]
  );

  const onEdgesChange = useCallback<OnEdgesChange>(
    (changes) => applyEdgeChanges(changes),
    [applyEdgeChanges]
  );

  const onConnect = useCallback<OnConnect>(
    (connection: Connection) => addConnection(connection),
    [addConnection]
  );

  const onPaneClick = useCallback(() => setSelection(undefined, undefined), [setSelection]);
  const onNodeClick = useCallback((_: React.MouseEvent, node: any) => setSelection(node.id, undefined), [setSelection]);
  const onEdgeClick = useCallback((_: React.MouseEvent, edge: any) => setSelection(undefined, edge.id), [setSelection]);

  return (
    <div className="flow-wrapper" ref={canvasRef}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onPaneClick={onPaneClick}
        onNodeClick={onNodeClick}
        onEdgeClick={onEdgeClick}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        onInit={onInit}
        snapToGrid
        snapGrid={[16, 16]}
        attributionPosition="bottom-right"
        panOnScroll
        selectionOnDrag
        minZoom={0.25}
        maxZoom={2.5}
      >
        <Background color="#d1d5db" gap={16} />
        <MiniMap pannable zoomable />
        <Controls position="bottom-left" />
      </ReactFlow>
    </div>
  );
}
