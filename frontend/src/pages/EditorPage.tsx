import { Alert, Box, CircularProgress, Snackbar } from '@mui/material';
import html2canvas from 'html2canvas';
import dagre from 'dagre';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ReactFlowInstance, ReactFlowProvider, Viewport } from 'reactflow';
import { useNavigate, useParams } from 'react-router-dom';
import {
  exportFlowAsYaml,
  loadFlow,
  saveFlow,
  validateFlowModel
} from '../lib/api';
import { FlowEdgeData, FlowNodeData, NodeKind, ValidationResult } from '../lib/schema';
import { useEditorStore } from '../lib/store';
import { quickValidateFlow } from '../lib/validators';
import FlowCanvas from '../components/FlowCanvas/FlowCanvas';
import HeaderBar from '../components/HeaderBar';
import Inspector from '../components/Sidebar/Inspector';
import Palette from '../components/Sidebar/Palette';
import PathsModal from '../components/PathsModal';

function slugifyFlowName(value?: string): string {
  if (!value) {
    return 'flow';
  }
  const slug = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 100);
  return slug.length > 0 ? slug : 'flow';
}

function EditorContent({ projectId, flowId }: { projectId: string; flowId: string }) {
  const navigate = useNavigate();
  const setFlow = useEditorStore((state) => state.setFlow);
  const flowName = useEditorStore((state) => state.name);
  const dirty = useEditorStore((state) => state.dirty);
  const setName = useEditorStore((state) => state.setName);
  const addNode = useEditorStore((state) => state.addNode);
  const setNodes = useEditorStore((state) => state.setNodes);
  const nodes = useEditorStore((state) => state.nodes);
  const edges = useEditorStore((state) => state.edges);
  const updateNodeData = useEditorStore((state) => state.updateNodeData);
  const updateEdgeData = useEditorStore((state) => state.updateEdgeData);
  const deleteNode = useEditorStore((state) => state.deleteNode);
  const removeEdge = useEditorStore((state) => state.removeEdge);
  const selectedNodeId = useEditorStore((state) => state.selectedNodeId);
  const selectedEdgeId = useEditorStore((state) => state.selectedEdgeId);
  const getFlowModel = useEditorStore((state) => state.getFlowModel);
  const markPristine = useEditorStore((state) => state.markPristine);

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [pathsOpen, setPathsOpen] = useState(false);
  const [snackbar, setSnackbar] = useState<string | null>(null);
  const canvasRef = useRef<HTMLDivElement>(null);
  const [instance, setInstance] = useState<ReactFlowInstance | null>(null);
  const [fitExportToContent, setFitExportToContent] = useState(true);

  const selectedNode = useMemo(() => nodes.find((node) => node.id === selectedNodeId), [nodes, selectedNodeId]);
  const selectedEdge = useMemo(() => edges.find((edge) => edge.id === selectedEdgeId), [edges, selectedEdgeId]);

  useEffect(() => {
    const fetchFlow = async () => {
      setLoading(true);
      try {
        const data = await loadFlow(projectId, flowId);
        setFlow(data);
      } finally {
        setLoading(false);
      }
    };
    void fetchFlow();
  }, [projectId, flowId, setFlow]);

  const handleAddNode = useCallback(
    (type: NodeKind) => {
      const bounds = canvasRef.current?.getBoundingClientRect();
      const center = bounds
        ? { x: bounds.width / 2, y: bounds.height / 2 }
        : { x: window.innerWidth / 2, y: window.innerHeight / 2 };
      const position = instance ? instance.project(center) : center;
      addNode(type, position);
    },
    [addNode, instance]
  );

  const handleSave = useCallback(async () => {
    if (!projectId || !flowId) return;
    const model = getFlowModel();
    if (!model) return;
    const quick = quickValidateFlow(model);
    if (quick.errors.length > 0) {
      setValidation({ valid: false, errors: quick.errors, warnings: quick.warnings, paths: [] });
      setPathsOpen(true);
      return;
    }
    setSaving(true);
    try {
      await saveFlow(projectId, flowId, model);
      markPristine();
      setSnackbar('Flow saved successfully');
    } catch (error) {
      setSnackbar('Failed to save flow');
    } finally {
      setSaving(false);
    }
  }, [projectId, flowId, getFlowModel, markPristine, setSnackbar]);

  const handleValidate = useCallback(async () => {
    const model = getFlowModel();
    if (!model) return;
    try {
      const result = await validateFlowModel(model);
      setValidation(result);
      setPathsOpen(true);
    } catch (error) {
      setSnackbar('Validation failed');
    }
  }, [getFlowModel, setSnackbar]);

  const handleExportYaml = useCallback(async () => {
    const model = getFlowModel();
    if (!model) return;
    try {
      const { yaml, filename } = await exportFlowAsYaml(model);
      const blob = new Blob([yaml], { type: 'text/yaml' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    } catch (error) {
      setSnackbar('Export failed');
    }
  }, [getFlowModel, setSnackbar]);

  const computeContentBounds = useCallback(() => {
    if (!instance) return null;
    const nodes = instance.getNodes();
    if (!nodes || nodes.length === 0) return null;
    let minX = Number.POSITIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;

    nodes.forEach((node) => {
      const position = node.positionAbsolute ?? node.position ?? { x: 0, y: 0 };
      const width = node.measured?.width ?? (node as any).width ?? 240;
      const height = node.measured?.height ?? (node as any).height ?? 120;
      minX = Math.min(minX, position.x);
      minY = Math.min(minY, position.y);
      maxX = Math.max(maxX, position.x + width);
      maxY = Math.max(maxY, position.y + height);
    });

    const padding = 48;
    return {
      x: minX - padding,
      y: minY - padding,
      width: Math.max(0, maxX - minX + padding * 2),
      height: Math.max(0, maxY - minY + padding * 2)
    };
  }, [instance]);

  const handleExportImage = useCallback(async () => {
    if (!canvasRef.current || !instance) return;

    const element = canvasRef.current;
    const previousViewport: Viewport | null = instance ? instance.getViewport() : null;
    let adjustedView = false;

    if (fitExportToContent) {
      const bounds = computeContentBounds();
      if (bounds) {
        instance.fitBounds(bounds, { padding: 0.05 });
        adjustedView = true;
        await new Promise((resolve) => setTimeout(resolve, 180));
      }
    } else {
      instance.fitView({ padding: 0.2 });
      adjustedView = true;
      await new Promise((resolve) => setTimeout(resolve, 180));
    }

    const canvas = await html2canvas(element, {
      backgroundColor: '#f9fafb',
      useCORS: true,
      scale: window.devicePixelRatio || 2
    });

    if (previousViewport && adjustedView) {
      instance.setViewport(previousViewport);
    }

    const link = document.createElement('a');
    link.href = canvas.toDataURL('image/jpeg', 0.92);
    link.download = `flow_${slugifyFlowName(flowName)}.jpg`;
    link.click();
  }, [canvasRef, computeContentBounds, fitExportToContent, flowName, instance]);

  const handleCenterView = useCallback(() => {
    instance?.fitView({ padding: 0.2 });
  }, [instance]);

  const handleAutoLayout = useCallback(() => {
    const graph = new dagre.graphlib.Graph();
    graph.setGraph({ rankdir: 'LR', nodesep: 200, ranksep: 160 });
    graph.setDefaultEdgeLabel(() => ({}));
    nodes.forEach((node) => {
      graph.setNode(node.id, { width: 240, height: 140 });
    });
    edges.forEach((edge) => {
      graph.setEdge(edge.source, edge.target);
    });
    dagre.layout(graph);
    const layouted = nodes.map((node) => {
      const positioned = graph.node(node.id) as { x: number; y: number } | undefined;
      if (!positioned) return node;
      return { ...node, position: { x: positioned.x, y: positioned.y } };
    });
    setNodes(layouted);
    requestAnimationFrame(() => instance?.fitView({ padding: 0.3 }));
  }, [nodes, edges, setNodes, instance]);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (!(event.ctrlKey || event.metaKey)) return;
      const key = event.key.toLowerCase();
      if (['s', 'e', 'j', 'p'].includes(key)) {
        event.preventDefault();
      }
      switch (key) {
        case 's':
          void handleSave();
          break;
        case 'e':
          void handleValidate();
          break;
        case 'j':
          void handleExportYaml();
          break;
        case 'p':
          void handleExportImage();
          break;
        default:
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [handleSave, handleValidate, handleExportYaml, handleExportImage]);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="100%">
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box className="page-container">
      <HeaderBar
        name={flowName}
        dirty={dirty || saving}
        onNameChange={setName}
        onSave={() => void handleSave()}
        onValidate={() => void handleValidate()}
        onExportYaml={() => void handleExportYaml()}
        onExportImage={() => void handleExportImage()}
        onCenterView={handleCenterView}
        onAutoLayout={handleAutoLayout}
        fitToContent={fitExportToContent}
        onToggleFitToContent={setFitExportToContent}
        onBack={() => navigate(`/projects/${projectId}/flows`)}
      />
      <Box className="page-content">
        <FlowCanvas onInit={setInstance} canvasRef={canvasRef} />
        <div className="sidebar">
          <Palette onAddNode={handleAddNode} />
          <Inspector
            selectedNode={selectedNode}
            selectedEdge={selectedEdge}
            onNodeChange={(id: string, data: Partial<FlowNodeData>) => updateNodeData(id, data)}
            onEdgeChange={(id: string, data: Partial<FlowEdgeData>) => updateEdgeData(id, data)}
            onDeleteNode={deleteNode}
            onDeleteEdge={removeEdge}
          />
        </div>
      </Box>
      <PathsModal open={pathsOpen} onClose={() => setPathsOpen(false)} result={validation} />
      <Snackbar
        open={Boolean(snackbar)}
        autoHideDuration={4000}
        onClose={() => setSnackbar(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        {snackbar && <Alert severity="info" onClose={() => setSnackbar(null)}>{snackbar}</Alert>}
      </Snackbar>
    </Box>
  );
}

export default function EditorPage() {
  const { projectId, flowId } = useParams<{ projectId: string; flowId: string }>();
  if (!projectId || !flowId) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" height="100%">
        Missing identifiers
      </Box>
    );
  }
  return (
    <ReactFlowProvider>
      <EditorContent projectId={projectId} flowId={flowId} />
    </ReactFlowProvider>
  );
}
