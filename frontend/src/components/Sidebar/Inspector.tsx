import {
  Box,
  Button,
  Divider,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import { useEffect, useMemo, useState } from 'react';
import { EditorEdge, EditorNode } from '../../lib/store';
import { FlowEdgeData, FlowNodeData } from '../../lib/schema';

interface InspectorProps {
  selectedNode?: EditorNode;
  selectedEdge?: EditorEdge;
  onNodeChange: (id: string, data: Partial<FlowNodeData>) => void;
  onEdgeChange: (id: string, data: Partial<FlowEdgeData>) => void;
  onDeleteNode: (id: string) => void;
  onDeleteEdge: (id: string) => void;
}

function MetadataEditor({
  metadata,
  onChange
}: {
  metadata: Record<string, string> | undefined;
  onChange: (value: Record<string, string>) => void;
}) {
  const entries = useMemo(() => Object.entries(metadata ?? {}), [metadata]);

  const handleKeyChange = (index: number, newKey: string) => {
    const entry = entries[index];
    if (!entry) return;
    const [, value] = entry;
    const next: Record<string, string> = {};
    entries.forEach(([key, val], idx) => {
      if (idx === index) {
        if (newKey.trim().length > 0) {
          next[newKey] = val;
        }
      } else {
        next[key] = val;
      }
    });
    onChange(next);
  };

  const handleValueChange = (index: number, newValue: string) => {
    if (!entries[index]) return;
    const next: Record<string, string> = {};
    entries.forEach(([key, val], idx) => {
      next[key] = idx === index ? newValue : val;
    });
    onChange(next);
  };

  const addMetadata = () => {
    const keyBase = `key${entries.length + 1}`;
    let key = keyBase;
    let counter = 1;
    const existing = new Set(entries.map(([existingKey]) => existingKey));
    while (existing.has(key)) {
      counter += 1;
      key = `${keyBase}-${counter}`;
    }
    onChange({ ...Object.fromEntries(entries), [key]: '' });
  };

  const removeEntry = (keyToRemove: string) => {
    const next: Record<string, string> = {};
    entries.forEach(([key, value]) => {
      if (key !== keyToRemove) {
        next[key] = value;
      }
    });
    onChange(next);
  };

  return (
    <Stack spacing={1.5}>
      {entries.map(([key, value], index) => (
        <Stack spacing={1} key={`${key}-${index}`} direction="row" alignItems="center">
          <TextField
            size="small"
            label="Key"
            value={key}
            onChange={(event) => handleKeyChange(index, event.target.value)}
            sx={{ flex: 1 }}
          />
          <TextField
            size="small"
            label="Value"
            value={value}
            onChange={(event) => handleValueChange(index, event.target.value)}
            sx={{ flex: 1 }}
          />
          <Button color="error" size="small" onClick={() => removeEntry(key)}>
            Remove
          </Button>
        </Stack>
      ))}
      <Button variant="outlined" onClick={addMetadata} size="small">
        Add metadata
      </Button>
    </Stack>
  );
}

function NodeInspector({ node, onChange, onDelete }: { node: EditorNode; onChange: InspectorProps['onNodeChange']; onDelete: (id: string) => void }) {
  const [parametersText, setParametersText] = useState('');
  const [parametersError, setParametersError] = useState<string | null>(null);

  useEffect(() => {
    const json = node.data.parameters && Object.keys(node.data.parameters).length > 0 ? JSON.stringify(node.data.parameters, null, 2) : '';
    setParametersText(json);
    setParametersError(null);
  }, [node.id, node.data.parameters]);

  const expectedAnswersText = (node.data.expectedAnswers ?? []).join('\n');

  return (
    <Stack spacing={2} className="inspector-section">
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Typography variant="h6">Node Inspector</Typography>
        <Button color="error" onClick={() => onDelete(node.id)}>
          Delete
        </Button>
      </Box>
      {node.type === 'question' && (
        <TextField
          label="Question"
          multiline
          minRows={3}
          value={node.data.question ?? ''}
          onChange={(event) => onChange(node.id, { question: event.target.value })}
        />
      )}
      {node.type === 'message' && (
        <TextField
          label="Message"
          multiline
          minRows={3}
          value={node.data.message ?? ''}
          onChange={(event) => onChange(node.id, { message: event.target.value })}
        />
      )}
      {node.type === 'action' && (
        <TextField
          label="Action"
          value={node.data.action ?? ''}
          onChange={(event) => onChange(node.id, { action: event.target.value })}
        />
      )}
      <TextField
        label="Check"
        value={node.data.check ?? ''}
        onChange={(event) => onChange(node.id, { check: event.target.value })}
      />
      <TextField
        label="Expected Answers"
        multiline
        minRows={3}
        helperText="One answer per line"
        value={expectedAnswersText}
        onChange={(event) => {
          const answers = event.target.value
            .split('\n')
            .map((value) => value.trim())
            .filter((value) => value.length > 0);
          onChange(node.id, { expectedAnswers: answers });
        }}
      />
      <TextField
        label="Parameters (JSON)"
        multiline
        minRows={4}
        value={parametersText}
        error={Boolean(parametersError)}
        helperText={parametersError ?? 'Provide JSON object for tool parameters'}
        onChange={(event) => setParametersText(event.target.value)}
        onBlur={() => {
          if (!parametersText.trim()) {
            onChange(node.id, { parameters: {} });
            setParametersError(null);
            return;
          }
          try {
            const parsed = JSON.parse(parametersText);
            onChange(node.id, { parameters: parsed });
            setParametersError(null);
          } catch (error) {
            setParametersError('Invalid JSON');
          }
        }}
      />
      <TextField
        label="Severity"
        value={node.data.severity ?? ''}
        onChange={(event) => onChange(node.id, { severity: event.target.value })}
      />
      <Box>
        <Typography variant="subtitle2" gutterBottom>
          Metadata
        </Typography>
        <MetadataEditor
          metadata={node.data.metadata}
          onChange={(value) => onChange(node.id, { metadata: value })}
        />
      </Box>
    </Stack>
  );
}

function EdgeInspector({ edge, onChange, onDelete }: { edge: EditorEdge; onChange: InspectorProps['onEdgeChange']; onDelete: (id: string) => void }) {
  const metadata = edge.data?.metadata ?? {};
  return (
    <Stack spacing={2} className="inspector-section">
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Typography variant="h6">Edge Inspector</Typography>
        <Button color="error" onClick={() => onDelete(edge.id)}>
          Delete
        </Button>
      </Box>
      <TextField
        label="Label"
        value={edge.data?.label ?? ''}
        onChange={(event) => onChange(edge.id, { label: event.target.value })}
      />
      <FormControl fullWidth>
        <InputLabel id="edge-style-label">Style</InputLabel>
        <Select
          labelId="edge-style-label"
          label="Style"
          value={edge.data?.style ?? 'default'}
          onChange={(event) => onChange(edge.id, { style: event.target.value as FlowEdgeData['style'] })}
        >
          <MenuItem value="default">Default</MenuItem>
          <MenuItem value="success">Success</MenuItem>
          <MenuItem value="warning">Warning</MenuItem>
          <MenuItem value="danger">Danger</MenuItem>
        </Select>
      </FormControl>
      <Box>
        <Typography variant="subtitle2" gutterBottom>
          Metadata
        </Typography>
        <MetadataEditor
          metadata={metadata}
          onChange={(value) => onChange(edge.id, { metadata: value })}
        />
      </Box>
    </Stack>
  );
}

export default function Inspector({
  selectedNode,
  selectedEdge,
  onNodeChange,
  onEdgeChange,
  onDeleteNode,
  onDeleteEdge
}: InspectorProps) {
  return (
    <div className="sidebar-section">
      {selectedNode ? (
        <NodeInspector node={selectedNode} onChange={onNodeChange} onDelete={onDeleteNode} />
      ) : selectedEdge ? (
        <EdgeInspector edge={selectedEdge} onChange={onEdgeChange} onDelete={onDeleteEdge} />
      ) : (
        <Box textAlign="center" py={4} px={2}>
          <Typography variant="subtitle1" gutterBottom>
            Select a node or edge
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Use the palette to add nodes and click on edges to edit labels, styles, and metadata.
          </Typography>
        </Box>
      )}
      {(selectedNode || selectedEdge) && <Divider sx={{ mt: 3 }} />}
    </div>
  );
}
