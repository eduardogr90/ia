import { Box } from '@mui/material';
import { memo, useCallback, useMemo, useState } from 'react';
import { BaseEdge, EdgeLabelRenderer, EdgeProps, getBezierPath } from 'reactflow';
import { useEditorStore } from '../../lib/store';
import { FlowEdgeData } from '../../lib/schema';

const EDGE_COLORS: Record<string, string> = {
  default: '#64748b',
  success: '#16a34a',
  warning: '#f59e0b',
  danger: '#dc2626'
};

function EdgeWithLabel(props: EdgeProps<FlowEdgeData>) {
  const { id, data, selected } = props;
  const [editing, setEditing] = useState(false);
  const updateEdge = useEditorStore((state) => state.updateEdgeData);
  const setSelection = useEditorStore((state) => state.setSelection);

  const [path, labelX, labelY] = getBezierPath(props);
  const color = useMemo(() => EDGE_COLORS[data?.style ?? 'default'] ?? EDGE_COLORS.default, [data?.style]);

  const handleDoubleClick = useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      event.stopPropagation();
      setSelection(undefined, id);
      setEditing(true);
    },
    [id, setSelection]
  );

  const handleClick = useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      event.stopPropagation();
      setSelection(undefined, id);
    },
    [id, setSelection]
  );

  const label = data?.label ?? '';

  return (
    <>
      <BaseEdge path={path} style={{ stroke: color, strokeWidth: selected ? 3 : 2 }} />
      <EdgeLabelRenderer>
        <Box
          onDoubleClick={handleDoubleClick}
          onClick={handleClick}
          sx={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            pointerEvents: 'all',
            backgroundColor: 'rgba(255,255,255,0.85)',
            borderRadius: 1,
            border: selected ? '1px solid #6366f1' : '1px solid rgba(148, 163, 184, 0.6)',
            minWidth: 60,
            px: 1,
            py: 0.5,
            boxShadow: selected ? '0 0 0 2px rgba(99,102,241,0.1)' : 'none'
          }}
        >
          {editing ? (
            <input
              className="label-input"
              autoFocus
              value={label}
              onChange={(event) => updateEdge(id, { label: event.target.value })}
              onBlur={() => setEditing(false)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  setEditing(false);
                }
              }}
            />
          ) : (
            <span>{label || 'label'}</span>
          )}
        </Box>
      </EdgeLabelRenderer>
    </>
  );
}

export default memo(EdgeWithLabel);
