import { Avatar, Box, Typography } from '@mui/material';
import BoltIcon from '@mui/icons-material/Bolt';
import { memo } from 'react';
import { Handle, Position } from 'reactflow';

function ActionNode({ data }: { data: { action?: string; parameters?: Record<string, unknown> } }) {
  return (
    <Box
      sx={{
        p: 2,
        borderRadius: 3,
        backgroundColor: '#7c3aed',
        color: 'white',
        minWidth: 200,
        boxShadow: '0 15px 30px rgba(124, 58, 237, 0.25)'
      }}
    >
      <Handle type="target" position={Position.Left} style={{ background: '#5b21b6' }} />
      <Box display="flex" alignItems="center" mb={1} className="node-drag-handle" sx={{ cursor: 'grab' }}>
        <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)', mr: 1, width: 32, height: 32 }}>
          <BoltIcon fontSize="small" />
        </Avatar>
        <Typography variant="subtitle2" sx={{ textTransform: 'uppercase', letterSpacing: 1 }}>
          Action
        </Typography>
      </Box>
      <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap', fontWeight: 600 }}>
        {data.action || 'Action name'}
      </Typography>
      {data.parameters && Object.keys(data.parameters).length > 0 && (
        <Typography variant="caption" sx={{ mt: 1, display: 'block', opacity: 0.85 }}>
          Params: {JSON.stringify(data.parameters)}
        </Typography>
      )}
      <Handle type="source" position={Position.Right} style={{ background: '#a855f7' }} />
    </Box>
  );
}

export default memo(ActionNode);
