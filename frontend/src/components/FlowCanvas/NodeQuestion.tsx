import { Avatar, Box, Typography } from '@mui/material';
import LiveHelpIcon from '@mui/icons-material/LiveHelp';
import { memo } from 'react';
import { Handle, Position } from 'reactflow';

function QuestionNode({ data }: { data: { question?: string } }) {
  return (
    <Box
      sx={{
        p: 2,
        borderRadius: 3,
        backgroundColor: '#2563eb',
        color: 'white',
        minWidth: 200,
        boxShadow: '0 15px 30px rgba(37, 99, 235, 0.25)'
      }}
    >
      <Handle type="target" position={Position.Top} style={{ background: '#1d4ed8' }} />
      <Box display="flex" alignItems="center" mb={1} className="node-drag-handle" sx={{ cursor: 'grab' }}>
        <Avatar sx={{ bgcolor: 'rgba(255,255,255,0.2)', mr: 1, width: 32, height: 32 }}>
          <LiveHelpIcon fontSize="small" />
        </Avatar>
        <Typography variant="subtitle2" sx={{ textTransform: 'uppercase', letterSpacing: 1 }}>
          Question
        </Typography>
      </Box>
      <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
        {data.question || 'Add your question prompt'}
      </Typography>
      <Handle type="source" position={Position.Bottom} style={{ background: '#60a5fa' }} />
    </Box>
  );
}

export default memo(QuestionNode);
