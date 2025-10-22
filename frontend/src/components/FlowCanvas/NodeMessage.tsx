import { Avatar, Box, Typography } from '@mui/material';
import ChatBubbleIcon from '@mui/icons-material/ChatBubble';
import { memo } from 'react';
import { Handle, Position } from 'reactflow';

function MessageNode({ data }: { data: { message?: string; severity?: string } }) {
  const badgeColor = data.severity === 'high' ? '#dc2626' : data.severity === 'warning' ? '#f59e0b' : '#15803d';
  return (
    <Box
      sx={{
        p: 2,
        borderRadius: 3,
        backgroundColor: '#22c55e',
        color: '#064e3b',
        minWidth: 200,
        boxShadow: '0 15px 30px rgba(34, 197, 94, 0.25)'
      }}
    >
      <Handle type="target" position={Position.Top} style={{ background: '#16a34a' }} />
      <Box display="flex" alignItems="center" mb={1} className="node-drag-handle" sx={{ cursor: 'grab' }}>
        <Avatar sx={{ bgcolor: 'rgba(6, 78, 59, 0.15)', mr: 1, width: 32, height: 32 }}>
          <ChatBubbleIcon fontSize="small" />
        </Avatar>
        <Typography variant="subtitle2" sx={{ textTransform: 'uppercase', letterSpacing: 1 }}>
          Message
        </Typography>
      </Box>
      <Typography variant="body2" sx={{ whiteSpace: 'pre-wrap' }}>
        {data.message || 'Response text'}
      </Typography>
      {data.severity && (
        <Box mt={1} display="inline-flex" alignItems="center" px={1} py={0.5} borderRadius={2} bgcolor={badgeColor} color="#fff" fontSize="0.7rem">
          {data.severity}
        </Box>
      )}
      <Handle type="source" position={Position.Bottom} style={{ background: '#4ade80' }} />
    </Box>
  );
}

export default memo(MessageNode);
