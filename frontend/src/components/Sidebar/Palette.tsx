import { Button, Stack, Typography } from '@mui/material';
import AddCircleIcon from '@mui/icons-material/AddCircle';
import { NodeKind } from '../../lib/schema';

interface PaletteProps {
  onAddNode: (type: NodeKind) => void;
}

const paletteItems: Array<{ type: NodeKind; label: string; description: string; color: string }> = [
  { type: 'question', label: 'Question', description: 'Prompt the agent', color: '#2563eb' },
  { type: 'action', label: 'Action', description: 'Invoke tools or APIs', color: '#7c3aed' },
  { type: 'message', label: 'Message', description: 'Send response or info', color: '#16a34a' }
];

export default function Palette({ onAddNode }: PaletteProps) {
  return (
    <div className="sidebar-section">
      <Typography variant="h6" gutterBottom>
        Palette
      </Typography>
      <Stack spacing={1.5}>
        {paletteItems.map((item) => (
          <Button
            key={item.type}
            variant="contained"
            startIcon={<AddCircleIcon />}
            onClick={() => onAddNode(item.type)}
            sx={{
              justifyContent: 'flex-start',
              backgroundColor: item.color,
              '&:hover': { backgroundColor: item.color }
            }}
          >
            <Stack direction="column" alignItems="flex-start">
              <Typography variant="subtitle2">{item.label}</Typography>
              <Typography variant="caption" sx={{ opacity: 0.8 }}>
                {item.description}
              </Typography>
            </Stack>
          </Button>
        ))}
      </Stack>
    </div>
  );
}
