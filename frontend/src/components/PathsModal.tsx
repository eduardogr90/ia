import {
  Box,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Stack,
  Typography,
  Button
} from '@mui/material';
import { ValidationResult } from '../lib/schema';

interface PathsModalProps {
  open: boolean;
  onClose: () => void;
  result?: ValidationResult | null;
}

export default function PathsModal({ open, onClose, result }: PathsModalProps) {
  const errors = result?.errors ?? [];
  const warnings = result?.warnings ?? [];
  const paths = result?.paths ?? [];

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Validation Results</DialogTitle>
      <DialogContent dividers>
        <Stack spacing={3} className="paths-list">
          <Box>
            <Typography variant="subtitle1" gutterBottom>
              Status
            </Typography>
            <Chip
              label={result?.valid ? 'Valid flow' : 'Validation issues detected'}
              color={result?.valid ? 'success' : 'error'}
              variant="outlined"
            />
          </Box>
          <Box>
            <Typography variant="subtitle1" gutterBottom>
              Errors
            </Typography>
            {errors.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No blocking errors.
              </Typography>
            ) : (
              <ul>
                {errors.map((error) => (
                  <li key={error}>
                    <Typography variant="body2">{error}</Typography>
                  </li>
                ))}
              </ul>
            )}
          </Box>
          <Divider />
          <Box>
            <Typography variant="subtitle1" gutterBottom>
              Warnings
            </Typography>
            {warnings.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No warnings.
              </Typography>
            ) : (
              <ul>
                {warnings.map((warning) => (
                  <li key={warning}>
                    <Typography variant="body2">{warning}</Typography>
                  </li>
                ))}
              </ul>
            )}
          </Box>
          <Divider />
          <Box>
            <Typography variant="subtitle1" gutterBottom>
              Paths
            </Typography>
            {paths.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No reachable paths could be determined.
              </Typography>
            ) : (
              <Stack spacing={1.5}>
                {paths.map((path, index) => (
                  <Box key={index} p={1.5} borderRadius={2} bgcolor="rgba(99,102,241,0.08)">
                    <Typography variant="caption" color="text.secondary">
                      Path {index + 1}
                    </Typography>
                    <Typography variant="body2">
                      {path.map((step) => (step.via ? `${step.nodeId} (${step.via})` : step.nodeId)).join(' → ')}
                    </Typography>
                  </Box>
                ))}
              </Stack>
            )}
          </Box>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} variant="contained">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
}
