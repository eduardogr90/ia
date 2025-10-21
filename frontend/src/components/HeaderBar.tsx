import {
  AppBar,
  Box,
  Button,
  FormControlLabel,
  IconButton,
  Stack,
  Switch,
  TextField,
  Toolbar,
  Tooltip,
  Typography
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import SaveIcon from '@mui/icons-material/Save';
import FactCheckIcon from '@mui/icons-material/FactCheck';
import GetAppIcon from '@mui/icons-material/GetApp';
import ImageIcon from '@mui/icons-material/Image';
import CenterFocusStrongIcon from '@mui/icons-material/CenterFocusStrong';
import AccountTreeIcon from '@mui/icons-material/AccountTree';

interface HeaderBarProps {
  name: string;
  dirty: boolean;
  onNameChange: (value: string) => void;
  onSave: () => void;
  onValidate: () => void;
  onExportYaml: () => void;
  onExportImage: () => void;
  onCenterView: () => void;
  onAutoLayout: () => void;
  fitToContent: boolean;
  onToggleFitToContent: (value: boolean) => void;
  onBack?: () => void;
}

export default function HeaderBar({
  name,
  dirty,
  onNameChange,
  onSave,
  onValidate,
  onExportYaml,
  onExportImage,
  onCenterView,
  onAutoLayout,
  fitToContent,
  onToggleFitToContent,
  onBack
}: HeaderBarProps) {
  return (
    <AppBar position="static" color="default" elevation={1} sx={{ borderBottom: '1px solid #e5e7eb' }}>
      <Toolbar sx={{ display: 'flex', justifyContent: 'space-between', gap: 2 }}>
        <Stack direction="row" spacing={2} alignItems="center">
          {onBack && (
            <IconButton edge="start" onClick={onBack} color="inherit">
              <ArrowBackIcon />
            </IconButton>
          )}
          <Box>
            <Typography variant="caption" color="text.secondary">
              Flow name
            </Typography>
            <TextField
              value={name}
              onChange={(event) => onNameChange(event.target.value)}
              variant="standard"
              sx={{ minWidth: 260, mr: 2 }}
            />
            {dirty && (
              <Typography component="span" variant="caption" color="warning.main" sx={{ ml: 1 }}>
                Unsaved changes
              </Typography>
            )}
          </Box>
        </Stack>
        <Stack direction="row" spacing={1} alignItems="center">
          <Tooltip title="Save (Ctrl+S)">
            <span>
              <Button variant="contained" color="primary" startIcon={<SaveIcon />} onClick={onSave}>
                Save
              </Button>
            </span>
          </Tooltip>
          <Tooltip title="Validate (Ctrl+E)">
            <Button variant="outlined" startIcon={<FactCheckIcon />} onClick={onValidate}>
              Validate
            </Button>
          </Tooltip>
          <Tooltip title="Export YAML (Ctrl+J)">
            <Button variant="outlined" startIcon={<GetAppIcon />} onClick={onExportYaml}>
              YAML
            </Button>
          </Tooltip>
          <Tooltip title="Export JPG (Ctrl+P)">
            <Button variant="outlined" startIcon={<ImageIcon />} onClick={onExportImage}>
              JPG
            </Button>
          </Tooltip>
          <FormControlLabel
            control={
              <Switch
                size="small"
                checked={fitToContent}
                onChange={(event) => onToggleFitToContent(event.target.checked)}
              />
            }
            label="Fit to content"
          />
          <Tooltip title="Center view">
            <IconButton color="primary" onClick={onCenterView}>
              <CenterFocusStrongIcon />
            </IconButton>
          </Tooltip>
          <Tooltip title="Auto layout">
            <IconButton color="primary" onClick={onAutoLayout}>
              <AccountTreeIcon />
            </IconButton>
          </Tooltip>
        </Stack>
      </Toolbar>
    </AppBar>
  );
}
