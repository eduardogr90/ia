import {
  Box,
  Button,
  Card,
  CardActions,
  CardContent,
  CircularProgress,
  Grid,
  Stack,
  TextField,
  Typography
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { useCallback, useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { createFlow, deleteFlow, listFlows, listProjects } from '../lib/api';
import { FlowSummary, ProjectSummary } from '../lib/schema';

export default function FlowsPage() {
  const { projectId } = useParams<{ projectId: string }>();
  const navigate = useNavigate();
  const [flows, setFlows] = useState<FlowSummary[]>([]);
  const [project, setProject] = useState<ProjectSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [newFlow, setNewFlow] = useState('');

  const loadData = useCallback(async () => {
    if (!projectId) return;
    setLoading(true);
    try {
      const [projects, projectFlows] = await Promise.all([listProjects(), listFlows(projectId)]);
      setProject(projects.find((item) => item.id === projectId) ?? null);
      setFlows(projectFlows);
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  const handleCreate = async () => {
    if (!projectId || !newFlow.trim()) return;
    setCreating(true);
    try {
      await createFlow(projectId, newFlow.trim());
      setNewFlow('');
      await loadData();
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (flowId: string) => {
    if (!projectId) return;
    await deleteFlow(projectId, flowId);
    await loadData();
  };

  return (
    <Box className="app-shell">
      <Box p={4} display="flex" justifyContent="space-between" alignItems="center">
        <Stack direction="row" spacing={2} alignItems="center">
          <Button startIcon={<ArrowBackIcon />} onClick={() => navigate('/projects')}>
            Back
          </Button>
          <Typography variant="h4">{project?.name ?? projectId}</Typography>
        </Stack>
        <Stack direction="row" spacing={2} alignItems="center">
          <TextField
            label="New flow"
            size="small"
            value={newFlow}
            onChange={(event) => setNewFlow(event.target.value)}
          />
          <Button variant="contained" onClick={handleCreate} disabled={creating}>
            Create
          </Button>
        </Stack>
      </Box>
      <Box px={4} pb={4}>
        {loading ? (
          <Box display="flex" justifyContent="center" py={6}>
            <CircularProgress />
          </Box>
        ) : flows.length === 0 ? (
          <Box textAlign="center" py={10} bgcolor="#fff" borderRadius={3}>
            <Typography variant="h6" gutterBottom>
              No flows yet
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Create a flow to start building orchestrations.
            </Typography>
          </Box>
        ) : (
          <Grid container spacing={3}>
            {flows.map((flow) => (
              <Grid item xs={12} md={6} lg={4} key={flow.id}>
                <Card>
                  <CardContent>
                    <Typography variant="h6">{flow.name}</Typography>
                    <Typography variant="caption" color="text.secondary">
                      Updated: {flow.updatedAt ?? '—'}
                    </Typography>
                  </CardContent>
                  <CardActions sx={{ justifyContent: 'space-between' }}>
                    <Button size="small" onClick={() => navigate(`/projects/${projectId}/flows/${flow.id}`)}>
                      Open editor
                    </Button>
                    <Button color="error" size="small" onClick={() => handleDelete(flow.id)}>
                      Delete
                    </Button>
                  </CardActions>
                </Card>
              </Grid>
            ))}
          </Grid>
        )}
      </Box>
    </Box>
  );
}
