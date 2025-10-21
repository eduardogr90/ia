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
import { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { createProject, deleteProject, listProjects, renameProject } from '../lib/api';
import { ProjectSummary } from '../lib/schema';

export default function ProjectsPage() {
  const [projects, setProjects] = useState<ProjectSummary[]>([]);
  const [newProject, setNewProject] = useState('');
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [nameEdits, setNameEdits] = useState<Record<string, string>>({});
  const navigate = useNavigate();

  const loadProjects = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listProjects();
      setProjects(data);
      setNameEdits(Object.fromEntries(data.map((project) => [project.id, project.name])));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadProjects();
  }, [loadProjects]);

  const handleCreate = async () => {
    if (!newProject.trim()) return;
    setCreating(true);
    try {
      await createProject(newProject.trim());
      setNewProject('');
      await loadProjects();
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (id: string) => {
    await deleteProject(id);
    await loadProjects();
  };

  const handleRename = async (id: string, name: string) => {
    const trimmed = name.trim();
    if (!trimmed) {
      setNameEdits((prev) => ({ ...prev, [id]: projects.find((project) => project.id === id)?.name ?? '' }));
      return;
    }
    await renameProject(id, trimmed);
    await loadProjects();
  };

  return (
    <Box className="app-shell">
      <Box p={4} display="flex" justifyContent="space-between" alignItems="center">
        <Typography variant="h4">Projects</Typography>
        <Stack direction="row" spacing={2} alignItems="center">
          <TextField
            label="New project"
            value={newProject}
            onChange={(event) => setNewProject(event.target.value)}
            size="small"
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
        ) : projects.length === 0 ? (
          <Box textAlign="center" py={10} bgcolor="#fff" borderRadius={3}>
            <Typography variant="h6" gutterBottom>
              No projects yet
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Create your first project to start modelling orchestrations.
            </Typography>
          </Box>
        ) : (
          <Grid container spacing={3}>
            {projects.map((project) => (
              <Grid item xs={12} md={6} lg={4} key={project.id}>
                <Card>
                  <CardContent>
                    <TextField
                      label="Project name"
                      value={nameEdits[project.id] ?? project.name}
                      variant="standard"
                      fullWidth
                      onChange={(event) =>
                        setNameEdits((prev) => ({ ...prev, [project.id]: event.target.value }))
                      }
                      onBlur={(event) => {
                        if ((nameEdits[project.id] ?? project.name) !== project.name) {
                          void handleRename(project.id, event.target.value);
                        }
                      }}
                    />
                    <Typography variant="caption" color="text.secondary">
                      ID: {project.id}
                    </Typography>
                  </CardContent>
                  <CardActions sx={{ justifyContent: 'space-between' }}>
                    <Button size="small" onClick={() => navigate(`/projects/${project.id}/flows`)}>
                      Open
                    </Button>
                    <Button color="error" size="small" onClick={() => handleDelete(project.id)}>
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
