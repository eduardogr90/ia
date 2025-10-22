import { CssBaseline, ThemeProvider, createTheme } from '@mui/material';
import { Navigate, Route, Routes } from 'react-router-dom';
import EditorPage from './pages/EditorPage';
import FlowsPage from './pages/FlowsPage';
import ProjectsPage from './pages/ProjectsPage';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#2563eb'
    },
    secondary: {
      main: '#7c3aed'
    }
  },
  typography: {
    fontFamily: 'Inter, Roboto, Helvetica, Arial, sans-serif'
  }
});

export default function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Routes>
        <Route path="/" element={<Navigate to="/projects" replace />} />
        <Route path="/projects" element={<ProjectsPage />} />
        <Route path="/projects/:projectId/flows" element={<FlowsPage />} />
        <Route path="/projects/:projectId/flows/:flowId" element={<EditorPage />} />
        <Route path="*" element={<Navigate to="/projects" replace />} />
      </Routes>
    </ThemeProvider>
  );
}
