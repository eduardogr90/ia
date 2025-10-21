import axios from 'axios';
import {
  FlowModel,
  FlowSummary,
  ProjectSummary,
  ValidationResult
} from './schema';

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:5000/api';

const client = axios.create({
  baseURL: API_BASE,
  headers: {
    'Content-Type': 'application/json'
  }
});

export async function listProjects(): Promise<ProjectSummary[]> {
  const { data } = await client.get<{ projects: ProjectSummary[] }>('/projects');
  return data.projects;
}

export async function createProject(name: string): Promise<ProjectSummary> {
  const { data } = await client.post<ProjectSummary>('/projects', { name });
  return data;
}

export async function renameProject(projectId: string, name: string): Promise<ProjectSummary> {
  const { data } = await client.patch<ProjectSummary>(`/projects/${projectId}`, { name });
  return data;
}

export async function deleteProject(projectId: string): Promise<void> {
  await client.delete(`/projects/${projectId}`);
}

export async function listFlows(projectId: string): Promise<FlowSummary[]> {
  const { data } = await client.get<{ flows: FlowSummary[] }>(`/projects/${projectId}/flows`);
  return data.flows;
}

export async function createFlow(projectId: string, name: string): Promise<FlowModel> {
  const { data } = await client.post<FlowModel>(`/projects/${projectId}/flows`, { name });
  return data;
}

export async function deleteFlow(projectId: string, flowId: string): Promise<void> {
  await client.delete(`/projects/${projectId}/flows/${flowId}`);
}

export async function loadFlow(projectId: string, flowId: string): Promise<FlowModel> {
  const { data } = await client.get<FlowModel>(`/projects/${projectId}/flows/${flowId}`);
  return data;
}

export async function saveFlow(projectId: string, flowId: string, payload: FlowModel): Promise<FlowModel> {
  const { data } = await client.put<FlowModel>(`/projects/${projectId}/flows/${flowId}`, payload);
  return data;
}

export async function validateFlowModel(payload: FlowModel): Promise<ValidationResult> {
  const { data } = await client.post<ValidationResult>('/validate', payload);
  return data;
}

export async function exportFlowAsYaml(payload: FlowModel): Promise<{ yaml: string; filename: string }> {
  const { data } = await client.post<{ yaml: string; filename: string }>(`/export/yaml`, payload);
  return data;
}
