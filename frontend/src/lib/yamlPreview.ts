import { FlowEdgePayload, FlowModel, FlowNodePayload } from './schema';

const NODE_TYPE_ORDER: Record<string, number> = { question: 0, action: 1, message: 2 };

function quote(value: string): string {
  if (value === '') return "''";
  if (/^[a-zA-Z0-9_]+$/.test(value)) {
    return value;
  }
  return JSON.stringify(value);
}

function indent(level: number): string {
  return '  '.repeat(level);
}

function formatArray(values: unknown[], level: number): string[] {
  const lines: string[] = [];
  values.forEach((item) => {
    if (item === undefined || item === null) {
      return;
    }
    if (Array.isArray(item)) {
      lines.push(`${indent(level)}-`);
      lines.push(...formatArray(item, level + 1));
    } else if (typeof item === 'object') {
      lines.push(`${indent(level)}-`);
      lines.push(...formatObject(item as Record<string, unknown>, level + 1));
    } else {
      lines.push(`${indent(level)}- ${quote(String(item))}`);
    }
  });
  return lines;
}

function formatObject(obj: Record<string, unknown>, level: number): string[] {
  const lines: string[] = [];
  const keys = Object.keys(obj).sort();
  keys.forEach((key) => {
    const value = obj[key];
    if (value === undefined || value === null) {
      return;
    }
    if (Array.isArray(value)) {
      if (value.length === 0) return;
      lines.push(`${indent(level)}${key}:`);
      lines.push(...formatArray(value, level + 1));
    } else if (typeof value === 'object') {
      lines.push(`${indent(level)}${key}:`);
      lines.push(...formatObject(value as Record<string, unknown>, level + 1));
    } else {
      lines.push(`${indent(level)}${key}: ${quote(String(value))}`);
    }
  });
  return lines;
}

function formatMetadata(metadata: Record<string, unknown> | undefined, level: number): string[] {
  if (!metadata || Object.keys(metadata).length === 0) {
    return [];
  }
  return [`${indent(level)}metadata:`, ...formatObject(metadata, level + 1)];
}

function sortNodes(nodes: FlowNodePayload[]): FlowNodePayload[] {
  return [...nodes].sort((a, b) => {
    const orderA = NODE_TYPE_ORDER[a.type] ?? Number.MAX_SAFE_INTEGER;
    const orderB = NODE_TYPE_ORDER[b.type] ?? Number.MAX_SAFE_INTEGER;
    if (orderA !== orderB) {
      return orderA - orderB;
    }
    return a.id.localeCompare(b.id);
  });
}

function formatNextSection(edges: FlowEdgePayload[], level: number): string[] {
  if (!edges || edges.length === 0) {
    return [];
  }
  const labelled = edges.filter((edge) => edge.viaLabel);
  if (edges.length === 1 && labelled.length === 0) {
    return [`${indent(level)}next: ${quote(edges[0].target)}`];
  }
  const lines = [`${indent(level)}next:`];
  const sorted = [...edges].sort((a, b) => {
    const targetCompare = a.target.localeCompare(b.target);
    if (targetCompare !== 0) return targetCompare;
    return (a.viaLabel ?? '').localeCompare(b.viaLabel ?? '');
  });
  sorted.forEach((edge) => {
    const label = edge.viaLabel && edge.viaLabel.trim().length > 0 ? edge.viaLabel : 'default';
    lines.push(`${indent(level + 1)}${quote(label)}: ${quote(edge.target)}`);
  });
  return lines;
}

function formatQuestionNode(node: FlowNodePayload, edges: FlowEdgePayload[], level: number): string[] {
  const lines: string[] = [`${indent(level)}type: question`];
  if (node.data.question) {
    lines.push(`${indent(level)}question: ${quote(node.data.question)}`);
  }
  if (node.data.check) {
    lines.push(`${indent(level)}check: ${quote(node.data.check)}`);
  }
  if (node.data.expectedAnswers && node.data.expectedAnswers.length > 0) {
    lines.push(`${indent(level)}expected_answers:`);
    lines.push(...node.data.expectedAnswers.map((answer) => `${indent(level + 1)}- ${quote(answer)}`));
  }
  lines.push(...formatNextSection(edges, level));
  lines.push(...formatMetadata(node.data.metadata, level));
  return lines;
}

function formatActionNode(node: FlowNodePayload, edges: FlowEdgePayload[], level: number): string[] {
  const lines: string[] = [`${indent(level)}type: action`];
  if (node.data.action) {
    lines.push(`${indent(level)}action: ${quote(node.data.action)}`);
  }
  if (node.data.parameters && Object.keys(node.data.parameters).length > 0) {
    lines.push(`${indent(level)}parameters:`);
    lines.push(...formatObject(node.data.parameters as Record<string, unknown>, level + 1));
  }
  lines.push(...formatNextSection(edges, level));
  lines.push(...formatMetadata(node.data.metadata, level));
  return lines;
}

function formatMessageNode(node: FlowNodePayload, edges: FlowEdgePayload[], level: number): string[] {
  const lines: string[] = [`${indent(level)}type: message`];
  if (node.data.message) {
    lines.push(`${indent(level)}message: ${quote(node.data.message)}`);
  }
  if (node.data.severity) {
    lines.push(`${indent(level)}severity: ${quote(node.data.severity)}`);
  }
  lines.push(...formatMetadata(node.data.metadata, level));
  lines.push(...formatNextSection(edges, level));
  return lines;
}

function formatNode(node: FlowNodePayload, edges: FlowEdgePayload[]): string[] {
  const level = 2;
  switch (node.type) {
    case 'question':
      return formatQuestionNode(node, edges, level);
    case 'action':
      return formatActionNode(node, edges, level);
    default:
      return formatMessageNode(node, edges, level);
  }
}

export function flowToYamlPreview(flow: FlowModel): string {
  const lines: string[] = [];
  lines.push(`id: ${quote(flow.id)}`);
  lines.push(`name: ${quote(flow.name)}`);
  if (flow.metadata && Object.keys(flow.metadata).length > 0) {
    lines.push('metadata:');
    lines.push(...formatObject(flow.metadata as Record<string, unknown>, 1));
  }
  lines.push('flow:');

  const sortedNodes = sortNodes(flow.nodes ?? []);
  sortedNodes.forEach((node) => {
    lines.push(`${indent(1)}${node.id}:`);
    const outgoing = (flow.edges ?? []).filter((edge) => edge.source === node.id);
    lines.push(...formatNode(node, outgoing));
  });

  return lines.join('\n');
}
