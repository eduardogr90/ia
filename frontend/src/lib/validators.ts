import { FlowEdgePayload, FlowModel, FlowNodePayload } from './schema';

function validateNode(node: FlowNodePayload, errors: string[], warnings: string[]) {
  if (!node.type) {
    errors.push(`Node ${node.id} is missing a type.`);
  }
  if ((node.type === 'question' || node.type === 'message') && !node.data.message && !node.data.question) {
    warnings.push(`Node ${node.id} does not define any message or question text.`);
  }
  if (node.type === 'action' && !node.data.action) {
    warnings.push(`Action node ${node.id} does not define an action name.`);
  }
}

function validateEdge(edge: FlowEdgePayload, errors: string[]) {
  if (!edge.source || !edge.target) {
    errors.push('All edges must define a source and target.');
  }
}

export function quickValidateFlow(flow: FlowModel): { errors: string[]; warnings: string[] } {
  const errors: string[] = [];
  const warnings: string[] = [];

  const nodeIds = new Set<string>();
  flow.nodes.forEach((node) => {
    if (nodeIds.has(node.id)) {
      errors.push(`Duplicate node identifier detected: ${node.id}`);
    }
    nodeIds.add(node.id);
    validateNode(node, errors, warnings);
  });

  const missingNodeEdges = flow.edges.filter(
    (edge) => !nodeIds.has(edge.source) || !nodeIds.has(edge.target)
  );
  if (missingNodeEdges.length > 0) {
    errors.push('Some edges reference nodes that are not present in the model.');
  }

  flow.edges.forEach((edge) => validateEdge(edge, errors));

  if (flow.nodes.length === 0) {
    errors.push('Flow must contain at least one node.');
  }

  return { errors: Array.from(new Set(errors)), warnings: Array.from(new Set(warnings)) };
}
