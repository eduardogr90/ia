# API Reference

Base URL: `http://localhost:5000/api`

All responses are JSON encoded. Errors return an `errors` array with human-readable messages. Authentication is not enforced for local development; add middleware before production.

## Projects

### `GET /projects`

List all projects.

```json
{
  "projects": [
    { "id": "marketing", "name": "Marketing Playbooks", "updatedAt": "2024-05-01T10:32:19Z" }
  ]
}
```

### `POST /projects`

Create a project.

Request body:

```json
{ "name": "Support Journeys" }
```

Response `201 Created`:

```json
{ "id": "support-journeys", "name": "Support Journeys", "createdAt": "2024-05-09T12:10:00Z" }
```

### `PATCH /projects/:projectId`

Rename a project. Body mirrors `POST /projects`.

### `DELETE /projects/:projectId`

Delete a project and all of its flows. Returns `204 No Content`.

## Flows

### `GET /projects/:projectId/flows`

Return flow summaries for the project.

```json
{
  "flows": [
    { "id": "onboarding", "name": "Onboarding Sequence", "updatedAt": "2024-05-02T08:22:00Z" }
  ]
}
```

### `POST /projects/:projectId/flows`

Create a new flow inside a project.

```json
{ "name": "Escalation" }
```

Response `201 Created` is a full `FlowModel`:

```json
{
  "id": "escalation",
  "name": "Escalation",
  "metadata": {},
  "nodes": [
    { "id": "start", "type": "question", "data": { "question": "How can we help?" } }
  ],
  "edges": []
}
```

### `GET /projects/:projectId/flows/:flowId`

Fetch a complete `FlowModel`.

### `PUT /projects/:projectId/flows/:flowId`

Persist the full flow payload (same schema as `GET`). Returns the saved `FlowModel` with refreshed timestamps.

### `DELETE /projects/:projectId/flows/:flowId`

Remove a flow. Returns `204 No Content`.

## Validation & Paths

### `POST /validate`

Validate a flow without persisting it.

Request body expects a `FlowModel`.

Response:

```json
{
  "valid": true,
  "errors": [],
  "warnings": [
    "Multiple start nodes detected; execution order may be ambiguous.",
    "Message node 'goodbye' has outgoing edges and will not terminate the flow."
  ],
  "paths": [
    [
      { "nodeId": "greeting" },
      { "nodeId": "identify", "via": "yes" },
      { "nodeId": "pass" }
    ],
    [
      { "nodeId": "greeting" },
      { "nodeId": "fallback", "via": "no" },
      { "nodeId": "escalate" }
    ]
  ]
}
```

Validation errors include duplicate IDs, missing references, cycle traces, invalid edge labels for question nodes, and missing terminal message nodes. The `paths` array only contains routes that finish at message nodes without outgoing edges.

## YAML Export

### `POST /export/yaml`

Transform a `FlowModel` into a deterministic YAML string.

Response:

```json
{
  "yaml": "id: escalation\nname: Escalation\nflow:\n  start:\n    type: question\n    question: How can we help?\n    next:\n      yes: qualify\n      no: goodbye\n  goodbye:\n    type: message\n    message: Thanks for reaching out\n",
  "filename": "escalation.yaml"
}
```

If validation fails before serialization the endpoint returns `400` with an `errors` array describing the payload issues.

## Error format

```json
{
  "errors": ["Flow must contain at least one terminal message node (message without outgoing edges)."],
  "warnings": [],
  "paths": []
}
```

The same structure is used by `/validate` with HTTP status `400` for Pydantic parsing problems and `200` for semantic validation issues (where `valid` is `false`).
