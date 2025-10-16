# ia

## Configuración de credenciales y variables de entorno

La aplicación necesita credenciales de Google Cloud para BigQuery y Vertex AI.
Sigue estos pasos antes de ejecutar cualquier comando que interactúe con los
servicios:

### BigQuery

1. Descarga el JSON del service account con permisos de BigQuery.
2. Guarda el archivo en `data-copilot/config/bq_service_account.json`. También puedes
   establecer la variable de entorno `BIGQUERY_SERVICE_ACCOUNT_JSON` con la ruta
   o con el contenido completo del JSON (una cadena con el objeto).

### Vertex AI (Gemini)

1. Obtén el JSON del service account con permisos de Vertex AI y guárdalo en
   `data-copilot/config/json_key_vertex.json` (puedes usar el archivo de ejemplo
   `json_key_vertex.sample.json` como plantilla) o define
   `GOOGLE_APPLICATION_CREDENTIALS` con la ruta o el contenido del JSON.
2. El orquestador obtiene automáticamente el `project_id` del JSON. Solo define
   la variable de entorno `VERTEX_PROJECT_ID` si quieres sobrescribirlo.
3. (Opcional) Define `VERTEX_LOCATION` si quieres usar una región distinta a
   `us-central1`.

Puedes exportar las variables en tu terminal antes de ejecutar la aplicación:

```bash
export VERTEX_PROJECT_ID="tu-proyecto"           # Opcional si el JSON ya lo incluye
export VERTEX_LOCATION="us-central1"             # Opcional
```

Si usas un archivo `.env`, asegúrate de que incluya las mismas variables en caso
de que quieras sobreescribir la configuración derivada del JSON.
