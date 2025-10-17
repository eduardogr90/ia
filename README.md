# IA Data Copilot

Aplicación web en Flask que actúa como copiloto de análisis de datos. El sistema
coordina varios agentes de CrewAI para interpretar preguntas en lenguaje
natural, generar SQL sobre BigQuery, ejecutar las consultas de forma segura y
redactar el análisis final con Gemini (Vertex AI). Las conversaciones y metadatos
se almacenan en disco mediante ficheros JSON.

## Arquitectura

```
data-copilot/
├── app.py                 # Entrypoint Flask y rutas HTTP
├── config/                # Configuración y utilidades de entorno
├── crew/                  # Orquestador y agentes de CrewAI
├── data/                  # Metadatos del modelo y conversaciones de ejemplo
├── services/              # Clientes externos (BigQuery, Gemini, almacenamiento JSON)
├── static/ y templates/   # Recursos front-end
└── requirements.txt       # Dependencias específicas del proyecto
```

### Flujo principal de los agentes

1. **Interpreter Agent**: decide si se necesita SQL y detecta la semántica de la
   pregunta.
2. **SQL Generator Agent**: crea la consulta apoyándose en los metadatos del
   dataset.
3. **Validator Agent**: analiza la consulta y aplica reglas de seguridad
   determinísticas.
4. **Executor Agent**: ejecuta la consulta en BigQuery con un límite de filas
   controlado.
5. **Analyzer Agent**: resume los resultados y genera insights narrativos con
   Gemini, incluyendo recomendaciones de visualización cuando aplica.

## Requisitos previos

- Python 3.11+
- Cuenta de servicio con permisos en BigQuery y Vertex AI
- Credenciales locales en formato JSON (ver siguiente sección)

Para instalar dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r data-copilot/requirements.txt
```

## Configuración de credenciales

La aplicación necesita credenciales de Google Cloud para BigQuery y Vertex AI.
Sigue estos pasos antes de ejecutar cualquier comando que interactúe con los
servicios:

### BigQuery

1. Descarga el JSON del service account con permisos de BigQuery.
2. Guarda el archivo en `data-copilot/config/bq_service_account.json`. También
   puedes establecer la variable de entorno `BIGQUERY_CREDENTIALS_PATH` con la
   ruta o `BIGQUERY_CREDENTIALS_JSON` con el contenido completo del JSON.

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

## Ejecución de la aplicación

```bash
cd data-copilot
export FLASK_APP=app.py
flask run --reload
```

Abre `http://127.0.0.1:5000/` y autentícate con las credenciales definidas en
`data-copilot/data/users.json`. Cada conversación nueva se almacena en
`data-copilot/data/conversations/<usuario>/<id>.json`.

## Pruebas y herramientas de calidad

El proyecto no incluye una suite de pruebas automatizadas, pero se recomiendan
los siguientes comandos para auditorías rápidas:

```bash
# Analizar imports y código no utilizado
python -m vulture data-copilot

# Formatear y ordenar imports
black data-copilot
isort data-copilot

# Generar documentación HTML con pdoc
pdoc --html data-copilot --output-dir docs
```

## Soporte y mantenimiento

- Actualiza las credenciales cuando cambie algún secreto o cuenta de servicio.
- Revisa `data-copilot/crew/orchestrator.py` para ajustar reglas de orquestación
  o costes por token.
- Los metadatos que alimentan a los agentes residen en `data-copilot/data/model`;
  cualquier cambio de esquema debe reflejarse allí.
