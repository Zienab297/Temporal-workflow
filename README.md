# Temporal Workflow: PDF Processing & AI Contract Review

A set of [Temporal](https://temporal.io/) workflows for processing PDFs stored in S3 and running an AI-powered, human-in-the-loop contract review pipeline. Includes a FastAPI gateway for triggering and monitoring workflows.

## Project structure

```
apps/
├── pdf-extractor-01/            # Standalone script: download PDF from S3, extract to Markdown, upload back
├── pdf-extractor-02-temporal/   # Same pipeline, implemented as a Temporal workflow + worker
├── ai-contract-review/          # Multi-PDF contract review: parent/child workflows, LLM synthesis, human review loop
└── client_app/                  # FastAPI gateway to start/query workflows

setup/
├── samples-server/              # Temporal server (docker-compose) setup, cloned from temporalio/docker-compose
└── services/                    # systemd service file for running Temporal as a service
```

### Workflows

- **`PDFPipelineWorkflow`** (`pdf-extractor-02-temporal`) — downloads a PDF from S3, extracts it to Markdown with `pymupdf4llm`, and uploads the result back to S3.
- **`ContractReviewWorkflow`** (`ai-contract-review`) — fans out one `PDFSummaryWorkflow` child per contract to extract and summarize it, synthesizes a combined risk report via an LLM, then waits for human review (`approve` / `revise`) with up to `max_revisions` revision cycles before completing.
- **`PDFSummaryWorkflow`** (`ai-contract-review`) — child workflow that extracts a single PDF and summarizes it (summary + key risks) via an LLM.

## Prerequisites

- Python 3.11+
- Docker and Docker Compose
- An S3-compatible bucket and credentials
- An [OpenRouter](https://openrouter.ai/) API key (for the contract review LLM calls)

## 1. Start the Temporal server

```bash
cd setup/samples-server/compose
docker compose -f docker-compose-postgres.yml up -d
```

This brings up Temporal Server, PostgreSQL, and the Temporal Web UI (default: [http://localhost:8080](http://localhost:8080)).

## 2. Configure environment variables

Each app reads its config from a `.env` file in its own directory. Create one per app with the variables below (adjust values to your setup).

**`apps/pdf-extractor-01/.env`** and **`apps/pdf-extractor-02-temporal/.env`**:

```env
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
AWS_S3_ENDPOINT_URL=
S3_BUCKET=
TEMP_DIR=

# pdf-extractor-02-temporal only
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_PDF_PROCESS_TASK_QUEUE=pdf-process-queue
```

**`apps/ai-contract-review/.env`**:

```env
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=
AWS_S3_ENDPOINT_URL=
S3_BUCKET=
TEMP_DIR=

TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=contract-review-queue

OPENROUTER_API_KEY=
OPENROUTER_MODEL=openai/gpt-4o-mini
```

**`apps/client_app/.env`**:

```env
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_PDF_PROCESS_TASK_QUEUE=pdf-process-queue
TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE=contract-review-queue
```

## 3. Install dependencies

```bash
# per app, e.g.
cd apps/ai-contract-review
pip install -r requirements.txt
```

Repeat for `pdf-extractor-01`, `pdf-extractor-02-temporal`, and `client_app`.

## 4. Run the workers

```bash
# PDF extraction worker
cd apps/pdf-extractor-02-temporal
python worker.py

# Contract review worker (separate terminal)
cd apps/ai-contract-review
python worker.py
```

## 5. Run the client (API gateway)

```bash
cd apps/client_app
uvicorn main:app --reload
```

The API is now available at `http://localhost:8000`.

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET`  | `/health` | Health check |
| `POST` | `/process-pdf/execute` | Run the PDF extraction pipeline synchronously |
| `POST` | `/contract-review/start` | Start a contract review workflow for one or more S3 PDFs |
| `GET`  | `/contract-review/{workflow_id}/status` | Poll workflow status |
| `GET`  | `/contract-review/{workflow_id}/report` | Fetch the current (or final) report |
| `POST` | `/contract-reviewer/{workflow_id}/assign` | Assign a human reviewer |
| `POST` | `/contract-review/{workflow_id}/revise` | Request a revision with feedback |
| `GET`  | `/contract-review/{workflow_id}/approve` | Approve the current report |

### Example: start a contract review

```bash
curl -X POST http://localhost:8000/contract-review/start \
  -H "Content-Type: application/json" \
  -d '{
    "s3_path": [
      "s3://temporal-dev/legal-docs/vendor-service-agreement.pdf",
      "s3://temporal-dev/legal-docs/nda-innovate-consultpro.pdf",
      "s3://temporal-dev/legal-docs/software-license-globalsoft.pdf"
    ],
    "max_revision": 2
  }'
```

Sample contract PDFs used for testing are in `apps/ai-contract-review/samples/`.

## Monitoring

Use the Temporal Web UI at `http://localhost:8080` to inspect running/completed workflows, view event history, and debug failures.

## Notes

- `setup/samples-server` is the [Temporal docker-compose](https://github.com/temporalio/docker-compose) reference setup, used here to run a local Temporal server for development.
- `setup/services/temporal.service` is an optional systemd unit for running the Temporal CLI dev server as a background service.