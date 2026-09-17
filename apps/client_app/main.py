import os
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dotenv import load_dotenv
from temporalio.client import Client

load_dotenv()

TEMPORAL_HOST = os.environ["TEMPORAL_HOST"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]
TEMPORAL_PDF_PROCESS_QUEUE = os.environ["TEMPORAL_PDF_PROCESS_TASK_QUEUE"]


app = FastAPI(
    title="PDF Extractor Client",
    description="Submit PDF processing jobs to Temporal and return results",
    version="1.0.0"
)

class ProcessPDFRequest(BaseModel):
    s3_path: str

class ProcessPDFExecuteResponse(BaseModel):
    workflow_id: str
    results: dict

class ProcessPDFStatusResponse(BaseModel):
    workflow_id: str


async def get_temporal_client() -> Client:
    return await Client.connect(
        TEMPORAL_HOST,
        namespace=TEMPORAL_NAMESPACE,
    )

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/process-pdf/execute", response_model=ProcessPDFExecuteResponse)
async def process_pdf(request: ProcessPDFRequest):

    workflow_id = f"pdf-pipeline-{uuid.uuid4}"

    client = await get_temporal_client()

    results = await client.execute_workflow(
        "PDFPipelineWorkflow",
        args=[
            {
                "s3_path": request.s3_path, 
            }
        ],
        id=workflow_id,
        task_queue=TEMPORAL_PDF_PROCESS_QUEUE,
        result_type=dict,
    )

    return ProcessPDFExecuteResponse(
        workflow_id=workflow_id,
        results=results
    )


@app.post("/process-pdf/status", response_model=ProcessPDFStatusResponse)
async def process_pdf(request: ProcessPDFRequest):

    workflow_id = f"pdf-pipeline-{uuid.uuid4}"

    client = await get_temporal_client()

    results = await client.start_workflow(
        "PDFPipelineWorkflow",
        args=[
            {
                "s3_path": request.s3_path, 
            }
        ],
        id=workflow_id,
        task_queue=TEMPORAL_PDF_PROCESS_QUEUE,
        result_type=dict,
    )

    return ProcessPDFStatusResponse(
        workflow_id=workflow_id,
        results=None
    )

@app.get("process_pdf/status/{workflow_id}")
async def get_workflow_status(workflow_id: str):
    client = await get_temporal_client()

    handle = client.get_workflow_handle(workflow_id=workflow_id)

    desc = await handle.describe()

    try:
        result = handle.result()
    except:
        result = None
    status = desc.status

    return {
        "workflow_id": workflow_id,
        "workflow status": status.name,
        "workflow_result": result
    }