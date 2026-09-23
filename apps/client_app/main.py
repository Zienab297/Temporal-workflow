import os
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.client import WorkflowExecutionStatus as WES

load_dotenv()

TEMPORAL_HOST = os.environ["TEMPORAL_HOST"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]
TEMPORAL_PDF_PROCESS_QUEUE = os.environ["TEMPORAL_PDF_PROCESS_TASK_QUEUE"]
TEMPORAL_PDF_PROCESS_QUEUE = os.environ["TEMPORAL_PDF_PROCESS_TASK_QUEUE"]
TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE = os.environ["TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE"]


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

class StartReviewRequest(BaseModel):
    s3_path: list[str]
    max_revision: int = 2

class AssignRequest:
    name: str

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


@app.post("/workflow/status", response_model=ProcessPDFStatusResponse)
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


@app.post("/contract-review/start")
async def start_contrct_review(request: StartReviewRequest):
    workflow_id = f"content-review-{uuid.uuid4()}"

    client = await get_temporal_client()

    await client.start_workflow(
        "ContractReviewWorkflow",
        args=[{
            "s3_path": request.s3_path,
            "max_revision": request.max_revision
        }],
        id= workflow_id,
        task_queue=TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE,
    )

    return {"workflow_id": workflow_id}


@app.get("/contract-review/{workflow_id}/status")
async def get_review_status(workflow_id: str):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id=workflow_id)
    desc = await handle.describe()

    workflow_state = None
    if desc.status == WES.WORKFLOW_EXECUTION_STATUS_RUNNING:

        try:
            workflow_state = await handle.query("get_status", result_type=dict)
        except:
            pass

    return {
        "workflow_id": workflow_id,
        "execution_status": desc.status.name,
        "workflow_state": workflow_state,
    }


@app.get("/contract-review/{workflow_id}/report")
async def get_review_status(workflow_id: str):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id=workflow_id)
    desc = await handle.describe()

    workflow_state = None
    if desc.status == WES.RUNNING:

        try:
            workflow_report = await handle.query("get_report", result_type=dict)
        except Exception as e:
            workflow_state = {"error": str(e)}

    return {
        "workflow_id": workflow_id,
        "execution_status": desc.status.name,
        "workflow_report": workflow_report,
    }


@app.post("/contract-reviewer/{workflow_id}/assign")
async def assign_reviewer(workflow_id: str, request: AssignRequest):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id=workflow_id)


    await handle.signal(
        "assign_reviewer", request.name
    )

    return {
        "status" : "ok",
        "message": f"Reviewer '{request.name}' assigned"
    }

@app.post("/contract-review/{workflow_id}/revise")
async def submit_revise(workflow_id: str, request: ReviseRequest):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)

    result = await handle.execute_update(
        "submit_decision", args=[
            "revise", request.feedback
        ]
    )

    return {"ok": True, "message": result}


@app.get("/contract-review/{workflow_id}/approve")
async def submit_approve(workflow_id: str):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)

    result = await handle.execute_update(
        "submit_decision", args=[
            "approve", ""
        ]
    )

    return {"ok": True, "message": result}