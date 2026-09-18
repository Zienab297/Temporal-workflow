import asyncio
import textwrap
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional
import json_repair

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy

from prompts import _SYNTHESIS_PROMPT

with workflow.unsafe.imports_passed_through():
    from activities import(
        call_llm, CallLLMInput,
    )
    from child_workflow import(
        PDFSummaryWorkflow, PDFSummaryInput
    )


@dataclass
class ContractReviewInput:
    s3_path: list[str]
    max_revisions: int = 2

@dataclass
class ContractReviewOutput:
    report: str
    sources: list
    approved_by: str


DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval= timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=4,
)


@workflow.defn
class ContractReviewWorkflow:

    def __init__(self):
        self._status: str = "processing"
        self._summaries: list = []
        self._report: str = ""

    @workflow.run
    async def run(self, params: ContractReviewInput) -> ContractReviewOutput:
        self._status = "extracting"

        workflow.logger.info(f"Fanning out to {len(params.s3_path)} child workflows")

        workflow_id = workflow.info().workflow_id
        workflow_task_queue = workflow.info().task_queue

        handles = await asyncio.gather(
            *[
                workflow.start_child_workflow(
                    PDFSummaryWorkflow.run,
                    PDFSummaryInput(
                        s3_path=current_s3_path
                    ),
                    id=f"{workflow_id}-pdf-{idx+1}",
                    task_queue= workflow_task_queue,
                    parent_close_policy=ParentClosePolicy.ABANDON
                )

                for idx, current_s3_path in enumerate(params.s3_path)
            ]
        )

        raw_results = await asyncio.gather(
            *handles,
            return_exceptions= True,
        )

        for i, res in enumerate(raw_results):

            if isinstance(res, Exception):
                workflow.logger.warning(f"PDF {i} failed: {res}")
            else:
                self._summaries.append({
                    "s3_path": res.s3_path,
                    "summary": res.summary,
                    "key_risks": res.key_risks
                })

        if len(self._summaries) == 0:
            raise ApplicationError("All PDFs failed to process.")

        #calling llm to summarize into risk report
        self._status = 'summarizing'
        workflow.logger.info(f"Synthesizing {len(self._summaries)} summaries")

        combined_summary = "\n\n".join([

            f"**Contract {i+1}** (`{summary['s3_path']}`):\n"
            f"Summary: {summary['summary']}\n"
            f"Risks: {summary['key_risks']}"

            for i, summary in enumerate(self._summaries)
        ])

        llm_prompt = _SYNTHESIS_PROMPT.format(
            summaries= combined_summary,
            n=len(self._summaries)
            )

        llm_result = await workflow.execute_activity(
            call_llm,
            CallLLMInput(
                prompt=llm_prompt
            ),
            start_to_close_timeout= timedelta(minutes=3),
            heartbeat_timeout=timedelta(seconds=180),
            retry_policy= DEFAULT_RETRY_POLICY
        )

        self._report = json_repair.loads(llm_result.content)

        return ContractReviewOutput(
            report=self._report,
            sources=params.s3_path,
            approved_by=""
        )

