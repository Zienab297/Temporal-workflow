from datetime import timedelta
from dataclasses import dataclass

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import(
        download_pdf,
        extract_to_markdown,
        upload_markdown
    )

    from helpers import(
        DownloadInput,
        ExtractInput, 
        UploadInput
    )


@dataclass
class PDFPipelineInput:
    s3_path: str

@dataclass
class PDFPipelineOutput:
    output_s3_path: str


DEFAULT_RETRY= RetryPolicy(
    initial_interval=timedelta(seconds=2), 
    backoff_coefficient=2.0, #double wait
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5
)


@workflow.defn
class PDFPipelineWorkflow:
    @workflow.run
    async def run(self, params:PDFPipelineInput) -> PDFPipelineOutput:

        workflow.logger.info(f"Strating PDF pipeline for: {params.s3_path}")

        download_result = await workflow.execute_activity(
            download_pdf,
            DownloadInput(s3_path=params.s3_path),
            retry_policy=DEFAULT_RETRY,
            start_to_close_timeout=timedelta(minutes=3)
        )


        extract_result = await workflow.execute_activity(
            extract_to_markdown,
            ExtractInput(local_path=download_result.local_path),
            retry_policy=DEFAULT_RETRY,
            start_to_close_timeout=timedelta(minutes=10)
        )

        upload_result = await workflow.execute_activity(
            upload_markdown,
            UploadInput(markdown_text=extract_result.markdown_text, original_s3_path=params.s3_path),
            retry_policy=DEFAULT_RETRY,
            start_to_close_timeout=timedelta(minutes=3)
        )

        workflow.logger.info(f"Completed Workflow. Output: {upload_result.output_path}")

        return PDFPipelineOutput(upload_result.output_path)