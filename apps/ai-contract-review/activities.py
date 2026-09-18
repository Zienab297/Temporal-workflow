import os
from dataclasses import dataclass
import math
import tempfile


import boto3
import fitz #page by page extraction
import pymupdf4llm
from dotenv import load_dotenv
from openai import OpenAI
from temporalio import activity
from pathlib import Path

load_dotenv()

@dataclass
class ExtractPDFInput:
    s3_path: str
    batch_size: int = 2

@dataclass
class ExtractPDFOutput:
    s3_path: str
    markdown_text: str
    page_count: int

@dataclass
class CallLLMInput:
    prompt: str

@dataclass
class CallLLMOutput:
    content: str

#Helpers
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_REGION"],
        endpoint_url=os.environ["AWS_S3_ENDPOINT_URL"],
    )

def parse_s3_path(s3_path: str):
    s3_path_no_scheme = s3_path.replace("s3://", "")
    bucket, _, key =  s3_path_no_scheme.partition("/")
    return bucket, key


# activity 1 is to extract pdf from s3
@activity.defn
async def extract_pdf(params: ExtractPDFInput) -> ExtractPDFOutput:
    activity.logger.info(f"Start Extraction: {params.s3_path}")

    activity.heartbeat({
        "stage": "downloading",
        "s3-path": params.s3_path,
        "pages_done": 0,
        "chars_extractor": 0,
    })
    s3_client = get_s3_client()
    bucket, key = parse_s3_path(s3_path=params.s3_path)

    filename= Path(key).name
    TEMP_DIR = os.environ["TEMP_DIR"]

    local_path = str(Path(TEMP_DIR) / filename)

    s3_client.download_file(
        bucket,
        key, 
        local_path
    )

    doc = fitz.open(local_path)
    total_pages = doc.page_count

    activity.logger.info(f"Download {total_pages}-page PDF: {params.s3_path}")

    all_text_chunks = []
    total_chars_num = 0
    num_batches = math.ceil(total_pages/params.batch_size)

    for batch_idx in range(num_batches):
        start_page = batch_idx * params.batch_size
        end_page = min(batch_idx + params.batch_size, total_pages)

        pages_to_extract = list(range(start_page, end_page))

        if not pages_to_extract:
            continue

        batch_md = pymupdf4llm.to_markdown(
            local_path,
            pages=pages_to_extract,
        )

        all_text_chunks.append(batch_md)
        total_chars_num += len(batch_md)

        activity.heartbeat({
            "stage": "extracting",
            "s3_path": params.s3_path,
            "pages_done": end_page,
            "total_pages": total_pages,
            "chars_extracted": total_pages,
            "progress_pct": round(end_page / total_pages * 100)
        })

    full_md = "\n\n".join(all_text_chunks)

    return ExtractPDFOutput(
        s3_path=params.s3_path,
        markdown_text=full_md,
        page_count=total_pages,
    )


#activity 2 is to send a call to the LLM via OpenRouter
@activity.defn
async def call_llm(params: CallLLMInput) -> CallLLMOutput:
    activity.logger.info("Calling LLM")
    activity.heartbeat({
                "stage": "calling_llm",
                "prompt_chars": len(params.prompt),
            })

    llm_client = OpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )

    response = llm_client.chat.completions.create(
        model=os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
        messages=[{"role": "user", "content": params.prompt}],
        max_tokens=8000,
    )


    content = response.choices[0].message.content

    activity.logger.info(f"LLM returned {len(content)} chars")

    return CallLLMOutput(
        content=content
    )