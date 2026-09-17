import os
import logging
from pathlib import Path

import pymupdf4llm
from dataclasses import dataclass
from temporalio import activity

from helpers import (DownloadInput, DownloadOutput, 
                     ExtractInput, ExtractOutput, 
                     UploadInput, UploadOutput, 
                     get_s3_client, parse_s3_path, TEMP_DIR)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)



@activity.defn
async def download_pdf(params: DownloadInput) -> DownloadOutput:
    bucket, key = parse_s3_path(params.s3_path)
    filename = Path(key).name

    local_path = str(Path(TEMP_DIR) / filename)
    activity.logger.info(f"Downloading: s3://{bucket}/{key} => {local_path}")

    s3_client = get_s3_client()
    s3_client.download_file(bucket, key, local_path)

    activity.logger.info(f"Completed Downloading: {local_path}")

    return DownloadOutput(local_path=local_path)


@activity.defn
async def extract_to_markdown(params: ExtractInput) -> ExtractOutput:
    activity.logger.info(f"Extracting text from {params.local_path}")
    markdown_text = pymupdf4llm.to_markdown(params.local_path)

    activity.logger.info(f"Completed Extraction - {len(markdown_text)} charcters")
    return ExtractOutput(markdown_text=markdown_text)



@activity.defn
async def upload_markdown(params: UploadInput) -> UploadOutput:
    bucket, key = parse_s3_path(params.original_s3_path)
    md_key = key.replace(".pdf", ".md")

    activity.logger.info(f"Uploading extracted markdown -> s3://{bucket}/{md_key}")
    s3_client = get_s3_client()

    s3_client.put_object(
        Bucket=bucket,
        Key=md_key,
        Body=params.markdown_text.encode("utf-8"),
        ContentType="text/markdown",
    )

    output_path = f"s3://{bucket}/{md_key}"
    activity.logger.info(f"Upload completed: {output_path}")

    return UploadOutput(output_path=output_path)