## Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
## SPDX-License-Identifier: MIT-0

## The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology 
## (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, 
## or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, 
## or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, 
## as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur 
## AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.”

import hashlib
import time
import random
import json
import logging
import os
import io

import boto3
from PIL import Image, ImageOps
from s3index import S3Lock, handle_notification_if_up_to_date

logger = logging.getLogger()
logger.setLevel(logging.INFO)


_OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
_DDB_TABLE_NAME = os.environ['DDB_TABLE']
_COORDINATION = os.environ['COORDINATION']
_SLOW_PROBABILITY = float(os.environ.get('SLOW_PROBABILITY', '0.5'))


# sample processing , below shows resize of an image
def do_work(event, context):
    logger.info("Starting processing")

    s3 = boto3.client('s3')

    input_bucket = event['detail']['bucket']['name']
    input_key = event['detail']['object']['key']
    input_version_id = event['detail']['object'].get('version-id')
    input_file = input_key.split('/')[-1]

    # Fetch image from S3
    version_dict = {'VersionId': input_version_id} if input_version_id else {}
    logger.info(f"Reading {input_bucket}/{input_key} {version_dict}")
    response = s3.get_object(Bucket=input_bucket, Key=input_key, **version_dict)
    body = response['Body'].read()

    # Inject random sleep to simulate a delay in processing:
    digest = hashlib.sha256(body).hexdigest()
    logger.info(f"Digest f{digest}")
    if _SLOW_PROBABILITY > 0 and random.random() < _SLOW_PROBABILITY:
        logger.info(f"Hit a slow run. {digest}")
        time.sleep(10)

    # Process the image:
    img = Image.open(io.BytesIO(body))
    img_inv = ImageOps.invert(img)
    output_bytesio = io.BytesIO()
    img_inv.save(output_bytesio, "JPEG")
    output_bytesio.seek(0)

    # Write the transformed image back to the output bucket:
    version_suffix = ("#" + input_version_id) if input_version_id else ""
    output_key = f'out-{input_file}{version_suffix}'
    logger.info(f'Writing transformed image to {_OUTPUT_BUCKET}/{output_key}')
    s3.upload_fileobj(output_bytesio, _OUTPUT_BUCKET, output_key)


def lambda_handler(event, context):
    random.seed()
    logger.info(f"Lambda Request ID: {context.aws_request_id}")
    logger.info(json.dumps(event, indent=4))

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(_DDB_TABLE_NAME)

    input_bucket = event['detail']['bucket']['name']
    input_key = event['detail']['object']['key']
    input_version_id = event['detail']['object'].get('version-id')
    version_suffix = '#' + (input_version_id or '')
    input_full_key = f"{input_bucket}/{input_key}{version_suffix}"

    if _COORDINATION == 'off':
        outcome = "COORDINATION variable value is off, Locking & Sequencer Check routine will NOT execute"
        result = do_work(event, context)
    else:
        outcome, result = handle_notification_if_up_to_date(
            event, context, S3Lock(input_full_key, table, context.aws_request_id), do_work)

    logger.info(f"Outcome: {outcome}")
    return result
