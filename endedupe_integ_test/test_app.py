## Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
## SPDX-License-Identifier: MIT-0

## The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology 
## (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, 
## or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, 
## or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, 
## as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur 
## AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.”



"""Integration test for the endedupe function. """

from typing import Tuple
import uuid
import logging
import boto3
from PIL import Image, ImageOps
from io import BytesIO

import pytest


CFN_STACK_NAME = 'enblog'


logging.basicConfig(level=logging.DEBUG)


## Fixtures

@pytest.fixture
def cfn_client():
    return boto3.client('cloudformation')


@pytest.fixture
def test_input_bucket(cfn_client):
    stack_descriptions = cfn_client.describe_stacks(StackName=CFN_STACK_NAME)
    stack_description = stack_descriptions['Stacks'][0]
    for output in stack_description['Outputs']:
        if output['OutputKey'] == 'TestInputBucketName':
            return output['OutputValue']
    raise RuntimeError('Test input bucket name could not be found')


@pytest.fixture
def output_bucket(cfn_client):
    stack_descriptions = cfn_client.describe_stacks(StackName=CFN_STACK_NAME)
    stack_description = stack_descriptions['Stacks'][0]
    for output in stack_description['Outputs']:
        if output['OutputKey'] == 'OutputBucketName':
            return output['OutputValue']
    raise RuntimeError('Output bucket name could not be found')


## Helpers:

def __make_test_event(for_bucket: str, for_key: str, for_sequencer: str) -> str:
    """Create an EventBridge-like "Object Created" event for an object with the given properties."""
    test_event_for_substitution = """{
        "version": "0",
        "id": "8dbe0493-cd8b-3300-02bb-17f9f95ea57f",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "123412341234",
        "time": "2023-03-08T20:53:12Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:s3:::TEST_INPUT_BUCKET"
        ],
        "detail": {
            "version": "0",
            "bucket": {
                "name": "TEST_INPUT_BUCKET"
            },
            "object": {
                "key": "TEST_INPUT_KEY",
                "size": 7969,
                "etag": "",
                "sequencer": "SEQUENCER"
            },
            "request-id": "ABCDABCDABCD",
            "requester": "123412341234",
            "source-ip-address": "1.2.3.4",
            "reason": "PutObject"
        }
    }
    """

    return test_event_for_substitution.replace('TEST_INPUT_BUCKET', for_bucket) \
                                      .replace('TEST_INPUT_KEY', for_key) \
                                      .replace('SEQUENCER', for_sequencer)


def __upload_image(s3_client, colour: Tuple[int, int], dest_bucket: str, dest_key: str) -> Image.Image:
    img = Image.new('RGB', (100, 100), colour)
    img_IO = BytesIO()
    img.save(img_IO, format='jpeg')
    img_IO.seek(0)
    s3_client.upload_fileobj(img_IO, dest_bucket, dest_key)
    return img

def __upload_and_replace_object_and_generate_events(s3_client, lambda_client, bucket: str, key: str,
        event1: dict, event2: dict) -> Tuple[Image.Image, Image.Image]:
    """Helper to perform test upload/lambda invocation sequence.

    Uploads an image, invokes notification function with `event1`.
    Uploads second image, invokes notification function with `event2`.
    Returns a tuple of the two images that were uploaded.
    """
    # Upload first image
    img_1 = __upload_image(s3_client, (0, 0, 0), bucket, key)

    # Generate notification
    lambda_client.invoke(
        FunctionName='notification_function',
        Payload=event1
    )

    # Upload second image:
    img_2 = __upload_image(s3_client, (80, 80, 80), bucket, key)

    # Generate notification
    lambda_client.invoke(
        FunctionName='notification_function',
        Payload=event2
    )

    return img_1, img_2


def test_ignores_older_sequencer(test_input_bucket: str, output_bucket: str) -> None:
    # Given: Two events that occur out of order
    test_input_key = f'test_image_{str(uuid.uuid4())}.jpg'
    test_event_1 = __make_test_event(test_input_bucket, test_input_key, "006408F5B89B102BAC")
    test_event_2 = __make_test_event(test_input_bucket, test_input_key, "006408F5B89B101BAC")
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    # When: the notifications are handled
    img_1, _ = __upload_and_replace_object_and_generate_events(
        s3_client, lambda_client, test_input_bucket, test_input_key, test_event_1, test_event_2
    )

    # Then: The later event with an older sequencer is discarded
    transformed_image_data = s3_client.get_object(
        Bucket=output_bucket,
        Key=f'out-{test_input_key}'
    )['Body'].read()
    expected_transform_img = ImageOps.invert(img_1)
    expected_transform_img_IO = BytesIO()
    expected_transform_img.save(expected_transform_img_IO, format='jpeg')

    assert transformed_image_data == expected_transform_img_IO.getvalue()


def test_ignores_duplicate_sequencer(test_input_bucket: str, output_bucket: str) -> None:
    # Given: Duplciate events
    test_input_key = f'test_image_{str(uuid.uuid4())}.jpg'
    test_event = __make_test_event(test_input_bucket, test_input_key, "006408F5B89B102BAC")
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    # When: the notifications are handled
    img_1, _ = __upload_and_replace_object_and_generate_events(
        s3_client, lambda_client, test_input_bucket, test_input_key, test_event, test_event
    )

    # Then: The later event with an older sequencer is discarded
    # We verify this by checking that the output is the transformed of the original object, since
    # the second event should have been ignored and therefore the newer input object not used.
    transformed_image_data = s3_client.get_object(
        Bucket=output_bucket,
        Key=f'out-{test_input_key}'
    )['Body'].read()
    expected_transform_img = ImageOps.invert(img_1)
    expected_transform_img_IO = BytesIO()
    expected_transform_img.save(expected_transform_img_IO, format='jpeg')

    assert transformed_image_data == expected_transform_img_IO.getvalue()


def test_doesnt_ignore_duplicate_if_first_processing_attempt_fails(test_input_bucket: str, output_bucket: str) -> None:
    # Given: Duplciate events
    test_input_key = f'test_image_{str(uuid.uuid4())}.jpg'
    test_event = __make_test_event(test_input_bucket, test_input_key, "006408F5B89B102BAC")
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    # When: the notifications are handled
    # - upload unparseable object to make processing fail:
    s3_client.upload_fileobj(BytesIO(b'not an image'), test_input_bucket, test_input_key)
    # Generate notification
    lambda_client.invoke(
        FunctionName='notification_function',
        Payload=test_event
    )

    # Upload second image:
    img = __upload_image(s3_client, (80, 80, 80), test_input_bucket, test_input_key)
    # Generate notification
    lambda_client.invoke(
        FunctionName='notification_function',
        Payload=test_event
    )

    # Then: The later event with an older sequencer is discarded
    # We verify this by checking that the output is the transformed of the original object, since
    # the second event should have been ignored and therefore the newer input object not used.
    transformed_image_data = s3_client.get_object(
        Bucket=output_bucket,
        Key=f'out-{test_input_key}'
    )['Body'].read()
    expected_transform_img = ImageOps.invert(img)
    expected_transform_img_IO = BytesIO()
    expected_transform_img.save(expected_transform_img_IO, format='jpeg')

    assert transformed_image_data == expected_transform_img_IO.getvalue()


def test_uses_newer_sequencer(test_input_bucket: str, output_bucket: str) -> None:
    # Given: Two events that occur in order
    test_input_key = f'test_image_{str(uuid.uuid4())}.jpg'
    test_event_1 = __make_test_event(test_input_bucket, test_input_key, "006408F5B89B101BAC")
    test_event_2 = __make_test_event(test_input_bucket, test_input_key, "006408F5B89B102BAC")
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    # When: the notifications are handled
    _, img_2 = __upload_and_replace_object_and_generate_events(
        s3_client, lambda_client, test_input_bucket, test_input_key, test_event_1, test_event_2
    )

    # Then: The later event is used and the second image is processed
    transformed_image_data = s3_client.get_object(
        Bucket=output_bucket,
        Key=f'out-{test_input_key}'
    )['Body'].read()
    expected_transform_img = ImageOps.invert(img_2)
    expected_transform_img_IO = BytesIO()
    expected_transform_img.save(expected_transform_img_IO, format='jpeg')

    assert transformed_image_data == expected_transform_img_IO.getvalue()
