## Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
## SPDX-License-Identifier: MIT-0

## The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology 
## (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, 
## or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, 
## or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, 
## as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur 
## AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.”


import pytest

from unittest import mock
from botocore.exceptions import ClientError

import s3index

# Naming convention: test_<given>_<when>_<then>
# getCurrentSequencer tests


def test_givenKeyNotSeen_getCurrentSequencer_returnsNone():
    class MockTableReturnsNothingOnGet:
        def get_item(*args, **kwargs):
            return {}

    idx = s3index.S3Lock("foo", MockTableReturnsNothingOnGet(), '')

    assert idx.getCurrentSequencer() is None


def test_givenKeySeen_getCurrentSequencer_returnsSequencer():
    class MockTableReturnsValueOnGet:
        def get_item(*args, **kwargs):
            return {'Item': {
                "lock_status": "",
                "s3key": "foo",
                "sequencer": "10"
            }}

    idx = s3index.S3Lock("foo", MockTableReturnsValueOnGet(), '')

    assert idx.getCurrentSequencer() == '10'


def test_givenRowLocked_getCurrentSequencer_raises():
    class MockTableReturnsValueOnGet:
        def get_item(*args, **kwargs):
            return {'Item': {
                "lock_status": "locked",
                "s3key": "foo",
                "sequencer": "10"
            }}

    with pytest.raises(s3index.ItemLockedException):
        idx = s3index.S3Lock("foo", MockTableReturnsValueOnGet(), '')
        assert idx.getCurrentSequencer() == '10'


# lockForSequencer tests

def test_givenConditionalFails_lockForSequencer_returnsFalse():
    class MockTableThrowsConditionErrorOnPut:
        def put_item(*args, **kwargs):
            assert 'Item' in kwargs
            assert kwargs['Item'][s3index.FIELD_KEY] == 'foo'
            assert kwargs['Item'][s3index.FIELD_SEQUENCER] == '10'
            assert kwargs['Item'][s3index.FIELD_LOCK_STATUS] == "locked"
            raise ClientError(
                {'Error': {'Code': 'ConditionalCheckFailedException'}}, "")

    idx = s3index.S3Lock("foo", MockTableThrowsConditionErrorOnPut(), '')
    assert not idx.lockForSequencer("", "10")


def test_givenPutItemSucceeds_lockForSequencer_returnsTrue():
    class MockTableThrowsConditionErrorOnPut:
        def put_item(*args, **kwargs):
            assert 'Item' in kwargs
            assert kwargs['Item'][s3index.FIELD_KEY] == 'foo'
            assert kwargs['Item'][s3index.FIELD_SEQUENCER] == '10'
            assert kwargs['Item'][s3index.FIELD_LOCK_STATUS] == "locked"
            return {}

    idx = s3index.S3Lock("foo", MockTableThrowsConditionErrorOnPut(), '')
    assert idx.lockForSequencer("", "10")


def test_givenPutItemThrowsUnrelatedError_lockForSequencer_reraises():
    class MockTableThrowsConditionErrorOnPut:
        def put_item(*args, **kwargs):
            raise RuntimeError()

    idx = s3index.S3Lock("foo", MockTableThrowsConditionErrorOnPut(), '')

    with pytest.raises(RuntimeError):
        idx.lockForSequencer("", "10")


# unlock tests

def test_callToUnlock_callsDDB():
    mockTable = mock.MagicMock()
    idx = s3index.S3Lock("foo", mockTable, '')
    idx.unlock()
    mockTable.update_item.assert_called()


# unlockAndRollback tests

def test_rollbackAndUnlock_callsDDB():
    mockTable = mock.MagicMock()
    idx = s3index.S3Lock("foo", mockTable, '')
    idx.unlock()
    mockTable.update_item.assert_called()
