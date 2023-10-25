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
from s3index import ItemLockedException, S3Lock
from s3index import handle_notification_if_up_to_date, OUTCOME_OUT_OF_DATE, OUTCOME_PROCESSED


def __event_with(key='foo', sequencer='0'):
    return {
        'detail': {
            'object': {
                'key': key,
                'sequencer': sequencer,
            },
            'bucket':  {
                'name': 'bar',
            },
        },
    }


@pytest.mark.parametrize("current_sequencer", ['0', '1f'])
def test_notification_is_ignored_when_it_is_old_or_duplicate(current_sequencer):
    # Given: a table entry that already has a sequencer '1f'
    lock = S3Lock('', None, '')
    lock.getCurrentSequencer = mock.MagicMock(return_value = '1f')
    backoff = mock.MagicMock()
    processing_fn = mock.MagicMock()

    # When: an event happens with older or same sequencer
    event = __event_with(sequencer=current_sequencer)
    outcome, rv = handle_notification_if_up_to_date(event, {}, lock, processing_fn, backoff)

    # Then: the processing function isn't called
    processing_fn.assert_not_called()
    backoff.assert_not_called()
    assert outcome == OUTCOME_OUT_OF_DATE
    assert rv == None


def test_notification_is_processed_when_up_to_date_and_no_lock():
    # Given: a table entry that already has a sequencer '0'
    lock = S3Lock('', None, '')
    lock.getCurrentSequencer = mock.MagicMock(return_value='0')
    lock.lockForSequencer = mock.MagicMock(return_value=True)
    lock.unlock = mock.MagicMock()
    backoff = mock.MagicMock()

    # When: an event happens with newer sequencer '1'
    event = __event_with(sequencer='1')
    processing_fn = mock.MagicMock(return_value="hello")
    outcome, rv = handle_notification_if_up_to_date(event, {}, lock, processing_fn, backoff)

    # Then: the processing function is called once
    processing_fn.assert_called_once()
    backoff.assert_not_called()
    assert outcome == OUTCOME_PROCESSED
    assert rv == "hello"


@pytest.mark.parametrize("current_sequencer", [None, '1f'])
def test_notification_is_processed_for_new_object(current_sequencer):
    # Given: no entry in the table
    lock = S3Lock('', None, '')
    lock.getCurrentSequencer = mock.MagicMock(return_value=current_sequencer)
    lock.lockForSequencer = mock.MagicMock(return_value=True)
    lock.unlock = mock.MagicMock()
    backoff = mock.MagicMock()

    # When: an event happens
    event = __event_with(sequencer='20')
    processing_fn = mock.MagicMock(return_value="hello")
    outcome, rv = handle_notification_if_up_to_date(event, {}, lock, processing_fn, backoff)

    # Then: the processing function is called once
    processing_fn.assert_called_once()
    backoff.assert_not_called()
    assert outcome == OUTCOME_PROCESSED
    assert rv == "hello"


@pytest.mark.parametrize('current_sequencer', [None, '0'])
def test_state_rolled_backed_after_processing_failure(current_sequencer):
    # Given: locking for sequencer works from either non-existing or existing value
    #   AND: the processing function throws an error:
    lock = S3Lock('', None, '')
    lock.getCurrentSequencer = mock.MagicMock(return_value=current_sequencer)
    lock.lockForSequencer = mock.MagicMock(return_value=True)
    lock.unlock = mock.MagicMock()
    lock.unlockAndRollBack = mock.MagicMock()
    backoff = mock.MagicMock()
    processing_fn = mock.MagicMock(side_effect=RuntimeError())

    # When: an event happens:
    with pytest.raises(RuntimeError):
        handle_notification_if_up_to_date(__event_with(sequencer='1'), {}, lock, processing_fn, backoff)

    # Then: the processing function is called once AND the lock is rolled back
    processing_fn.assert_called_once()
    backoff.assert_not_called()
    lock.unlock.assert_not_called()
    lock.unlockAndRollBack.assert_called_once_with(current_sequencer)


def test_backs_off_when_locked_updating_sequencer():
    # Given: a table entry that already has a sequencer '0'
    #   AND: the lock can't be acquired
    lock = S3Lock('', None, '')
    lock.getCurrentSequencer = mock.MagicMock(return_value='0')
    lock.lockForSequencer = mock.MagicMock(return_value=False)
    lock.unlock = mock.MagicMock()

    # When: an event happens with newer sequencer '1'
    event = __event_with(sequencer='1')
    processing_fn = mock.MagicMock()
    with pytest.raises(Exception):
        backoff = mock.MagicMock(side_effect=Exception())
        handle_notification_if_up_to_date(event, {}, lock, processing_fn, backoff_fn=backoff)

    # Then: we back off (and, because the backoff fails, the processing isn't called):
    backoff.assert_called()
    processing_fn.assert_not_called()


def test_backs_off_when_locked_getting_sequencer():
    # Given: a table entry that already has a sequencer '0'
    #   AND: the row is locked at the time the sequencer is checked
    lock = S3Lock('', None, '')
    lock.getCurrentSequencer = mock.MagicMock(side_effect=ItemLockedException())
    lock.lockForSequencer = mock.MagicMock(return_value=False)
    lock.unlock = mock.MagicMock()

    # When: an event happens with newer sequencer '1'
    event = __event_with(sequencer='1')
    processing_fn = mock.MagicMock()
    with pytest.raises(Exception):
        backoff = mock.MagicMock(side_effect=Exception())
        handle_notification_if_up_to_date(event, {}, lock, processing_fn, backoff_fn=backoff)

    # Then: we back off (and, because the backoff fails, the processing isn't called):
    backoff.assert_called()
    processing_fn.assert_not_called()
