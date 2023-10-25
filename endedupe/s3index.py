## Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
## SPDX-License-Identifier: MIT-0

## The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology 
## (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, 
## or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, 
## or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, 
## as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur 
## AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.”


from typing import Any, Optional, Callable
from botocore.exceptions import ClientError
import random
import logging
import time

logger = logging.getLogger(__name__)


# DDB table field names
FIELD_SEQUENCER = 'sequencer'
FIELD_KEY = "s3key"
FIELD_LOCK_STATUS = 'lock_status'
FIELD_UPDATED_BY = 'updated_by'

# Values for the lock field:
LOCK_VALUE_LOCKED = 'locked'
LOCK_VALUE_UNLOCKED = ''

# Possible outcomes from handling a notification
OUTCOME_OUT_OF_DATE = 1
OUTCOME_PROCESSED = 2


def _backoff():
    n = random.random()
    logger.debug(f"Backoff: {n} seconds")
    time.sleep(n)


class ItemLockedException(Exception):
    """Thrown when the item requested is locked by another user."""
    pass


class S3Lock:
    """Managing locking of keys in the secondary index."""

    _key: str
    _execution_id: str

    def __init__(self, key: str, locktable, execution_id: str):
        self._key = key
        self._locktable = locktable
        self._execution_id = execution_id

    def getCurrentSequencer(self) -> Optional[str]:
        """Return current sequencer or None if one isn't present."""
        response = self._locktable.get_item(Key={FIELD_KEY: self._key},
                                            ConsistentRead=True)

        if 'Item' in response:
            # Item present.  Check it isn't locked, then return the sequencer
            locked = response['Item'][FIELD_LOCK_STATUS]
            sequencer = response['Item'][FIELD_SEQUENCER]

            if locked:
                raise ItemLockedException()
            return sequencer
        else:
            # Item not seen before
            return None

    def lockForSequencer(self, oldSequencer: str, newSequencer: str) -> bool:
        """Locks the key and updates sequencer to :newSequencer.

        Return True if successful, or False if item is already locked or
        already has a newer sequencer.
        """
        # TODO name the lock so we can check it's ours later?
        try:
            self._locktable.put_item(
                Item={
                    FIELD_KEY: self._key,
                    FIELD_SEQUENCER: newSequencer,
                    FIELD_LOCK_STATUS: LOCK_VALUE_LOCKED,
                    FIELD_UPDATED_BY: self._execution_id,
                },
                ConditionExpression=(
                    f'attribute_not_exists({FIELD_KEY}) OR '
                    + f'({FIELD_SEQUENCER} = :old_sequencer and '
                    + f'{FIELD_LOCK_STATUS} = :empty_lock)'),
                ExpressionAttributeValues={
                    ':old_sequencer': oldSequencer,
                    ':empty_lock': LOCK_VALUE_UNLOCKED,
                    }
                )
        except ClientError as e:
            if (e.response['Error']['Code']
                    == 'ConditionalCheckFailedException'):
                return False
            raise

        return True

    def unlockAndRollBack(self, to_sequencer: str) -> None:
        """Unlocks and sets the sequencer to the `to_sequencer` value.

        This could be used if the processing of the notification for the
        current sequencer failed, otherwise a retry will not be able to
        get the lock.
        """
        self._locktable.update_item(
            Key={FIELD_KEY: self._key},
            UpdateExpression=f"SET {FIELD_LOCK_STATUS} = :empty_lock, {FIELD_UPDATED_BY} = :updated_by, " +
                             f"{FIELD_SEQUENCER} = :sequencer",
            ExpressionAttributeValues={
                ':empty_lock': LOCK_VALUE_UNLOCKED,
                ':sequencer': to_sequencer,
                ':updated_by': self._execution_id,
                }
        )

    def unlock(self) -> None:
        """Unlock the key."""
        # TODO check it's our lock we are clearing?
        self._locktable.update_item(
            Key={FIELD_KEY: self._key},
            UpdateExpression=f"SET {FIELD_LOCK_STATUS} = :empty_lock, {FIELD_UPDATED_BY} = :updated_by",
            ExpressionAttributeValues={
                ':empty_lock': LOCK_VALUE_UNLOCKED,
                ':updated_by': self._execution_id,
                }
        )


def handle_notification_if_up_to_date(event: dict, context: dict, lock: S3Lock,
                                      processing_fn: Callable[[dict, dict], Any],
                                      backoff_fn=_backoff) -> tuple:
    """Calls processing_fn after checking that notification is safe to process.

    If another newer or identical notification has already been processed,
    then this function returns without calling the processing function.
    This function will also ensure that an exclusive lock for this object is
    taken during processing, so that if another notification occurs in
    parallel it will not do any work until this one completes.

    Returns a tuple of the outcome, and the return value from the processing
    function if applicable.  The outcome is one of the OUTCOME_* constants
    defined in this module.
    """
    sequencer = event['detail']['object']['sequencer']
    input_bucket = event['detail']['bucket']['name']
    input_key = event['detail']['object']['key']
    input_full_key = f"{input_bucket}/{input_key}"

    rv = None, None
    while True:
        try:
            oldsequencer = lock.getCurrentSequencer()
        except ItemLockedException:
            backoff_fn()
        else:
            # If the last recorded sequencer is older than the one for this
            # notification, we should attempt to process the notification
            # and update the sequencer:
            if oldsequencer is None or oldsequencer < sequencer:
                logger.info(f"Attempting to get lock for {oldsequencer} -> {sequencer}")
                if not lock.lockForSequencer(oldsequencer, sequencer):
                    backoff_fn()
                    continue

                try:
                    logger.info(f"Locked {input_full_key} for {sequencer}")
                    rv = OUTCOME_PROCESSED, processing_fn(event, context)
                except Exception as e:
                    logger.exception("Processing method throw exception", exc_info=e)
                    lock.unlockAndRollBack(oldsequencer)
                    logger.info(f"Unlocked {input_full_key} and rolled back to {oldsequencer}")
                    raise e
                else:
                    lock.unlock()
                    logger.info(f"Unlocked {input_full_key}")
                    break
            else:
                # the same or newer sequencer was already seen, we can skip
                # this notification.
                logger.info(f"notification with sequencer {sequencer} "
                            + f"older than {oldsequencer}: skipping")
                rv = OUTCOME_OUT_OF_DATE, None
                break

    return rv
