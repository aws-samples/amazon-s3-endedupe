## Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
## SPDX-License-Identifier: MIT-0

## The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology
## (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement,
## or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts,
## or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code,
## as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur
## AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.”



#!/bin/bash

set -e

usage() {
    cat <<EOF >&2
Usage: $0 [-n] [-c <count>] [-s <stack>] [-h] output-directory

Configures the test lambda function to demonstrate filtering of duplicate/
unordered events either with or without locking, runs the upload tool to
upload a series of test inputs to the input bucket, fetches the results,
and creates a montage using the ImageMagick 'montage' tool.

-n: turn off locking in the lambda function before running.  Otherwise it
    will be enabled.  (Set COORDINATION=off.)
-s <stack>: Set the CloudFormation stack name, if it's not eventbridge-blog.
-c <count>: How many images to upload (default 100).
-h: Help: display this message and exit.
<output-directory>: Name of the directory to fetch the results to.
EOF
}

COUNT=100
COORDINATION="COORDINATION=on"
STACK=eventbridge-blog
while getopts "hs:nc:" opt; do
    case $opt in
        n)
            COORDINATION="COORDINATION=off"
            ;;
        c)
            COUNT=$OPTARG
            ;;
        s)
            STACK=$OPTARG
            ;;
        h)
            usage
            exit
            ;;
    esac
done
shift $((OPTIND - 1))
OUTPUT_DIR=${1:-output}
if [ -z "${OUTPUT_DIR}" ] ; then
    usage
    exit 1
fi

# Get bucket names from CloudFormation stack
function get_param() {
    key="$1"
    aws cloudformation describe-stacks --stack-name eventbridge-blog | \
        jq -r '.Stacks[0].Outputs[] | select(.OutputKey == "'"${key}"'").OutputValue'
}
INPUT_BUCKET=$(get_param InputBucketName)
OUTPUT_BUCKET=$(get_param OutputBucketName)
DDB_TABLE=$(get_param LockTableName)

echo "$INPUT_BUCKET -> $OUTPUT_BUCKET $COORDINATION"

# Update function configuration to either do co-ordination/locking, or not:
aws --no-paginate lambda update-function-configuration \
    --function-name notification_function \
    --environment "Variables={DDB_TABLE=${DDB_TABLE},OUTPUT_BUCKET=${OUTPUT_BUCKET},${COORDINATION}}"

# Clean up any previous test artifacts:
[ ! -e $OUTPUT_DIR ] || rm -r $OUTPUT_DIR
aws s3 rm s3://$OUTPUT_BUCKET --recursive
aws s3 rm s3://$INPUT_BUCKET --recursive

# Run the test
python3 upload.py $COUNT $INPUT_BUCKET

# Get results
echo "---"
echo "Pausing to allow notifications to complete execution."
sleep 60
echo "--- ...done"
mkdir -p $OUTPUT_DIR
aws s3 sync s3://$OUTPUT_BUCKET/ $OUTPUT_DIR/

montage $OUTPUT_DIR/*.jpg result.png
