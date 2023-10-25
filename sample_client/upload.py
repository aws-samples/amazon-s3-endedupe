## Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
## SPDX-License-Identifier: MIT-0

## The sample code; software libraries; command line tools; proofs of concept; templates; or other related technology 
## (including any of the foregoing that are provided by our personnel) is provided to you as AWS Content under the AWS Customer Agreement, 
## or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content in your production accounts, 
## or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS Content, such as sample code, 
## as appropriate for production grade use based on your specific quality control practices and standards. Deploying AWS Content may incur 
## AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or using Amazon S3 storage.”


import time
import boto3
from concurrent.futures import ThreadPoolExecutor
import sys


S3 = boto3.client('s3')


def main():
    num_objects = int(sys.argv[1])
    dest_bucket = sys.argv[2]

    # Upload the images:
    with open('dog.jpg', 'rb') as f:
        dog_img = f.read()
    with open('cat.jpg', 'rb') as f:
        cat_img = f.read()

    executor = ThreadPoolExecutor(10)
    for i in range(num_objects):
        executor.submit(process_image, i, dest_bucket, dog_img, cat_img)
    executor.shutdown(True)

def process_image(n: int, dest_bucket: str, dog_img, cat_img):
    global S3

    output_name = f'img{n}.jpg'
    for img in ['cat', 'dog']:
        if img == 'dog':
            upload_img = dog_img
        else:
            upload_img = cat_img
        print(f"Uploading {img} to {output_name}")
        S3.put_object(Body=upload_img, Bucket=dest_bucket, Key=output_name)
        if img == 'cat':
            time.sleep(5)


if __name__ == "__main__":
    main()
