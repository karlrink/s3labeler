#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3

s3_client = boto3.client('s3')

s3bucket = "ninfo-property-images"
s3key    = "2eece964b6f902124052810e5a92d6f9ca715c1b.jpg"

s3_result = s3_client.get_object_tagging(Bucket=s3bucket, Key=s3key)

#print(str(s3_result))

for key in s3_result['TagSet']:
    print(key['Key'])


#print(str(s3_result))
#🧨 karl.rink@Karl-MacBook-Pro t % ./get_object_tagging.py 
#{'ResponseMetadata': {'RequestId': 'BNKZRR1D1SZQEZNG', 'HostId': 'fiF/2hP+GkIeI3nNy/yakSzstQqZflVa4L0ccLSvuNOfkusXdx4XRvafshQHhktO0HXQRfm8VH4=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'fiF/2hP+GkIeI3nNy/yakSzstQqZflVa4L0ccLSvuNOfkusXdx4XRvafshQHhktO0HXQRfm8VH4=', 'x-amz-request-id': 'BNKZRR1D1SZQEZNG', 'date': 'Thu, 22 Jul 2021 19:22:07 GMT', 'x-amz-version-id': 'null', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 1}, 'VersionId': 'null', 'TagSet': [{'Key': 'rekognition_json_location', 'Value': 'https://ninfo-property-images.s3.us-west-2.amazonaws.com/rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json'}]}


# aws s3api get-object-tagging --bucket ninfo-property-images --key 2eece964b6f902124052810e5a92d6f9ca715c1b.jpg

#response = client.get_object_tagging(
#    Bucket='string',
#    Key='string',
#    VersionId='string',
#    ExpectedBucketOwner='string',
#    RequestPayer='requester'
#)

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object_tagging


