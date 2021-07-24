#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3
import sys


s3bucket = sys.argv[1]
s3object = sys.argv[2]
tagkey   = sys.argv[3]
tagval   = sys.argv[4]

print(s3bucket)
print(s3object)

s3_client = boto3.client('s3',
    #region_name='region-name',
    #aws_access_key_id='aws-access-key-id',
    #aws_secret_access_key='aws-secret-access-key',
)

get_tags_response = s3_client.get_object_tagging(
    Bucket=s3bucket,
    Key=s3object,
)

put_tags_response = s3_client.put_object_tagging(
    Bucket=s3bucket,
    Key=s3object,
    Tagging={
        'TagSet': [
            {
                'Key': 'labeler',
                'Value': 'karl.rink'
            },
        ]
    }
)






#s3 = boto3.resource('s3')
#https://boto.readthedocs.io/en/latest/ref/s3.html#boto.s3.bucket.Bucket.set_tags
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object_tagging





