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

#print(str(get_tags_response))
#{'ResponseMetadata': {'RequestId': 'ER2JF8H8MTBZ4QFD', 'HostId': 'QUg4TPXqM7OaizEBKA9ZOWtvTUwt5uXmQI2EwJu5d4tC4x76x8yzFPBVgp65L1GZMZB5eukbq34=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'QUg4TPXqM7OaizEBKA9ZOWtvTUwt5uXmQI2EwJu5d4tC4x76x8yzFPBVgp65L1GZMZB5eukbq34=', 'x-amz-request-id': 'ER2JF8H8MTBZ4QFD', 'date': 'Sat, 24 Jul 2021 00:12:34 GMT', 'x-amz-version-id': 'null', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 1}, 'VersionId': 'null', 'TagSet': [{'Key': 'labeler', 'Value': 'karl.rink'}]}

#get existing tags,
s3Tags = {}
for key in get_tags_response['TagSet']:
    __k = key['Key']
    __v = key['Value']
    s3Tags[__k]=__v

#print(str(s3Tags))

#overwrite existing key/value w/ new 
s3Tags[tagkey]=tagval

#now write out dict
KeyValList = []
for k,v in s3Tags.items():
    kvs={}
    kvs['Key']   = k
    kvs['Value'] = v
    KeyValList.append(kvs)

TagSet = { 'TagSet': KeyValList }

put_tags_response = s3_client.put_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
        Tagging=TagSet
)

print(str(put_tags_response))

#for key in put_tags_response['ResponseMetadata']:
    #print(key, key[HTTPStatusCode])
    #print(key['HTTPStatusCode'])
    #if key == 'HTTPStatusCode':
    #    print(key)
    #    print(key['HTTPStatusCode'])

print(put_tags_response['ResponseMetadata']['HTTPStatusCode'])



#🧨 karl.rink@Karl-MacBook-Pro t % ./bototagupdater.py ninfo-property-images 2eece964b6f902124052810e5a92d6f9ca715c1b.jpg thiskey thisvalue





#s3 = boto3.resource('s3')
#https://boto.readthedocs.io/en/latest/ref/s3.html#boto.s3.bucket.Bucket.set_tags
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object_tagging





