#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3

#s3 = boto3.resource('s3')

#conn = boto3.client('s3')

#my_bucket = s3.Bucket('ninfo-property-images/rekognition/')
#for my_bucket_object in my_bucket.objects.all():
#    print(my_bucket_object)

#for key in conn.list_objects(Bucket='ninfo-property-images')['rekognition']:
#    print(key['Key'])

#    from boto3  import client
#    bucket_name = "my_bucket"
#    prefix      = "my_key/sub_key/lots_o_files"
#
#    s3_conn   = client('s3')  # type: BaseClient  ## again assumes boto.cfg setup, assume AWS S3
#    s3_result =  s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter = "/")
#
#    if 'Contents' not in s3_result:
#        #print(s3_result)
#        return []
#

bucket      = "ninfo-property-images"
prefix      = "rekognition/"

s3_conn   = boto3.client('s3')
s3_result =  s3_conn.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter = "/")

for key in s3_result['Contents']:
    print(key['Key'])













