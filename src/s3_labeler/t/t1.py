#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3

s3 = boto3.resource('s3')

my_bucket = s3.Bucket('ninfo-property-images/rekognition/')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object)




