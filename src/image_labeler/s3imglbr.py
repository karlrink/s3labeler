#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = '0.0.0'

import sys

if sys.version_info < (3, 8, 1):
    raise RuntimeError('Requires Python version 3.8.1 or higher. This version: ' + str(sys.version_info))

from flask import Flask
from flask import request
from flask import jsonify
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

#import request

import boto3

#s3 = boto3.resource('s3')

#my_bucket = s3.Bucket('some/path/')

#GET    /                             # Show status
@app.route("/", methods=['GET'])
def root():
    return jsonify(status=200, message="OK", version=__version__), 200


#GET    /tag/<image>                  #
@app.route("/tag/<image>", methods=['GET'])
def get_tag(image=None):
    assert image == request.view_args['image']

    #check if file exist

    #check if lagels already exist

    url = ''

    return jsonify(status=200, message="TAG", image=image), 200

#GET    /s3/<s3object>
#@app.route("/s3/<s3object>", methods=['GET'])
#def get_tag(s3object=None):
#    assert s3object == request.view_args['s3object']


#@app.route("/s3", methods=['GET'])
#def get_s3():
#    region = None
#    s3 = get_s3resource(region)
#    list_my_buckets(s3)
#    return jsonify(status=200, message="OK", success=True), 200

@app.route("/s3", methods=['GET'])
def get_s3(region=None):

    s3 = boto3.resource('s3', region_name=region)

    bucket_list = [b.name for b in s3.buckets.all()]

    #for bucket in bucket_list:
    #    print(bucket)

    #return jsonify(status=200, message="OK", success=True), 200
    return jsonify(bucket_list), 200




@app.route("/s3/<s3bucket>/<s3prefix>", methods=['GET'])
def get_s3bucketprefix(s3bucket=None,s3prefix=None):
    assert s3bucket == request.view_args['s3bucket']
    assert s3prefix == request.view_args['s3prefix']

    print(s3bucket)
    print(s3prefix)

    s3_client = boto3.client('s3')
    s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3prefix, Delimiter = "/")

#    for key in s3_result['Contents']:
#        print(key['Key'])

    _exist=False

    try:
        for key in s3_result['Contents']:
            #print(key['Key'])
            _k = key['Key']
            _exist=True
    except KeyError as e:
        _k = s3_result['Prefix']
        return jsonify(status=404, message="Not Found", existing=_exist, key=_k), 404

    #message = { 'status': 404, 'errorType': 'Not Found: ' + request.url }
    #return jsonify(message), 404


    #print(str(type(s3_result)))
    #print(str(s3_result))

    #item = s3_result.get('Contents').get('Key', None)

    #item = s3_result['Contents'].get('Key', None)
    #item = s3_result['Contents']['Key']

    #print(str(item))

    return jsonify(status=200, message="OK", existing=_exist, key=_k), 200


     

    





@app.errorhandler(404)
def not_found(error=None):
    message = { 'status': 404, 'errorType': 'Not Found: ' + request.url }
    return jsonify(message), 404


def get_s3resource(region=None):
    """
    Get a Boto 3 Amazon S3 resource with a specific AWS Region or with your
    default AWS Region.
    """
    return boto3.resource('s3', region_name=region) if region else boto3.resource('s3')

def list_my_buckets(s3):
    print('Buckets:\n\t', *[b.name for b in s3.buckets.all()], sep="\n\t")


def main():
    app.run(port=8880, debug=False)    


if __name__ == "__main__":
    main()


#https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-demo_bucket_basics.py.html


