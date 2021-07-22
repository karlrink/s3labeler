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

import boto3
import botocore

#GET    /                             # Show version
@app.route("/", methods=['GET'])
def root():
    return jsonify(status=200, message="OK", version=__version__), 200, {'Content-Type':'application/json;charset=utf-8'}


#GET    /s3                           # Show OK
@app.route("/s3", methods=['GET'])
def get_s3():
    return jsonify(status=200, message="OK", path="/s3"), 200, {'Content-Type':'application/json;charset=utf-8'}


#GET    /s3/                          # List all buckets
@app.route("/s3/", methods=['GET'])
def get_s3buckets(region=None):

    s3 = boto3.resource('s3', region_name=region)

    bucket_list = [b.name for b in s3.buckets.all()]

    #for bucket in bucket_list:
    #    print(bucket)

    #return jsonify(status=200, message="OK", success=True), 200
    return jsonify(bucket_list), 200, {'Content-Type':'application/json;charset=utf-8'}



#GET    /s3/<s3bucket>/<s3object>     # List object
#GET    /s3/<s3bucket>/<s3object>?q=  # rekognition=detect-labels

#GET    /s3/<s3bucket>/<s3object>?q=  # tags=s3|rekognition


@app.route("/s3/<s3bucket>/<s3object>", methods=['GET'])
def get_s3bucketobject(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    rekognition = request.args.get("rekognition", None)
    tags        = request.args.get("tags", None)

    s3_client = boto3.client('s3')

    try:
        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")
    except botocore.exceptions.EndpointConnectionError as e:
        return jsonify(status=599, message="EndpointConnectionError", error=str(e)), 599, {'Content-Type':'application/json;charset=utf-8'}

    try:
        for key in s3_result['Contents']:
            #print(key['Key'])
            _k = key['Key']
    except KeyError as e:
        _k = s3_result['Prefix']
        return jsonify(status=404, message="Not Found", existing=False, key=_k), 404, {'Content-Type':'application/json;charset=utf-8'}

    if _k != s3object:
        return jsonify(status=569, message="Objects Do Not Match", object1=str(_k), ojbect2=str(s3object)), 569, {'Content-Type':'application/json;charset=utf-8'}

    #s3object#

    if tags:

        if tags == 's3':
            print('s3tags')
            #aws s3api get-object-tagging --bucket ninfo-property-images --key 2eece964b6f902124052810e5a92d6f9ca715c1b.jpg

        if tags == 'rekognition':
            print('rekognition json')
            #retrieve corresponding rekognition json file
            # /rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json

            rekognition_json_content = get_rekognition_json(s3bucket, s3object)
            print('DEV.TEST')
            print(str(type(rekognition_json_content))) #<class 'bytes'>
            print(str(rekognition_json_content))       #b'{\n    "Labels": [\n        {\n            "Name": "Tree",\n 



    return jsonify(status=200, message="OK", existing=True, key=_k), 200, {'Content-Type':'application/json;charset=utf-8'}



##GET    /s3/<s3bucket>/<s3object>     # List object
#@app.route("/s3/<s3bucket>/<s3object>", methods=['GET'])
#def get_s3bucketobject(s3bucket=None,s3object=None):
#    assert s3bucket == request.view_args['s3bucket']
#    assert s3object == request.view_args['s3object']
#
#    s3_client = boto3.client('s3')
#
#    try:
#        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")
#    except botocore.exceptions.EndpointConnectionError as e:
#        return jsonify(status=599, message="EndpointConnectionError", error=str(e)), 599
#
#    try:
#        for key in s3_result['Contents']:
#            #print(key['Key'])
#            _k = key['Key']
#    except KeyError as e:
#        _k = s3_result['Prefix']
#        return jsonify(status=404, message="Not Found", existing=False, key=_k), 404
#
#    if _k != s3object:
#        return jsonify(status=569, message="Objects Do Not Match", object1=str(_k), ojbect2=str(s3object)), 569
#
#    return jsonify(status=200, message="OK", existing=True, key=_k), 200


#GET    /s3/<s3bucket>/<s3subdir>/<s3object> # List object
@app.route("/s3/<s3bucket>/<s3subdir>/<s3object>", methods=['GET'])
def get_s3bucketsubdirobject(s3bucket=None,s3subdir=None,s3object=None):
    assert s3bucket == request.view_args['s3bucket']
    assert s3subdir == request.view_args['s3subdir']
    assert s3object == request.view_args['s3object']

    #print(s3bucket)
    #print(s3subdir)
    #print(s3object)

    s3_client = boto3.client('s3')

    prefix = s3subdir + '/' + s3object

    s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix, Delimiter = "/")

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
        return jsonify(status=404, message="Not Found", existing=_exist, key=_k), 404, {'Content-Type':'application/json;charset=utf-8'}

    return jsonify(status=200, message="OK", existing=_exist, key=_k), 200, {'Content-Type':'application/json;charset=utf-8'}


#GET    /s3/<s3bucket>/<s3subdir>/                         # List bucket directory files (1000 limit)
@app.route("/s3/<s3bucket>/<s3subdir>/", methods=['GET'])
def get_s3bucketsubdir(s3bucket=None,s3subdir=None):       
    assert s3bucket == request.view_args['s3bucket']
    assert s3subdir == request.view_args['s3subdir']

    #print(s3bucket)
    #print(s3subdir)

    #bucket      = "bucket-name"
    #prefix      = "folder/"

    prefix = s3subdir + '/'

    s3_client = boto3.client('s3')
    s3_result =  s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix, Delimiter = "/")

    s3objects=[]
    try:
        for key in s3_result['Contents']:
            #print(key['Key'])
            s3objects.append(key['Key'])
    except KeyError as e:
        return jsonify(status=404, message="Not Found", prefix=prefix), 404, {'Content-Type':'application/json;charset=utf-8'}

    print(len(s3objects))
    if len(s3objects) < 0:
        return jsonify(status=404, message="Not Found", existing=False, prefix=prefix), 404, {'Content-Type':'application/json;charset=utf-8'}

    #return jsonify(status=200, message="OK", existing=_exist, key=None), 200
    #return jsonify(status=200, message="OK", prefix=prefix), 200
    return jsonify(s3objects), 200, {'Content-Type':'application/json;charset=utf-8'}
     

#GET    /s3/<s3bucket>/                         # List bucket directory files (1000 limit)
@app.route("/s3/<s3bucket>/", methods=['GET'])
def get_s3bucketdir(s3bucket=None):             
    assert s3bucket == request.view_args['s3bucket']

    #print(s3bucket)

    s3_client = boto3.client('s3')

    try:
        s3_result =  s3_client.list_objects_v2(Bucket=s3bucket, Delimiter = "/")
    except s3_client.exceptions.NoSuchBucket as e:
        return jsonify(status=404, message="No Such Bucket", s3bucket=s3bucket), 404, {'Content-Type':'application/json;charset=utf-8'}

    s3objects=[]
    try:
        for key in s3_result['Contents']:
            s3objects.append(key['Key'])
    except KeyError as e:
        return jsonify(status=404, message="Not Found", s3bucket=s3bucket), 404, {'Content-Type':'application/json;charset=utf-8'}

    #print(len(s3objects))
    if len(s3objects) < 0:
        return jsonify(status=404, message="Not Found", s3objects=0, s3bucket=s3bucket), 404, {'Content-Type':'application/json;charset=utf-8'}

    return jsonify(s3objects), 200, {'Content-Type':'application/json;charset=utf-8'}
     


@app.errorhandler(404)
def not_found(error=None):
    message = { 'status': 404, 'errorType': 'Not Found: ' + request.url }
    return jsonify(message), 404, {'Content-Type':'application/json;charset=utf-8'}

###############################################################################################################################################

def get_rekognition_json(s3bucket, s3object):
    rekognition_json_file = 'rekognition/' + s3object + '.json'
    #s3 = boto3.resource('s3')
    #getobject = s3.Object('bucket_name','key')
    #getobject = s3.Object(s3bucket, s3object)
    #print(str(type(getobject))) #<class 'boto3.resources.factory.s3.Object'>
    #print(str(getobject))       #s3.Object(bucket_name='ninfo-property-images', key='2eece964b6f902124052810e5a92d6f9ca715c1b.jpg')
    #return getobject
    #obj = s3.Object(s3bucket, s3object)
    #body = obj.get()['Body'].read()

    return get_s3object_body(s3bucket, rekognition_json_file)


def get_s3object_body(s3bucket, s3object):
    s3 = boto3.resource('s3')
    obj = s3.Object(s3bucket, s3object)
    body = obj.get()['Body'].read()
    return body


def main():
    app.run(port=8880, debug=False)    


if __name__ == "__main__":
    main()


#https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-demo_bucket_basics.py.html
#https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html


