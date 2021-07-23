#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = '0.0.0.a2'

import sys

if sys.version_info < (3, 8, 1):
    raise RuntimeError('Requires Python version 3.8.1 or higher. This version: ' + str(sys.version_info))

from flask import Flask
from flask import request
from flask import jsonify
from werkzeug.exceptions import HTTPException

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True    #default False
app.config['JSON_SORT_KEYS'] = True                 #default True
app.config['JSONIFY_MIMETYPE'] = 'application/json' #default 'application/json'

import boto3
import botocore
#from botocore.exceptions import ClientError

import json

#GET    /                             # Show version
@app.route("/", methods=['GET'])
def root():
    return jsonify(status=200, message="OK", version=__version__), 200, {'Content-Type':'application/json;charset=utf-8'}


#GET    /s3                           # Show OK
@app.route("/s3", methods=['GET'])
def get_s3():
    return jsonify(status=200, message="OK", path="/s3"), 200, {'Content-Type':'application/json;charset=utf-8'}


#GET    /s3/                          # List all buckets (limt 1000?)
@app.route("/s3/", methods=['GET'])   # fun, this method has a "Bucket List"
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

#GET    /s3/<s3bucket>/<s3object>?q=
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
        return jsonify(status=404, message="Not Found", s3object=False, name=_k), 404, {'Content-Type':'application/json;charset=utf-8'}

    if _k != s3object:
        return jsonify(status=569, message="Objects Do Not Match", object1=str(_k), ojbect2=str(s3object)), 569, {'Content-Type':'application/json;charset=utf-8'}

    #s3object#

    if tags:

        if tags == 's3':
            print('s3tags')
            #aws s3api get-object-tagging --bucket ninfo-property-images --key 2eece964b6f902124052810e5a92d6f9ca715c1b.jpg

            get_s3tags = get_s3object_tags(s3bucket, s3object)
            #s3tags = get_s3object_tags(s3bucket, s3object)
            #print(s3tags)

            #for key in s3tags['TagSet']:
            #    #print(key)
            #    print(key['Key'], key['Value'])

            #print(str(s3tags['TagSet']))

            s3Tags = {}
            for key in get_s3tags['TagSet']:
                __k = key['Key']
                __v = key['Value']
                s3Tags[__k]=__v

            return jsonify(s3Tags), 200, {'Content-Type':'application/json;charset=utf-8'}


        if tags == 'rekognition':
            #print('rekognition json')
            #retrieve corresponding rekognition json file
            # /rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json

            rekognition_json_content = get_rekognition_json(s3bucket, s3object)
            #print(str(rekognition_json_content))

            #print('DEV.TEST')
            #print(str(type(rekognition_json_content))) #<class 'bytes'>
            #print(str(rekognition_json_content))       #b'{\n    "Labels": [\n        {\n            "Name": "Tree",\n 

            #print(str(type(rekognition_json_content))) #<class 'str'>
            #print(str(rekognition_json_content))       #{ "Labels": [ { "Name": "Tree", 

            #print(rekognition_json_content)

            #jdata = json.loads(rekognition_json_content)
            #print(jdata)

            #return jsonify(rekognition_json_content), 200, {'Content-Type':'application/json;charset=utf-8'}

            if rekognition_json_content:
                return rekognition_json_content, 200, {'Content-Type':'application/json;charset=utf-8'}
            else:
                location_str = 'rekognition/' + s3object + '.json'
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=location_str), 404, {'Content-Type':'application/json;charset=utf-8'}


            



    return jsonify(status=200, message="OK", s3object=True, name=_k), 200, {'Content-Type':'application/json;charset=utf-8'}



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


#PUT      /s3/<s3bucket>/<s3object>         # set s3object tag set keys and values   
@app.route("/s3/<s3bucket>/<s3object>", methods=['PUT'])
def set_s3bucketobject(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    if not request.headers['Content-Type'] == 'application/json':
        return jsonify(status=412, errorType="Precondition Failed"), 412

    jpost = request.get_json()

    #print(str(type(post))) #<class 'dict'>
    #print(str(post))       #{'labler': 'karl...
    
    #jsondata = json.loads('{}')

    #settagset = set_s3object_tags(s3bucket, s3object, jsondata)
    settagset = set_s3object_tags(s3bucket, s3object, jpost)

    print(settagset)


    return jsonify(status=200, message="OK", name=s3object, method="PUT"), 200, {'Content-Type':'application/json;charset=utf-8'}



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
        return jsonify(status=404, message="Not Found", s3object=_exist, name=_k), 404, {'Content-Type':'application/json;charset=utf-8'}

    return jsonify(status=200, message="OK", s3object=_exist, key=_k), 200, {'Content-Type':'application/json;charset=utf-8'}


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
        return jsonify(status=404, message="Not Found", s3object=False, prefix=prefix), 404, {'Content-Type':'application/json;charset=utf-8'}

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


@app.errorhandler(Exception)
def handle_exception(e):

    if isinstance(e, HTTPException):
        return jsonify(status=e.code, errorType="HTTPException", errorMessage=str(e)), e.code, {'Content-Type':'application/json;charset=utf-8'}

    return jsonify(status=599, errorType="Exception", errorMessage=str(e)), 599, {'Content-Type':'application/json;charset=utf-8'}


@app.errorhandler(404)
def not_found(error=None):
    message = { 'status': 404, 'errorType': 'Not Found: ' + request.url }
    return jsonify(message), 404, {'Content-Type':'application/json;charset=utf-8'}



#@app.errorhandler(Exception)
#def handle_exception(e):
#
#    if isinstance(e, HTTPException):
#        return jsonify(status=e.code, errorType="HTTP Exception", errorMessage=str(e)), e.code
#
#    if type(e).__name__ == 'OperationalError':
#        return jsonify(status=512, errorType="OperationalError", errorMessage=str(e)), 512
#
#    if type(e).__name__ == 'InterfaceError':
#        return jsonify(status=512, errorType="InterfaceError", errorMessage=str(e)), 512
#
#    if type(e).__name__ == 'ProgrammingError':
#        return jsonify(status=512, errorType="ProgrammingError", errorMessage=str(e)), 512
#
#    if type(e).__name__ == 'AttributeError':
#        return jsonify(status=512, errorType="AttributeError", errorMessage=str(e)), 512
#
#    res = {'status': 500, 'errorType': 'Internal Server Error'}
#    res['errorMessage'] = str(e)
#    return jsonify(res), 500
#


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


def get_s3object_body(s3bucket, s3object): #gets file contents data
    s3 = boto3.resource('s3')
    obj = s3.Object(s3bucket, s3object)
    try:
        body = obj.get()['Body'].read()
    except botocore.exceptions.ClientError as ex:
        print('ClientError ' + str(ex))
        #if ex.response['Error']['Code'] == 'NoSuchKey':
        #    print('NoSuchKey')
        #    return None
        return None

    #return body                #<class 'bytes'>
    return body.decode('utf-8') #<class 'str'>

def get_s3object_tags(s3bucket, s3object):
    s3_client = boto3.client('s3')
    s3_result = s3_client.get_object_tagging(Bucket=s3bucket, Key=s3object)
    return s3_result

def set_s3object_tags(s3bucket, s3object, jpost):

    s3_client = boto3.client('s3')

    print(str(jpost))

    #s3_result = s3_client.put_object_tagging(
    #        Bucket=s3bucket,
    #        Key=s3object,
    #        Tagging={'TagSet':[]}
    #        )

    return None

#put_tags_response = s3_client.put_object_tagging(
#    Bucket='your-bucket-name',
#    Key='folder-if-any/file-name.extension',
#    Tagging={
#        'TagSet': [
#            {
#                'Key': 'tag-key',
#                'Value': 'tag-value'
#            },
#        ]
#    }
#)



def main():
    app.run(port=8880, debug=False)    


if __name__ == "__main__":
    main()


#https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-demo_bucket_basics.py.html
#https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html

#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html


