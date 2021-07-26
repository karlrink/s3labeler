#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = '0.0.0.a8'

import sys

if sys.version_info < (3, 8, 1):
    raise RuntimeError('Requires Python version 3.8.1 or higher. This version: ' + str(sys.version_info))

usage = "Usage: " + sys.argv[0] + " option" + """

    options:

        buckets

        ls  <s3bucket>/<s3object>
        set <s3bucket>/<s3object> '{"label":"value"}'
        del <s3bucket>/<s3object> label

        get    <s3bucket>/<s3object>
        save   <s3bucket>/<s3object> destination
        upload source <s3bucket>/<s3object>

        rekognition <s3bucket>/<s3object>
        rekognition <s3bucket>/<s3object> detect-labels
        rekognition <s3bucket>/<s3object> detect-labels destination

        object   <s3bucket>/<s3object>
        identify <s3bucket>/<s3object>

        server 8880

        --help
        --version
"""

import boto3
import botocore

import json

from hashlib import blake2b, blake2s

from flask import Flask
from flask import request
from flask import jsonify
from werkzeug.exceptions import HTTPException

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True    #default False
app.config['JSON_SORT_KEYS'] = True                 #default True
app.config['JSONIFY_MIMETYPE'] = 'application/json' #default 'application/json'


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
#GET    /s3/<s3bucket>/<s3object>?q=  # rekognition=json|words|detect-labels
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

            rekognition_json_file = 'rekognition/' + s3object + '.json'

            #rekognition_json_content = get_rekognition_json(s3bucket, rekognition_json_file)
            rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

            if rekognition_json_content:
                return rekognition_json_content, 200, {'Content-Type':'application/json;charset=utf-8'}
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404, {'Content-Type':'application/json;charset=utf-8'}


    if rekognition:

        if rekognition == 'json':
            rekognition_json_file = 'rekognition/' + s3object + '.json'
            #rekognition_json_content = get_rekognition_json(s3bucket, rekognition_json_file)
            rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)
            if rekognition_json_content:
                return rekognition_json_content, 200, {'Content-Type':'application/json;charset=utf-8'}
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404, {'Content-Type':'application/json;charset=utf-8'}

        if rekognition == 'words':

            rekognition_json_file = 'rekognition/' + s3object + '.json'
            #rekognition_json_content = get_rekognition_json(s3bucket, rekognition_json_file)
            rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

            #print(str(type(rekognition_json_content)))
            #print(str(rekognition_json_content))

            #_wL = extract_rekognition_words(rekognition_json_content)
            #print(str(_wL))

            if rekognition_json_content:

                #wordList = [ 'Tree', 'Garage', 'Donuts' ]

                wordList = extract_rekognition_words(rekognition_json_content)
                print(str(wordList))

                return jsonify(wordList), 200, {'Content-Type':'application/json;charset=utf-8'}
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404, {'Content-Type':'application/json;charset=utf-8'}


            #wordList = [ 'Tree', 'Garage', 'Donuts' ]
            #if len(wordList) > 0:
            #    return jsonify(wordList), 200, {'Content-Type':'application/json;charset=utf-8'}
            #else:
            #    return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404, {'Content-Type':'application/json;charset=utf-8'}


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

    post = request.get_json()

    #print(str(type(post))) #<class 'dict'>
    #print(str(post))       #{'labler': 'karl...
    
    #jsondata = json.loads('{}')

    #settagset = set_s3object_tags(s3bucket, s3object, jsondata)
    settagset = set_s3object_tags(s3bucket, s3object, post)

    print(settagset)

    return jsonify(status=200, message="OK", name=s3object, method="PUT"), 200, {'Content-Type':'application/json;charset=utf-8'}


#PATCH    /s3/<s3bucket>/<s3object>         # set s3object tag    
@app.route("/s3/<s3bucket>/<s3object>", methods=['PATCH'])
def set_s3bucketobjectpatch(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    if not request.headers['Content-Type'] == 'application/json':
        return jsonify(status=412, errorType="Precondition Failed"), 412

    post = request.get_json()

    if len(post) > 1:
        return jsonify(status=405, errorType="Method Not Allowed", errorMessage="Single Key-Value Only", update=False), 405

    print(post)
    for k,v in post.items():
        tag=k
        value=v

    update = update_s3object_tag(s3bucket, s3object, tag, value)
   
    print(update)

    if update is True:
        return jsonify(status=200, message="OK", name=s3object, method="PATCH"), 200, {'Content-Type':'application/json;charset=utf-8'}

    #return jsonify(status=200, message="OK", name=s3object, method="PUT"), 200, {'Content-Type':'application/json;charset=utf-8'}
    return jsonify(status=465, message="Failed Patch", name=s3object, tag=tag, method="PATCH", update=False), 465, {'Content-Type':'application/json;charset=utf-8'}




#DELETE      /s3/<s3bucket>/<s3object>?tag=name      # delete s3object tag 
@app.route("/s3/<s3bucket>/<s3object>", methods=['DELETE'])
def delete_s3bucketobjecttag(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    tag = request.args.get("tag", None)

    if tag:
        #return jsonify(status=466, message="No Such Tag", name=s3object, tag=tag, method="DELETE", delete=False), 466, {'Content-Type':'application/json;charset=utf-8'}
        #return jsonify(status=466, message="No Such Tag", name=s3object, tag=tag, method="DELETE", delete=False), 466, {'Content-Type':'application/json;charset=utf-8'}

        delete_tag = delete_s3object_tag(s3bucket, s3object, tag)

        if delete_tag is True:

            return jsonify(status=211, message="Deleted", name=s3object, tag=tag, method="DELETE", delete=True), 211, {'Content-Type':'application/json;charset=utf-8'}


    #return jsonify(status=211, message="Deleted", name=s3object, tag=tag, method="DELETE", delete=True), 211, {'Content-Type':'application/json;charset=utf-8'}
    return jsonify(status=466, message="Failed Delete", name=s3object, tag=tag, method="DELETE", delete=False), 466, {'Content-Type':'application/json;charset=utf-8'}




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

#def get_rekognition_json(s3bucket, s3object):
#    return get_s3object_body(s3bucket, s3object)


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

def set_s3object_tags(s3bucket, s3object, post):
    """ multi key/value """

    s3_client = boto3.client('s3')

    KeyValList = []

    for k,v in post.items():
        kvs={}
        kvs['Key']   = k
        kvs['Value'] = v

        KeyValList.append(kvs)

    TagSet = { 'TagSet': KeyValList }

    s3_result = s3_client.put_object_tagging(
            Bucket=s3bucket,
            Key=s3object,
            Tagging=TagSet
            )
    status_code = s3_result['ResponseMetadata']['HTTPStatusCode']

    if int(status_code) == 200:
        return True
    else:
        return False
    #return None



def update_s3object_tag(s3bucket, s3object, tag, value):
    """ single key/value """

    s3_client = boto3.client('s3')

    get_tags_response = s3_client.get_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
    )

    # get existing tags
    s3Tags = {}
    for key in get_tags_response['TagSet']:
        __k = key['Key']
        __v = key['Value']
        s3Tags[__k]=__v

    # overwrite existing key/value w/ new 
    s3Tags[tag]=value

    # write out new dict
    KeyValList = []
    for k,v in s3Tags.items():
        kvs={}
        kvs['Key']   = k
        kvs['Value'] = v
        KeyValList.append(kvs)

    TagSet = { 'TagSet': KeyValList }

    # put new dict
    put_tags_response = s3_client.put_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
        Tagging=TagSet
    )

    #print(str(put_tags_response))
    #for key in put_tags_response['ResponseMetadata']:

    status_code = put_tags_response['ResponseMetadata']['HTTPStatusCode']

    if int(status_code) == 200:
        return True
    else:
        return False




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

def delete_s3object_tag(s3bucket, s3object, tag):
    
    s3_client = boto3.client('s3',
    #region_name='region-name',
    #aws_access_key_id='aws-access-key-id',
    #aws_secret_access_key='aws-secret-access-key',
    )

    get_tags_response = s3_client.get_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
    )
    get_tags_response_code = get_tags_response['ResponseMetadata']['HTTPStatusCode']
    if int(get_tags_response_code) != 200:
        return False

    # get existing tags
    s3Tags = {}
    for key in get_tags_response['TagSet']:
        __k = key['Key']
        __v = key['Value']
        s3Tags[__k]=__v

    #overwrite existing key/value w/ new 
    #s3Tags[tagkey]=tagval

    # delete the key regardless
    #s3Tags.pop(tag, None)

    if tag not in s3Tags:
        return False

    # delete the key regardless
    delete = s3Tags.pop(tag, None)
    if delete is None:
        return False

    #write out new dict
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

    #print(str(put_tags_response))
    #for key in put_tags_response['ResponseMetadata']:

    put_tags_response_code = put_tags_response['ResponseMetadata']['HTTPStatusCode']

    if int(put_tags_response_code) == 200:
        return True
    else:
        return False



def extract_rekognition_words(rekognition_json_content):

    data = json.loads(rekognition_json_content)

    List = []

    for key in data['Labels']:
        List.append(str(key['Name']))

#order matters...
# dict
#{'Name': 'Tree', 'Confidence': 90.44393920898438, 'Instances': [], 'Parents': [{'Name': 'Plant'}]}
#{'Name': 'Plant', 'Confidence': 90.44393920898438, 'Instances': [], 'Parents': []}
#{'Name': 'Urban', 'Confidence': 85.79068756103516, 'Instances': [], 'Parents': []}
#{'Name': 'Building', 'Confidence': 74.22905731201172, 'Instances': [], 'Parents': []}
#{'Name': 'Flagstone', 'Confidence': 72.1240005493164, 'Instances': [], 'Parents': []}
#{'Name': 'Vegetation', 'Confidence': 71.17005920410156, 'Instances': [], 'Parents': [{'Name': 'Plant'}]}
#{'Name': 'Garage', 'Confidence': 66.02606964111328, 'Instances': [], 'Parents': []}
#{'Name': 'Housing', 'Confidence': 65.3967056274414, 'Instances': [], 'Parents': [{'Name': 'Building'}]}
#{'Name': 'Tarmac', 'Confidence': 64.69490814208984, 'Instances': [], 'Parents': []}
#{'Name': 'Asphalt', 'Confidence': 64.69490814208984, 'Instances': [], 'Parents': []}
#{'Name': 'Home Decor', 'Confidence': 62.33171081542969, 'Instances': [], 'Parents': []}
#{'Name': 'Walkway', 'Confidence': 58.68953323364258, 'Instances': [], 'Parents': [{'Name': 'Path'}]}
#{'Name': 'Path', 'Confidence': 58.68953323364258, 'Instances': [], 'Parents': []}
#{'Name': 'Suburb', 'Confidence': 57.164859771728516, 'Instances': [], 'Parents': [{'Name': 'Urban'}, {'Name': 'Building'}]}
#{'Name': 'Bush', 'Confidence': 56.60989761352539, 'Instances': [], 'Parents': [{'Name': 'Vegetation'}, {'Name': 'Plant'}]}
#{'Name': 'Slate', 'Confidence': 56.16239547729492, 'Instances': [], 'Parents': []}
#{'Name': 'Concrete', 'Confidence': 55.33432388305664, 'Instances': [], 'Parents': []}

#[
#  "Tree", 
#  "Plant", 
#  "Urban", 
#  "Building", 
#  "Flagstone", 
#  "Vegetation", 
#  "Garage", 
#  "Housing", 
#  "Tarmac", 
#  "Asphalt", 
#  "Home Decor", 
#  "Walkway", 
#  "Path", 
#  "Suburb", 
#  "Bush", 
#  "Slate", 
#  "Concrete"
#]

    return List

#def list_s3buckets():
#    s3 = boto3.resource('s3')
#    #print('hit buckets')
#    try:
#        bucket_list = [b.name for b in s3.buckets.all()]
#    except botocore.exceptions.EndpointConnectionError as e:
#        #print(json.dumps({'EndpointConnectionError':'s3.buckets'}))
#        #return []
#        #return [json.dumps({'EndpointConnectionError':'s3.buckets'})]
#        #return [{'EndpointConnectionError':'s3.buckets', 'errorMessage':''}]
#        return [{'EndpointConnectionError': str(e) }]
#    #except botocore.exceptions.ClientError as e:
#    #    if e.response['Error']['Code'] == 'EndpointConnectionError':
#    #        print(json.dumps({'EndpointConnectionError':s3object}))
#    #        return []
#
#    return bucket_list


def list_s3buckets():
    s3 = boto3.resource('s3')
    bucket_list = [b.name for b in s3.buckets.all()]
    return bucket_list





#def get_s3bucket_objects(s3path):
def get_s3bucket_objects(s3bucket, s3object):

    #s3bucket = s3path.split("/", 1)[0]
    #try:
    #    s3prefix = s3path.split("/", 1)[1]
    #except IndexError as e:
    #    s3prefix = ''

    #print(s3bucket)
    #print(s3prefix)

    s3_client = boto3.client('s3')

    s3_result = {}

    try:
        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")

    #except botocore.exceptions.ClientError as ex:
    #    raise RuntimeError('s3 ClientError: ' + str(ex))
    except botocore.exceptions.EndpointConnectionError as e:
        #s3_result = None
        #raise RuntimeError('s3 EndpointConnectionError: ' + str(ex))
        #print('s3 EndpointConnectionError: ' + str(e))
        s3_result['ResponseMetadata'] = {'HTTPStatusCode': 0, 'BotoError': str(e)}


    return s3_result
    


#def bucketprefix(s3path):
#    print(s3path)
#    s3bucket = s3path.split("/")[0]
#    s3prefix = s3path.split("/")[1:]
#    print(s3bucket, ' ', s3prefix)
#    return s3bucket, s3prefix

   

###############################################################################################################################################


def b2sum(_file):
    is_64bits = sys.maxsize > 2**32
    if is_64bits:
        blake = blake2b(digest_size=20)
    else:
        blake = blake2s(digest_size=20)
    try:
        with open(_file, 'rb') as bfile:
            _f = bfile.read()
    except FileNotFoundError as e:
        return str(e)

    blake.update(_f)
    return str(blake.hexdigest())

def b2checksum(_string):
    is_64bits = sys.maxsize > 2**32
    if is_64bits:
        blake = blake2b(digest_size=20)
    else:
        blake = blake2s(digest_size=20)

    s = _string.encode('utf-8')

    blake.update(s)
    return str(blake.hexdigest())


###############################################################################################################################################


def main():

    if sys.argv[1:]:

        if sys.argv[1] == "--help":
            sys.exit(print(usage))

        if sys.argv[1] == "--version":
            sys.exit(print(__version__))

        if sys.argv[1] == "buckets":

            try:
                buckets = list_s3buckets()
            #except botocore.exceptions.ClientError as e:
            #except botocore.exceptions.EndpointConnectionError as e:
            except Exception as e:
                #buckets = None
                #sys.exit(print(e))
                print(json.dumps({'error':str(e)}))
                sys.exit(1)
            

            sys.exit(print(json.dumps(buckets, indent=2)))

        if sys.argv[1] == "del":
            s3path = sys.argv[2]
            tag    = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            delete = delete_s3object_tag(s3bucket, s3object, tag)

            if delete == True:
                print(json.dumps({'delete':True}))
                sys.exit(0)
            else:
                print(json.dumps({'delete':False}))
                sys.exit(1)


        if sys.argv[1] == "set":

            s3path = sys.argv[2]
            s3json = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]

            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            data = json.loads(s3json)

            #print(len(data))

            if len(data) > 1:
                print('data larger than 1')
                sys.exit(1)

           
            for k,v in data.items():
                #print(k)
                #print(v)
                tag = k
                val = v

            
            #update = update_s3object_tag(s3bucket, s3object, tag, value)

            update = update_s3object_tag(s3bucket, s3object, tag, val)
            #print(update)

            if update == True:
                print(json.dumps({'update':True}))
                sys.exit(0)
            else:
                print(json.dumps({'update':False}))
                sys.exit(1)
            

        if sys.argv[1] == "ls":
            s3path = sys.argv[2]
            s3bucket = s3path.split("/", 1)[0]

            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            if s3path.endswith("/"):
                #list all files
                #list_objects = list_s3objects(s3bucket, s3object)

                #print('list.all')

                #s3object = s3object + '/'

                s3_client = boto3.client('s3')

                #for key in s3_client.list_objects(Bucket=s3bucket, Prefix=s3object)['Contents']:
                #for key in s3_client.list_objects(Bucket=s3bucket, Prefix=s3object)['Contents']:
                #    print(key['Key'])

                #for key in s3_client.list_objects(Bucket=s3bucket, Prefix=s3object):

                #for key in s3_client.list_objects(Bucket=s3bucket, Prefix=s3object)['Contents']:

                List=[]
                for key in s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object)['Contents']:
                    #print(key['Key'])
                    List.append(key['Key'])
                print(json.dumps(List, indent=2))

                sys.exit(0)


            #get_s3tags={}
            #get_s3tags['TagSet']=[]

            try:
                get_s3tags = get_s3object_tags(s3bucket, s3object)

            except botocore.exceptions.EndpointConnectionError as e:
                print(json.dumps({'EndpointConnectionError':str(e)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.ParamValidationError as e:
                print(json.dumps({'ParamValidationError':str(e)}, indent=2))
                sys.exit(1)

            #except botocore.errorfactory.NoSuchKey as e:
            except botocore.exceptions.ClientError as e:
                #print('BotoError ' + str(e))
                if e.response['Error']['Code'] == 'NoSuchKey':
                    #print('NoSuchKey')
                    #print('NoSuchKey: ' + str(s3object))
                   #print(json.dumps({'NoSuchKey':s3object, 'ClientError':str(e)}, indent=2))
                   print(json.dumps({'NoSuchKey':s3object}, indent=2))
                   #sys.exit(1)
                else:
                   # print(e)
                   print(json.dumps({'ClientError':str(e)}, indent=2))
                   #sys.exit(1)

                sys.exit(1)

            #print(s3tags)

            s3Tags = {}
            for key in get_s3tags['TagSet']:
                __k = key['Key']
                __v = key['Value']
                s3Tags[__k]=__v

            print(json.dumps(s3Tags, indent=2))
            #print(json.dumps(s3Tags))
            sys.exit(0)

        if sys.argv[1] == "ls-v1":

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''


            #s3objects = get_s3bucket_objects(s3path)
            s3objects = get_s3bucket_objects(s3bucket, s3object)

            http_status_code = s3objects['ResponseMetadata']['HTTPStatusCode']

            #print(objects)


            if int(http_status_code) == 200:
                #print('good data')
                #print(s3objects)

                if 'CommonPrefixes' in s3objects:
                    for key in s3objects['CommonPrefixes']:
                        #print(key)
                        print(key['Prefix'])

                if 'Contents' in s3objects:
                    for key in s3objects['Contents']:
                        #print(key)
                        print(key['Key'])
                        #s3tags = get_s3object_tags(s3bucket, s3object)

                        s3tags = get_s3object_tags(s3bucket, key['Key'])
                        print(s3tags)


            else:
                #botoerror = s3objects['ResponseMetadata']['BotoError']
                #print(botoerror)
                print(s3objects['ResponseMetadata']['BotoError'])

            sys.exit()


        if sys.argv[1] == "get":
            #print('get')

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            #print(s3bucket)
            #print(s3object)

            content = get_s3object_body(s3bucket, s3object)

            #print(content)
            print(content.rstrip())

            sys.exit(0) 

        if sys.argv[1] == "save":
            #print('save')

            s3path      = sys.argv[2]
            destination = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            #print(s3bucket)
            #print(s3object)

            #content = get_s3object_body(s3bucket, s3object)
            #print(content)

            #download
            s3 = boto3.client('s3')

            #with open('FILE_NAME', 'wb') as f:
            #    s3.download_fileobj('BUCKET_NAME', 'OBJECT_NAME', f)

            #with open(destination, 'wb') as f:
            #    s3.download_fileobj(s3bucket, s3object, f)
            #    print(json.dumps({'saved':destination}, indent=2))
            #    sys.exit(0)
            #else:
            #    print(json.dumps({'failed':destination}))
            #    sys.exit(1)

            #s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')

            s3 = boto3.client('s3')

            try:
                s3.download_file(s3bucket, s3object, destination)

            except botocore.exceptions.ClientError as e:
                #print(e.response)
                #{'Error': {'Code': '404', 'Message': 'Not Found'}, 'ResponseMetadata': {'RequestId': 
                #if e.response['Error']['Message'] == 'Not Found':
                if e.response['Error']['Code'] == '404':
                    print(json.dumps({'Not Found':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)

            except OSError as e:
                print(json.dumps({'OSError':str(e)}, indent=2))
                sys.exit(1)

            #print(download) #None
            print(json.dumps({'saved':destination}, indent=2))

            sys.exit(0)


        if sys.argv[1] == "rekognition":
            print('rekognition')

            s3path = sys.argv[2] #s3bucket/s3object

            try:
                option = sys.argv[3] #detect-labels
            except IndexError as e:
                option = None

            try:
                destination = sys.argv[4] #destination
            except IndexError as e:
                destination = None


            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            print(s3bucket)
            print(s3object)
            print(option)
            print(destination)

            #get rekognition json file content, if it exists

            rekognition_json_file = 'rekognition/' + s3object + '.json'

            s3 = boto3.resource('s3')

            obj = s3.Object(s3bucket, rekognition_json_file)
            try:
                body = obj.get()['Body'].read()

            except botocore.exceptions.ClientError as e:
                #print(e.response)
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(e)}))

                sys.exit(1)

            #print(body)
            #.decode("utf-8", "strict") # 'strict' (raise a UnicodeDecodeError exception)
            
            content = body.decode("utf-8", "strict").rstrip()

            print(content)




            sys.exit()


        if sys.argv[1] == "object":
            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            #print(s3bucket)
            #print(s3object)

            s3 = boto3.resource('s3')
            obj = s3.Object(s3bucket, s3object)

            #print(obj)
            #print(obj.get())
            #body = obj.get()['Body'].read()

            try: 
                s3response = obj.get()

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)
                

            HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            ContentLength  = s3response['ContentLength']
            ContentType    = s3response['ContentType']
            LastModified   = s3response['LastModified']

            #print(HTTPStatusCode)
            #print(ContentLength)
            #print(ContentType)
            #print(LastModified)

            Objects = {}
            #Objects['HTTPStatusCode'] = HTTPStatusCode
            Objects['ContentLength'] = ContentLength
            Objects['ContentType'] = ContentType
            Objects['LastModified'] = LastModified

            #print(Objects)

            #print(json.dumps(Objects, indent=2)) #TypeError: Object of type datetime is not JSON serializable
            # {'HTTPStatusCode': 200, 'ContentLength': 124169, 'ContentType': 'image/jpeg', 'LastModified': datetime.datetime(2021, 7, 24, 23, 50, 21, tzinfo=tzutc())}
            print(json.dumps(Objects, indent=2, sort_keys=True, default=str))

            #sys.exit(print(json.dumps(objects, indent=2)))
            sys.exit(0)

        if sys.argv[1] == "identify":

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            s3 = boto3.resource('s3')
            obj = s3.Object(s3bucket, s3object)

            #s3 = boto3.resource('s3')
            #obj = s3.Object(s3bucket, s3object)
            #s3response = obj.get()

            try:
                s3response = obj.get()

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.EndpointConnectionError as e:
                print(json.dumps({'EndpointConnectionError':str(e)}, indent=2))
                sys.exit(1)


            HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            ContentLength  = s3response['ContentLength']
            ContentType    = s3response['ContentType']
            LastModified   = s3response['LastModified']

            #print(HTTPStatusCode)
            #print(ContentLength)
            #print(ContentType)
            #print(LastModified)

            Objects = {}

            Objects['Name'] = s3object

            #Objects['HTTPStatusCode'] = HTTPStatusCode

            Objects['ContentLength'] = ContentLength
            Objects['ContentType'] = ContentType
            Objects['LastModified'] = LastModified

            #body = obj.get()['Body'].read()

            body = s3response['Body'].read()


            #import tempfile
            #fp = tempfile.TemporaryFile()
            #fp.write(body)
            #with open(fp, 'rb') as bfile:
            #    _f = bfile.read()


            is_64bits = sys.maxsize > 2**32
            if is_64bits:
                blake = blake2b(digest_size=20)
            else:
                blake = blake2s(digest_size=20)

            #with open(body, 'rb') as bfile:
            #    _f = bfile.read()
            #blake.update(_f)
            blake.update(body)
            print(str(blake.hexdigest()))

#                        except botocore.exceptions.EndpointConnectionError as e:
#                print(json.dumps({'EndpointConnectionError':str(e)}, indent=2))
#                sys.exit(1)




            sys.exit(0)





        if sys.argv[1] == "identify-v1":
            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            s3 = boto3.resource('s3')
            obj = s3.Object(s3bucket, s3object)

            #print(obj)
            #print(obj.get())
            #body = obj.get()['Body'].read()

            try:
                s3response = obj.get()

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)


            HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            ContentLength  = s3response['ContentLength']
            ContentType    = s3response['ContentType']
            LastModified   = s3response['LastModified']

            #print(HTTPStatusCode)
            #print(ContentLength)
            #print(ContentType)
            #print(LastModified)

            Objects = {}

            Objects['Name'] = s3object

            #Objects['HTTPStatusCode'] = HTTPStatusCode

            Objects['ContentLength'] = ContentLength
            Objects['ContentType'] = ContentType
            Objects['LastModified'] = LastModified

            #body = obj.get()['Body'].read()

            body = s3response['Body'].read()

            try:
                data = body.decode('utf-8', 'strict').rstrip()
                Objects['Encoding'] = 'utf-8'
                #print(b2checksum(data))
                Objects['b2sum'] = b2checksum(data)
            except UnicodeDecodeError:
                data = body
                Objects['Encoding'] = 'bytes'

            #print(b2checksum(data))
            #print(data)

            
            #print(Objects)
            #print(json.dumps(Objects, indent=2)) #TypeError: Object of type datetime is not JSON serializable
            # {'HTTPStatusCode': 200, 'ContentLength': 124169, 'ContentType': 'image/jpeg', 'LastModified': datetime.datetime(2021, 7, 24, 23, 50, 21, tzinfo=tzutc())}
            print(json.dumps(Objects, indent=2, sort_keys=True, default=str))

            #sys.exit(print(json.dumps(objects, indent=2)))
            sys.exit(0)





        if sys.argv[1] == "server":
            try:
                port = int(sys.argv[2])
            except IndexError:
                port = 8880
            #app.run(port=8880, debug=False)    
            app.run(port=port, debug=False)    

        sys.exit(print(usage))
    else:
        sys.exit(print(usage))

if __name__ == "__main__":
    main()


#https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-demo_bucket_basics.py.html
#https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html
#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html

