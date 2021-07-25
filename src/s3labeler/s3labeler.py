#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = '0.0.0.a6'

import sys

if sys.version_info < (3, 8, 1):
    raise RuntimeError('Requires Python version 3.8.1 or higher. This version: ' + str(sys.version_info))

usage = "Usage: " + sys.argv[0] + " option" + """

    options:

        buckets

        ls <s3bucket>/<s3object>

        get <s3bucket>/<s3object>
        set <s3bucket>/<s3object> '{"label":"value"}'
        del <s3bucket>/<s3object> label

        server 8880

        --help
        --version
"""


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

            rekognition_json_content = get_rekognition_json(s3bucket, rekognition_json_file)

            if rekognition_json_content:
                return rekognition_json_content, 200, {'Content-Type':'application/json;charset=utf-8'}
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404, {'Content-Type':'application/json;charset=utf-8'}


    if rekognition:

        if rekognition == 'json':
            rekognition_json_file = 'rekognition/' + s3object + '.json'
            rekognition_json_content = get_rekognition_json(s3bucket, rekognition_json_file)
            if rekognition_json_content:
                return rekognition_json_content, 200, {'Content-Type':'application/json;charset=utf-8'}
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404, {'Content-Type':'application/json;charset=utf-8'}

        if rekognition == 'words':

            rekognition_json_file = 'rekognition/' + s3object + '.json'
            rekognition_json_content = get_rekognition_json(s3bucket, rekognition_json_file)

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

def get_rekognition_json(s3bucket, s3object):
    return get_s3object_body(s3bucket, s3object)


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

    # write out new dic
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

def list_s3buckets():
    s3 = boto3.resource('s3')
    bucket_list = [b.name for b in s3.buckets.all()]
    return bucket_list


def get_s3bucket_objects(s3path):

    s3bucket = s3path.split("/", 1)[0]
    try:
        s3prefix = s3path.split("/", 1)[1]
    except IndexError as e:
        s3prefix = ''

    #print(s3bucket)
    #print(s3prefix)

    s3_client = boto3.client('s3')

    s3_result = {}

    try:
        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3prefix, Delimiter = "/")

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

    


def main():

    if sys.argv[1:]:

        if sys.argv[1] == "--help":
            sys.exit(print(usage))

        if sys.argv[1] == "--version":
            sys.exit(print(__version__))

        if sys.argv[1] == "buckets":
            buckets = list_s3buckets()
            sys.exit(print(json.dumps(buckets, indent=2)))

        if sys.argv[1] == "ls":

            s3path = sys.argv[2]

            s3objects = get_s3bucket_objects(s3path)

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

            else:
                #botoerror = s3objects['ResponseMetadata']['BotoError']
                #print(botoerror)
                print(s3objects['ResponseMetadata']['BotoError'])

            sys.exit()

#        if sys.argv[1] == "objects":
#            s3path = sys.argv[2]
#            objects = list_s3bucket_objects(s3path)
#            print(objects)
#            sys.exit()
#            #sys.exit(print(json.dumps(objects, indent=2)))

        if sys.argv[1] == "get":
            print('get')
            s3bucket = sys.argv[2]
            s3object = sys.argv[3]

            print(s3bucket)
            print(s3object)
            
            sys.exit()

        if sys.argv[1] == "server":
            try:
                port = int(sys.argv[2])
            except IndexError:
                port = 8880
            #app.run(port=8880, debug=False)    
            app.run(port=port, debug=False)    

    else:
        sys.exit(print(usage))

if __name__ == "__main__":
    main()


#https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-s3_basics-demo_bucket_basics.py.html
#https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html

#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html


#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html


