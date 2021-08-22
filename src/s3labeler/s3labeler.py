#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__version__ = '1.0.3'

import sys

if sys.version_info < (3, 8, 1):
    raise RuntimeError('Requires Python version 3.8.1 or higher. This version: ' + str(sys.version_info))

usage = "Usage: " + sys.argv[0] + " option" + """

    options:

        list-buckets|buckets

        list|ls    <s3bucket>/<s3object>
        label|set  <s3bucket>/<s3object> '{"label":"value"}'
        delete|del <s3bucket>/<s3object> label

        get    <s3bucket>/<s3object>
        save   <s3bucket>/<s3object> destination
        upload source <s3bucket>/<s3object>

        rekognition <s3bucket>/<s3object>
        rekognition <s3bucket>/<s3object> detect-labels
        rekognition <s3bucket>/<s3object> words
        rekognition <s3bucket>/<s3object> s3tag

        object      <s3bucket>/<s3object>
        b2sum       <s3bucket>/<s3object>
        identify|id <s3bucket>/<s3object>

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

from flask_cors import CORS

app = Flask(__name__)
CORS(app)

app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True    #default False
app.config['JSON_SORT_KEYS'] = True                 #default True
app.config['JSONIFY_MIMETYPE'] = 'application/json' #default 'application/json'


#GET    /                             # Show version
@app.route("/", methods=['GET'])
def root():
    return jsonify(status=200, message="OK", version=__version__), 200


#GET    /s3                           # Show OK
@app.route("/s3", methods=['GET'])
def get_s3():
    return jsonify(status=200, message="OK", path="/s3"), 200


#GET    /s3/                          # List all buckets (limt 1000?)
@app.route("/s3/", methods=['GET'])   # fun, this method has a "Bucket List"
def get_s3buckets(region=None):

    s3 = boto3.resource('s3', region_name=region)

    bucket_list = [b.name for b in s3.buckets.all()]

    return jsonify(bucket_list), 200


#GET    /s3/<s3bucket>/<s3object>     # List object
#GET    /s3/<s3bucket>/<s3object>?q=  # rekognition=json|words|detect-labels
#GET    /s3/<s3bucket>/<s3object>?q=  # tags=s3|rekognition
#GET    /s3/<s3bucket>/<s3object>?q=  # delete=name
#GET    /s3/<s3bucket>/<s3object>?q=  # label=name&value=something

#GET    /s3/<s3bucket>/<s3object>?q=
@app.route("/s3/<s3bucket>/<s3object>", methods=['GET'])
def get_s3bucketobject(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    rekognition = request.args.get("rekognition", None)
    tags        = request.args.get("tags", None)
    save        = request.args.get("save", None)
    image       = request.args.get("image", None)
    delete      = request.args.get("delete", None)
    label       = request.args.get("label", None)
    value       = request.args.get("value", None)

    s3_client = boto3.client('s3')

    if not request.args:

        try:
            get_s3tags = get_s3object_tags(s3bucket, s3object)

        except botocore.exceptions.EndpointConnectionError as e:
            return jsonify(status=599, message="EndpointConnectionError", s3object=False, name=s3object), 599

        except botocore.exceptions.ParamValidationError as e:
            return jsonify(status=599, message="ParamValidationError", s3object=False, name=s3object), 599

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return jsonify(status=404, message="NoSuchKey", s3object=False, name=s3object), 404
            return jsonify(status=599, message="ClientError", s3object=False, name=s3object, error=str(e)), 599

        s3Tags = {}
        for key in get_s3tags['TagSet']:
            __k = key['Key']
            __v = key['Value']
            s3Tags[__k]=__v

        return jsonify(s3Tags), 200


    try:
        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")
    except botocore.exceptions.EndpointConnectionError as e:
        return jsonify(status=599, message="EndpointConnectionError", error=str(e)), 599


    try:
        for key in s3_result['Contents']:
            #print(key['Key'])
            _k = key['Key']
    except KeyError as e:
        _k = s3_result['Prefix']
        return jsonify(status=404, message="Not Found", s3object=False, name=_k), 404

    if _k != s3object:
        return jsonify(status=569, message="Objects Do Not Match", object1=str(_k), ojbect2=str(s3object)), 569


    if image:
        #get s3object and send to browser

        s3 = boto3.resource('s3')
        obj = s3.Object(s3bucket, s3object)
        try:
            body = obj.get()['Body'].read()
        except botocore.exceptions.ClientError as e:
            return jsonify(status=599, message="ClientError", error=str(e)), 599

        #return body                #<class 'bytes'>
        #return body.decode('utf-8') #<class 'str'>

        #return body, 200, {'Content-Type':'image/jpeg'}
        return body, 200, {'Content-Type':'image/' + image}


    if tags:

        if tags == 's3':

            get_s3tags = get_s3object_tags(s3bucket, s3object)

            s3Tags = {}
            for key in get_s3tags['TagSet']:
                __k = key['Key']
                __v = key['Value']
                s3Tags[__k]=__v

            return jsonify(s3Tags), 200


        if tags == 'rekognition':

            rekognition_json_file = 'rekognition/' + s3object + '.json'

            rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

            if rekognition_json_content:
                return rekognition_json_content, 200
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404


    if rekognition:

        if rekognition == 'detect-labels':

            #either specify region or auto get region from boto call
            s3 = boto3.client('s3')
            region = s3.head_bucket(Bucket=s3bucket)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']

            client = boto3.client('rekognition', region_name=region)

            #response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}}, MaxLabels=10)
            response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}})

            if save:

                from tempfile import mkstemp
                fd, path = mkstemp()

                with open(path, 'w') as f:
                    f.write(json.dumps(response, indent=4))

                s3_client = boto3.client('s3')

                try:
                    rekognition_json_file = 'rekognition/' + s3object + '.json'
                    
                    s3_upload = s3_client.upload_file(path, s3bucket, rekognition_json_file)

                except botocore.exceptions.ClientError as e:
                    return jsonify(status=599, message="ClientError", error=str(e)), 599

                return jsonify(response), 201
            else:
                return jsonify(response), 200


        if rekognition == 'json':
            rekognition_json_file = 'rekognition/' + s3object + '.json'
            rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)
            if rekognition_json_content:
                return rekognition_json_content, 200
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404

        if rekognition == 'words':

            rekognition_json_file = 'rekognition/' + s3object + '.json'
            rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

            if rekognition_json_content:

                wordList = extract_rekognition_words(rekognition_json_content)

                if save == 's3tag':

                    s3 = boto3.resource('s3')
                    obj = s3.Object(s3bucket, rekognition_json_file)
                    try:
                        body = obj.get()['Body'].read()

                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == 'NoSuchKey':
                            return jsonify(status=404, message="Not Found", s3object=False, key=rekognition_json_file), 404
                        else:
                            return jsonify(status=599, message="ClientError", s3object=False, key=rekognition_json_file, error=str(e)), 599

                    content = body.decode("utf-8", "strict").rstrip()

                    data = json.loads(content)

                    List=[]
                    for key in data['Labels']:
                        List.append(key['Name'])

                    listToStr = ' '.join([str(elem) for elem in List])

                    tag = 'rekognition-words'

                    update = update_s3object_tag(s3bucket, s3object, tag, listToStr)

                    if update == True:
                        return jsonify(status=201, message="Created S3Tag", label=True), 201
                    else:
                        return jsonify(status=465, message="Failed S3Tag", label=False), 465


                return jsonify(wordList), 200
            else:
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404

    if delete:

        delete_tag = delete_s3object_tag(s3bucket, s3object, delete)

        if delete_tag is True:
            return jsonify(status=211, message="Deleted", name=s3object, tag=delete, method="DELETE", delete=True), 211

        return jsonify(status=466, message="Failed Delete", name=s3object, tag=delete, method="DELETE", delete=False), 466

    if label:

        update = update_s3object_tag(s3bucket, s3object, label, value)

        if update is True:
            return jsonify(status=200, message="OK", name=s3object, label=label, method="GET"), 200

        return jsonify(status=465, message="Failed POST", name=s3object, label=str(label), method="GET", update=False), 465


    return jsonify(status=200, message="OK", s3object=True, name=_k), 200


#PUT      /s3/<s3bucket>/<s3object>         # set s3object tag set keys and values   
@app.route("/s3/<s3bucket>/<s3object>", methods=['PUT'])
def set_s3bucketobject(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    if not request.headers['Content-Type'] == 'application/json':
        return jsonify(status=412, errorType="Precondition Failed"), 412

    post = request.get_json()

    settagset = set_s3object_tags(s3bucket, s3object, post)

    return jsonify(status=200, message="OK", name=s3object, method="PUT"), 200


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

    #print(post)
    for k,v in post.items():
        tag=k
        value=v

    update = update_s3object_tag(s3bucket, s3object, tag, value)
   
    #print(update)

    if update is True:
        return jsonify(status=200, message="OK", name=s3object, method="PATCH"), 200

    return jsonify(status=465, message="Failed Patch", name=s3object, tag=tag, method="PATCH", update=False), 465


#POST    /s3/<s3bucket>/<s3object>         # set s3object tag
# handle form data (form post) and/or json single key/value
@app.route("/s3/<s3bucket>/<s3object>", methods=['POST'])
def set_s3bucketobjectpost(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    if request.is_json:
        post = request.get_json()
        if len(post) > 1:
            return jsonify(status=405, errorType="Method Not Allowed", errorMessage="Single Key-Value Only", update=False), 405

        for k,v in post.items():
            label=k
            value=v

    else:

        label = request.form.get('label', None)
        value = request.form.get('value', None)

    if label:
        update = update_s3object_tag(s3bucket, s3object, label, value)

    if update is True:
        return jsonify(status=200, message="OK", name=s3object, method="POST"), 200

    return jsonify(status=465, message="Failed POST", name=s3object, label=str(label), method="POST", update=False), 465

#POST    /api         # set s3object tag
# handle form data (form post) and/or json single key/value
@app.route("/api", methods=['POST'])
def set_s3bucketobjectapipost(s3bucket=None,s3object=None):

    if request.is_json:
        post = request.get_json()

        if len(post) > 3:
            return jsonify(status=405, errorType="Method Not Allowed", errorMessage="More than 3 items", update=False), 405

        s3bucket = post.get('s3bucket', None)
        s3object = post.get('s3object', None)

        post.pop('s3bucket', None)
        post.pop('s3object', None)

        if len(post) > 1:
            return jsonify(status=405, errorType="Method Not Allowed", errorMessage="Single Key-Value Only", update=False), 405

        for k,v in post.items():
            label=k
            value=v

    else:

        s3bucket = request.form.get('s3bucket', None)
        s3object = request.form.get('s3object', None)
        label = request.form.get('label', None)
        value = request.form.get('value', None)

    if label:
        update = update_s3object_tag(s3bucket, s3object, label, value)

    if update is True:
        return jsonify(status=200, message="OK", name=s3object, method="POST"), 200

    return jsonify(status=465, message="Failed POST", name=s3object, label=str(label), method="POST", update=False), 465



#DELETE      /s3/<s3bucket>/<s3object>?tag=name      # delete s3object tag 
@app.route("/s3/<s3bucket>/<s3object>", methods=['DELETE'])
def delete_s3bucketobjecttag(s3bucket=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3object == request.view_args['s3object']

    tag = request.args.get("tag", None)

    if tag:

        delete_tag = delete_s3object_tag(s3bucket, s3object, tag)

        if delete_tag is True:

            return jsonify(status=211, message="Deleted", name=s3object, tag=tag, method="DELETE", delete=True), 211

    return jsonify(status=466, message="Failed Delete", name=s3object, tag=tag, method="DELETE", delete=False), 466


#GET    /s3/<s3bucket>/<s3subdir>/<s3object> # List object
@app.route("/s3/<s3bucket>/<s3subdir>/<s3object>", methods=['GET'])
def get_s3bucketsubdirobject(s3bucket=None,s3subdir=None,s3object=None):

    assert s3bucket == request.view_args['s3bucket']
    assert s3subdir == request.view_args['s3subdir']
    assert s3object == request.view_args['s3object']

    s3_client = boto3.client('s3')

    prefix = s3subdir + '/' + s3object

    s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix, Delimiter = "/")

    _exist=False

    try:
        for key in s3_result['Contents']:
            _k = key['Key']
            _exist=True
    except KeyError as e:
        _k = s3_result['Prefix']
        return jsonify(status=404, message="Not Found", s3object=_exist, name=_k), 404

    return jsonify(status=200, message="OK", s3object=_exist, key=_k), 200


#GET    /s3/<s3bucket>/<s3subdir>/                         # List bucket directory files (1000 limit)
@app.route("/s3/<s3bucket>/<s3subdir>/", methods=['GET'])
def get_s3bucketsubdir(s3bucket=None,s3subdir=None):       

    assert s3bucket == request.view_args['s3bucket']
    assert s3subdir == request.view_args['s3subdir']

    prefix = s3subdir + '/'

    s3_client = boto3.client('s3')
    s3_result =  s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix, Delimiter = "/")

    s3objects=[]
    try:
        for key in s3_result['Contents']:
            s3objects.append(key['Key'])
    except KeyError as e:
        return jsonify(status=404, message="Not Found", prefix=prefix), 404

    print(len(s3objects))
    if len(s3objects) < 0:
        return jsonify(status=404, message="Not Found", s3object=False, prefix=prefix), 404

    return jsonify(s3objects), 200
     

#GET    /s3/<s3bucket>/                         # List bucket directory files (1000 limit)
@app.route("/s3/<s3bucket>/", methods=['GET'])
def get_s3bucketdir(s3bucket=None):             

    assert s3bucket == request.view_args['s3bucket']

    s3_client = boto3.client('s3')

    try:
        s3_result =  s3_client.list_objects_v2(Bucket=s3bucket, Delimiter = "/")
    except s3_client.exceptions.NoSuchBucket as e:
        return jsonify(status=404, message="No Such Bucket", s3bucket=s3bucket), 404

    s3objects=[]
    try:
        for key in s3_result['Contents']:
            s3objects.append(key['Key'])
    except KeyError as e:
        return jsonify(status=404, message="Not Found", s3bucket=s3bucket), 404

    if len(s3objects) < 0:
        return jsonify(status=404, message="Not Found", s3objects=0, s3bucket=s3bucket), 404

    return jsonify(s3objects), 200


@app.errorhandler(Exception)
def handle_exception(e):

    if isinstance(e, HTTPException):
        return jsonify(status=e.code, errorType="HTTPException", errorMessage=str(e)), e.code

    return jsonify(status=599, errorType="Exception", errorMessage=str(e)), 599


@app.errorhandler(404)
def not_found(error=None):
    message = { 'status': 404, 'errorType': 'Not Found: ' + request.url }
    return jsonify(message), 404



def get_s3object_body(s3bucket, s3object): #gets file contents data
    s3 = boto3.resource('s3')
    obj = s3.Object(s3bucket, s3object)
    try:
        body = obj.get()['Body'].read()
    except botocore.exceptions.ClientError as ex:
        print('ClientError ' + str(ex))
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



def update_s3object_tag(s3bucket, s3object, tag, value):
    """ single key/value """

    s3_client = boto3.client('s3')

    get_tags_response = s3_client.get_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
    )

    s3Tags = {}
    for key in get_tags_response['TagSet']:
        __k = key['Key']
        __v = key['Value']
        s3Tags[__k]=__v

    s3Tags[tag]=value

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

    status_code = put_tags_response['ResponseMetadata']['HTTPStatusCode']

    if int(status_code) == 200:
        return True
    else:
        return False


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

    s3Tags = {}
    for key in get_tags_response['TagSet']:
        __k = key['Key']
        __v = key['Value']
        s3Tags[__k]=__v

    if tag not in s3Tags:
        return False

    delete = s3Tags.pop(tag, None)
    if delete is None:
        return False

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

    return List


def list_s3buckets():
    s3 = boto3.resource('s3')
    bucket_list = [b.name for b in s3.buckets.all()]
    return bucket_list


def get_s3bucket_objects(s3bucket, s3object):

    s3_client = boto3.client('s3')

    s3_result = {}

    try:
        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")

    except botocore.exceptions.EndpointConnectionError as e:
        s3_result['ResponseMetadata'] = {'HTTPStatusCode': 0, 'BotoError': str(e)}

    return s3_result
    

def main():

    if sys.argv[1:]:

        if sys.argv[1] == "--help":
            sys.exit(print(usage))

        if sys.argv[1] == "--version":
            sys.exit(print(__version__))

        if sys.argv[1] == "buckets" or sys.argv[1] == "list-buckets":

            try:
                buckets = list_s3buckets()
            except Exception as e:
                print(json.dumps({'error':str(e)}))
                sys.exit(1)
            
            sys.exit(print(json.dumps(buckets, indent=2)))

        if sys.argv[1] == "del" or sys.argv[1] == "delete":
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


        if sys.argv[1] == "set" or sys.argv[1] == "label":

            s3path = sys.argv[2]
            s3json = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]

            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            data = json.loads(s3json)

            if len(data) > 1:
                print('data larger than 1')
                sys.exit(1)

           
            for k,v in data.items():
                tag = k
                val = v

            update = update_s3object_tag(s3bucket, s3object, tag, val)

            if update == True:
                print(json.dumps({'label':True}))
                sys.exit(0)
            else:
                print(json.dumps({'label':False}))
                sys.exit(1)
            

        if sys.argv[1] == "ls" or sys.argv[1] == "list":
            s3path = sys.argv[2]
            s3bucket = s3path.split("/", 1)[0]

            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            if s3path.endswith("/"):

                s3_client = boto3.client('s3')

                List=[]

                try:
                    for key in s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object)['Contents']:
                        List.append(key['Key'])
                except KeyError as e:
                    print('KeyError ' + str(e))
                    sys.exit(1)
                except botocore.exceptions.ClientError as e:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                    sys.exit(1)

                print(json.dumps(List, indent=2))
                sys.exit(0)

            try:
                get_s3tags = get_s3object_tags(s3bucket, s3object)

            except botocore.exceptions.EndpointConnectionError as e:
                print(json.dumps({'EndpointConnectionError':str(e)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.ParamValidationError as e:
                print(json.dumps({'ParamValidationError':str(e)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                   print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                   print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)

            s3Tags = {}
            for key in get_s3tags['TagSet']:
                __k = key['Key']
                __v = key['Value']
                s3Tags[__k]=__v

            print(json.dumps(s3Tags, indent=2))
            sys.exit(0)



        if sys.argv[1] == "get":

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            content = get_s3object_body(s3bucket, s3object)

            print(content.rstrip())
            sys.exit(0) 

        if sys.argv[1] == "save":

            s3path      = sys.argv[2]
            destination = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            s3 = boto3.client('s3')

            try:
                s3.download_file(s3bucket, s3object, destination)

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(json.dumps({'Not Found':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)

            except OSError as e:
                print(json.dumps({'OSError':str(e)}, indent=2))
                sys.exit(1)

            print(json.dumps({'saved':destination}, indent=2))
            sys.exit(0)


        if sys.argv[1] == "rekognition":

            s3path = sys.argv[2] #s3bucket/s3object

            try: option = sys.argv[3] #detect-labels
            except IndexError: option = None

            s3bucket = s3path.split("/", 1)[0]
            try: s3object = s3path.split("/", 1)[1]
            except IndexError: s3object = ''

            if option == 's3tag':

                rekognition_json_file = 'rekognition/' + s3object + '.json'
                s3 = boto3.resource('s3')
                obj = s3.Object(s3bucket, rekognition_json_file)
                try:
                    body = obj.get()['Body'].read()

                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    else:
                        print(json.dumps({'ClientError':str(e)}))
                    sys.exit(1)

                content = body.decode("utf-8", "strict").rstrip()

                data = json.loads(content)

                List=[]
                for key in data['Labels']:
                    List.append(key['Name'])

                listToStr = ' '.join([str(elem) for elem in List])

                tag = 'rekognition-words'

                update = update_s3object_tag(s3bucket, s3object, tag, listToStr)

                if update == True:
                    print(json.dumps({'label':True}))
                    sys.exit(0)
                else:
                    print(json.dumps({'label':False}))
                    sys.exit(1)



            if option == 'words':

                rekognition_json_file = 'rekognition/' + s3object + '.json'
                s3 = boto3.resource('s3')
                obj = s3.Object(s3bucket, rekognition_json_file)
                try:
                    body = obj.get()['Body'].read()

                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    else:
                        print(json.dumps({'ClientError':str(e)}))
                    sys.exit(1)

                content = body.decode("utf-8", "strict").rstrip()

                data = json.loads(content)

                List=[]
                for key in data['Labels']:
                    List.append(key['Name'])

                print(json.dumps(List, indent=2))
                sys.exit(0)



            if option == 'detect-labels':

                s3 = boto3.client('s3')
                region = s3.head_bucket(Bucket=s3bucket)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']

                client = boto3.client('rekognition', region_name=region)

                #response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}}, MaxLabels=10)
                response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}})

                print(json.dumps(response, indent=4))

                from tempfile import mkstemp
                fd, path = mkstemp()

                with open(path, 'w') as f:
                    f.write(json.dumps(response, indent=4))

                s3_client = boto3.client('s3')
                try:

                    rekognition_json_file = 'rekognition/' + s3object + '.json'

                    s3_upload = s3_client.upload_file(path, s3bucket, rekognition_json_file)

                except botocore.exceptions.ClientError as e:
                    print(json.dumps({'ClientError':str(e)}, indent=2))
                    sys.exit(1)

                print(json.dumps({'RekognitionUpload':str('Success')}))
                sys.exit(0)

            rekognition_json_file = 'rekognition/' + s3object + '.json'

            s3 = boto3.resource('s3')

            obj = s3.Object(s3bucket, rekognition_json_file)
            try:
                body = obj.get()['Body'].read()

            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    sys.exit(1)
                else:
                    print(json.dumps({'ClientError':str(e)}))
                    sys.exit(1)

            content = body.decode("utf-8", "strict").rstrip()
            print(content)
            sys.exit(0)


        if sys.argv[1] == "object":
            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            s3 = boto3.resource('s3')
            obj = s3.Object(s3bucket, s3object)

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

            Objects = {}
            #Objects['HTTPStatusCode'] = HTTPStatusCode
            Objects['ContentLength'] = ContentLength
            Objects['ContentType'] = ContentType
            Objects['LastModified'] = LastModified

            print(json.dumps(Objects, indent=2, sort_keys=True, default=str))
            sys.exit(0)

        if sys.argv[1] == "identify" or sys.argv[1] == "id":

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            s3 = boto3.resource('s3')
            obj = s3.Object(s3bucket, s3object)

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

            Objects = {}

            Objects['Name'] = s3object

            #Objects['HTTPStatusCode'] = HTTPStatusCode
            Objects['ContentLength'] = ContentLength
            Objects['ContentType'] = ContentType
            Objects['LastModified'] = LastModified

            body = s3response['Body'].read()

            is_64bits = sys.maxsize > 2**32
            if is_64bits:
                blake = blake2b(digest_size=20)
            else:
                blake = blake2s(digest_size=20)

            blake.update(body)

            Objects['b2sum'] = str(blake.hexdigest())

            try:
                data = body.decode('utf-8', 'strict')
                Objects['Encoding'] = 'utf-8'
            except UnicodeDecodeError:
                data = body
                Objects['Encoding'] = 'bytes'

            print(json.dumps(Objects, indent=2, sort_keys=True, default=str))
            sys.exit(0)

        if sys.argv[1] == "b2sum":

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError as e:
                s3object = ''

            s3 = boto3.resource('s3')
            obj = s3.Object(s3bucket, s3object)

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

            except botocore.exceptions.ReadTimeoutError as e:
                print(json.dumps({'ReadTimeoutError':str(e)}, indent=2))
                sys.exit(1)


            HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            ContentLength  = s3response['ContentLength']
            ContentType    = s3response['ContentType']
            LastModified   = s3response['LastModified']

            Objects = {}

            body = s3response['Body'].read()

            is_64bits = sys.maxsize > 2**32
            if is_64bits:
                blake = blake2b(digest_size=20)
            else:
                blake = blake2s(digest_size=20)

            blake.update(body)

            Objects['b2sum'] = str(blake.hexdigest())

            print(json.dumps(Objects, indent=2, sort_keys=True, default=str))
            sys.exit(0)


        if sys.argv[1] == "upload":

            source = sys.argv[2]
            s3path = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            s3object = s3path.split("/", 1)[1]

            s3_client = boto3.client('s3')

            try:
                s3_upload = s3_client.upload_file(source, s3bucket, s3object)
            except botocore.exceptions.ClientError as e:
                print(json.dumps({'ClientError':str(e)}, indent=2))
                sys.exit(1)

            print(json.dumps({'upload':True}, indent=2))
            sys.exit(0)


        if sys.argv[1] == "server":
            try:
                port = int(sys.argv[2])
            except IndexError:
                port = 8880
            app.run(port=port, debug=False)    

        sys.exit(print(usage))
    else:
        sys.exit(print(usage))

if __name__ == "__main__":
    main()


