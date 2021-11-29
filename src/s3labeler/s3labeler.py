#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""s3labeler: app."""

__version__ = '1.0.5-0-20211129-0'

import sys
import json
from hashlib import blake2b, blake2s
import os
from tempfile import mkstemp

import webbrowser

import boto3
import botocore

if sys.version_info < (3, 6, 1):
    raise RuntimeError('Requires Python version 3.6.1 or higher. Running: ' + str(sys.version_info))

usage = "Usage: " + sys.argv[0] + " option" + """

    options:

        list-buckets|buckets

        list|ls    <s3bucket>/<s3object>
        label|set  <s3bucket>/<s3object> '{"label":"value"}'
        delete|del <s3bucket>/<s3object> label

        browser    <s3bucket>/<s3object>
        view       <s3bucket>/<s3object>

        get    <s3bucket>/<s3object>
        save   <s3bucket>/<s3object> destination
        upload source <s3bucket>/<s3object>

        rekognition <s3bucket>/<s3object>
        rekognition <s3bucket>/<s3object> detect-labels

        rekognition <s3bucket>/<s3object> words
        rekognition <s3bucket>/<s3object> s3tag words

        rekognition <s3bucket>/<s3object> confidence
        rekognition <s3bucket>/<s3object> confidence top 3
        rekognition <s3bucket>/<s3object> confidence top 90 percent

        rekognition <s3bucket>/<s3object> s3tag confidence
        rekognition <s3bucket>/<s3object> s3tag confidence top 3
        rekognition <s3bucket>/<s3object> s3tag confidence top 90 percent

        object      <s3bucket>/<s3object>
        b2sum       <s3bucket>/<s3object>
        identify|id <s3bucket>/<s3object>

        server 8880

        --help
        --version
"""

#######################################################################################

def create_server(port, debug=False):
    """create server: flask."""
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
        """GET: / - show version."""
        return jsonify(status=200, message="OK", version=__version__), 200


    #GET    /s3                           # Show OK
    @app.route("/s3", methods=['GET'])
    def get_s3():
        """GET: /s3 - show ok."""
        return jsonify(status=200, message="OK", path="/s3"), 200


    #GET    /s3/                          # List all buckets (limt 1000?)
    @app.route("/s3/", methods=['GET'])   # fun, this method has a "Bucket List"
    def get_s3buckets(region=None):
        """GET: /s3/ - list all buckets, limit 1000."""
        _s3 = boto3.resource('s3', region_name=region)
        bucket_list = [b.name for b in _s3.buckets.all()]
        return jsonify(bucket_list), 200


    #GET    /s3/<s3bucket>/<s3object>     # List object
    #GET    /s3/<s3bucket>/<s3object>?q=  # rekognition=json|words|detect-labels
    #GET    /s3/<s3bucket>/<s3object>?q=  # tags=s3|rekognition
    #GET    /s3/<s3bucket>/<s3object>?q=  # delete=name
    #GET    /s3/<s3bucket>/<s3object>?q=  # label=name&value=something

    #GET    /s3/<s3bucket>/<s3object>?q=
    @app.route("/s3/<s3bucket>/<s3object>", methods=['GET'])
    def get_s3bucketobject(s3bucket=None,s3object=None):
        """GET: /s3/<s3bucket>/<s3object> - List and query."""
        assert s3bucket == request.view_args['s3bucket']
        assert s3object == request.view_args['s3object']

        rekognition = request.args.get("rekognition", None)
        tags = request.args.get("tags", None)
        save = request.args.get("save", None)
        image = request.args.get("image", None)
        delete = request.args.get("delete", None)
        label = request.args.get("label", None)
        value = request.args.get("value", None)
        top = request.args.get("top", None)
        percent = request.args.get("percent", None)

        s3_client = boto3.client('s3')

        if not request.args:

            try:
                get_s3tags = get_s3object_tags(s3bucket, s3object)

            #except botocore.exceptions.EndpointConnectionError as e:
            except botocore.exceptions.EndpointConnectionError:
                return jsonify(status=599, message="EndpointConnectionError", s3object=False, name=s3object), 599

            #except botocore.exceptions.ParamValidationError as e:
            except botocore.exceptions.ParamValidationError:
                return jsonify(status=599, message="ParamValidationError", s3object=False, name=s3object), 599

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    return jsonify(status=404, message="NoSuchKey", s3object=False, name=s3object), 404
                return jsonify(status=599, message="ClientError", s3object=False, name=s3object, error=str(error)), 599

            s3tags = {}
            for key in get_s3tags['TagSet']:
                __k = key['Key']
                __v = key['Value']
                s3tags[__k] = __v

            return jsonify(s3tags), 200

        try:
            s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")
        except botocore.exceptions.EndpointConnectionError as error:
            return jsonify(status=599, message="EndpointConnectionError", error=str(error)), 599

        try:
            for key in s3_result['Contents']:
                #print(key['Key'])
                _k = key['Key']
        #except KeyError as e:
        except KeyError:
            _k = s3_result['Prefix']
            return jsonify(status=404, message="Not Found", s3object=False, name=_k), 404

        if _k != s3object:
            return jsonify(status=569, message="Objects Do Not Match", object1=str(_k), ojbect2=str(s3object)), 569

        if image:
            #get s3object and send to browser

            _s3 = boto3.resource('s3')
            obj = _s3.Object(s3bucket, s3object)
            try:
                body = obj.get()['Body'].read()
            except botocore.exceptions.ClientError as error:
                return jsonify(status=599, message="ClientError", error=str(error)), 599

            #return body                #<class 'bytes'>
            #return body.decode('utf-8') #<class 'str'>

            #return body, 200, {'Content-Type':'image/jpeg'}
            return body, 200, {'Content-Type':'image/' + image}

        if tags:

            if tags == 's3':

                get_s3tags = get_s3object_tags(s3bucket, s3object)

                s3tags = {}
                for key in get_s3tags['TagSet']:
                    __k = key['Key']
                    __v = key['Value']
                    s3tags[__k] = __v

                return jsonify(s3tags), 200


            if tags == 'rekognition':

                rekognition_json_file = 'rekognition/' + s3object + '.json'

                rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

                if rekognition_json_content:
                    return rekognition_json_content, 200
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404

        if rekognition:

            ########################################################################################

            if rekognition == 'detect-labels':

                #either specify region or auto get region from boto call
                _s3 = boto3.client('s3')
                region = _s3.head_bucket(Bucket=s3bucket)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']

                client = boto3.client('rekognition', region_name=region)

                #response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}}, MaxLabels=10)
                response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}})

                if save:

                    #from tempfile import mkstemp
                    _fd, path = mkstemp()

                    with open(path, 'w', encoding="utf8") as _f:
                        _f.write(json.dumps(response, indent=4))

                    s3_client = boto3.client('s3')

                    try:
                        rekognition_json_file = 'rekognition/' + s3object + '.json'
                        #s3_upload = s3_client.upload_file(path, s3bucket, rekognition_json_file)
                        s3_client.upload_file(path, s3bucket, rekognition_json_file)

                    except botocore.exceptions.ClientError as error:
                        return jsonify(status=599, message="ClientError", error=str(error)), 599

                    return jsonify(response), 201
                return jsonify(response), 200

            ########################################################################################

            if rekognition == 'json':
                rekognition_json_file = 'rekognition/' + s3object + '.json'
                rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)
                if rekognition_json_content:
                    return rekognition_json_content, 200
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404

            ########################################################################################

            if rekognition == 'words':

                rekognition_json_file = 'rekognition/' + s3object + '.json'
                rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

                if rekognition_json_content:

                    wordlist = extract_rekognition_words(rekognition_json_content)

                    if save == 's3tag':

                        _s3 = boto3.resource('s3')
                        obj = _s3.Object(s3bucket, rekognition_json_file)
                        try:
                            body = obj.get()['Body'].read()

                        except botocore.exceptions.ClientError as error:
                            if error.response['Error']['Code'] == 'NoSuchKey':
                                return jsonify(status=404, message="Not Found", s3object=False, key=rekognition_json_file), 404
                            return jsonify(status=599, message="ClientError", s3object=False, key=rekognition_json_file, error=str(error)), 599

                        content = body.decode("utf-8", "strict").rstrip()

                        data = json.loads(content)

                        _list=[]
                        for key in data['Labels']:
                            _list.append(key['Name'])

                        listtostr = ' '.join([str(elem) for elem in _list])

                        tag = 'rekognition-words'

                        update = update_s3object_tag(s3bucket, s3object, tag, listtostr)

                        if update is True:
                            return jsonify(status=201, message="Created S3Tag", label=True), 201
                        return jsonify(status=465, message="Failed S3Tag", label=False), 465
                    return jsonify(wordlist), 200
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404

            ########################################################################################

            if rekognition == 'confidence':

                rekognition_json_file = 'rekognition/' + s3object + '.json'
                rekognition_json_content = get_s3object_body(s3bucket, rekognition_json_file)

                if rekognition_json_content:

                    #wordList = extract_rekognition_words(rekognition_json_content)

                    _s3 = boto3.resource('s3')
                    obj = _s3.Object(s3bucket, rekognition_json_file)

                    try:
                        body = obj.get()['Body'].read()
                    except botocore.exceptions.ClientError as error:
                        if error.response['Error']['Code'] == 'NoSuchKey':
                            return jsonify(status=404, message="Not Found", s3object=False, key=rekognition_json_file), 404
                        return jsonify(status=599, message="ClientError", s3object=False, key=rekognition_json_file, error=str(error)), 599

                    content = body.decode("utf-8", "strict").rstrip()
                    data = json.loads(content)

                    _dict={}

                    ################################################################################
                    if top:

                        if percent:

                            for key in data['Labels']:
                                _name = key['Name']
                                _confidence = key['Confidence']
                                if int(top) <= int(_confidence):
                                    _dict[_name]= str(_confidence)
                        else:
                            count=0
                            for key in data['Labels']:
                                count += 1
                                if count <= int(top):
                                    _name = key['Name']
                                    _confidence = key['Confidence']
                                    _dict[_name]= str(_confidence)

                    else:
                        for key in data['Labels']:
                            _name = key['Name']
                            _confidence = key['Confidence']
                            _dict[_name]= str(_confidence)


                    ################################################################################
                    if save == 's3tag':

                        updated=0
                        for _k,_v in _dict.items():
                            try:
                                update = update_s3object_tag(s3bucket, s3object, _k, _v)
                                updated += 1
                            except botocore.exceptions.ClientError as error:
                                return jsonify(status=465, message="Failed S3Tag", label=False, error=str(error)), 465

                        if updated > 0:
                            return jsonify(status=201, message="Created S3Tag", label=True), 201
                        return jsonify(status=465, message="Failed S3Tag", label=False), 465

                    return jsonify(_dict), 200
                return jsonify(status=404, message="Not Found", s3object=False, rekognition_json_location=rekognition_json_file), 404
            ########################################################################################

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
        """PUT: /s3/<s3bucket>/<s3object> - set s3object key/value."""
        assert s3bucket == request.view_args['s3bucket']
        assert s3object == request.view_args['s3object']

        if not request.headers['Content-Type'] == 'application/json':
            return jsonify(status=412, errorType="Precondition Failed"), 412

        post = request.get_json()
        #settagset = set_s3object_tags(s3bucket, s3object, post)
        set_s3object_tags(s3bucket, s3object, post)

        return jsonify(status=200, message="OK", name=s3object, method="PUT"), 200


    #PATCH    /s3/<s3bucket>/<s3object>         # set s3object tag
    @app.route("/s3/<s3bucket>/<s3object>", methods=['PATCH'])
    def set_s3bucketobjectpatch(s3bucket=None,s3object=None):
        """PATCH: /s3/<s3bucket>/<s3object> - set s3object tag."""
        assert s3bucket == request.view_args['s3bucket']
        assert s3object == request.view_args['s3object']

        if not request.headers['Content-Type'] == 'application/json':
            return jsonify(status=412, errorType="Precondition Failed"), 412

        post = request.get_json()

        if len(post) > 1:
            return jsonify(status=405, errorType="Method Not Allowed", errorMessage="Single Key-Value Only", update=False), 405

        #print(post)
        for _k,_v in post.items():
            tag = _k
            value = _v

        update = update_s3object_tag(s3bucket, s3object, tag, value)

        #print(update)

        if update is True:
            return jsonify(status=200, message="OK", name=s3object, method="PATCH"), 200

        return jsonify(status=465, message="Failed Patch", name=s3object, tag=tag, method="PATCH", update=False), 465


    #POST    /s3/<s3bucket>/<s3object>         # set s3object tag
    # handle form data (form post) and/or json single key/value
    @app.route("/s3/<s3bucket>/<s3object>", methods=['POST'])
    def set_s3bucketobjectpost(s3bucket=None,s3object=None):
        """POST: /s3/<s3bucket>/<s3object> - set s3object."""
        assert s3bucket == request.view_args['s3bucket']
        assert s3object == request.view_args['s3object']

        if request.is_json:
            post = request.get_json()
            if len(post) > 1:
                return jsonify(status=405, errorType="Method Not Allowed", errorMessage="Single Key-Value Only", update=False), 405

            for _k,_v in post.items():
                label = _k
                value = _v

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
        """POST: /api - set s3object tag."""
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

            for _k,_v in post.items():
                label = _k
                value = _v

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
        """DELETE: /s3/<s3bucket>/<s3object> - delete s3object tag."""
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
        """GET: /s3/<s3bucket>/<s3subdir>/<s3object> - List objects."""
        assert s3bucket == request.view_args['s3bucket']
        assert s3subdir == request.view_args['s3subdir']
        assert s3object == request.view_args['s3object']

        s3_client = boto3.client('s3')

        prefix = s3subdir + '/' + s3object

        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix, Delimiter = "/")

        _exist = False

        try:
            for key in s3_result['Contents']:
                _k = key['Key']
                _exist = True
        #except KeyError as e:
        except KeyError:
            _k = s3_result['Prefix']
            return jsonify(status=404, message="Not Found", s3object=_exist, name=_k), 404

        return jsonify(status=200, message="OK", s3object=_exist, key=_k), 200


    #GET    /s3/<s3bucket>/<s3subdir>/                         # List bucket directory files (1000 limit)
    @app.route("/s3/<s3bucket>/<s3subdir>/", methods=['GET'])
    def get_s3bucketsubdir(s3bucket=None,s3subdir=None):
        """GET /s3/<s3bucket>/<s3subdir>/ - List dir files."""
        assert s3bucket == request.view_args['s3bucket']
        assert s3subdir == request.view_args['s3subdir']

        prefix = s3subdir + '/'

        s3_client = boto3.client('s3')
        s3_result =  s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix, Delimiter = "/")

        s3objects=[]
        try:
            for key in s3_result['Contents']:
                s3objects.append(key['Key'])
        #except KeyError as e:
        except KeyError:
            return jsonify(status=404, message="Not Found", prefix=prefix), 404

        print(len(s3objects))
        if len(s3objects) < 0:
            return jsonify(status=404, message="Not Found", s3object=False, prefix=prefix), 404

        return jsonify(s3objects), 200


    #GET    /s3/<s3bucket>/                         # List bucket directory files (1000 limit)
    @app.route("/s3/<s3bucket>/", methods=['GET'])
    def get_s3bucketdir(s3bucket=None):
        """GET: /s3/<s3bucket>/ - List bucket dir files."""
        assert s3bucket == request.view_args['s3bucket']

        s3_client = boto3.client('s3')

        try:
            s3_result =  s3_client.list_objects_v2(Bucket=s3bucket, Delimiter = "/")
        #except s3_client.exceptions.NoSuchBucket as e:
        except s3_client.exceptions.NoSuchBucket:
            return jsonify(status=404, message="No Such Bucket", s3bucket=s3bucket), 404

        s3objects=[]
        try:
            for key in s3_result['Contents']:
                s3objects.append(key['Key'])
        #except KeyError as e:
        except KeyError:
            return jsonify(status=404, message="Not Found", s3bucket=s3bucket), 404

        if len(s3objects) < 0:
            return jsonify(status=404, message="Not Found", s3objects=0, s3bucket=s3bucket), 404

        return jsonify(s3objects), 200


    @app.errorhandler(Exception)
    def handle_exception(error):
        """Exception: HTTP Exception."""
        if isinstance(error, HTTPException):
            return jsonify(status=error.code, errorType="HTTPException", errorMessage=str(error)), error.code

        return jsonify(status=599, errorType="Exception", errorMessage=str(error)), 599


    @app.errorhandler(404)
    def not_found():
        """Not_Found: HTTP File Not Found 404."""
        message = { 'status': 404, 'errorType': 'Not Found: ' + request.url }
        return jsonify(message), 404

    return app

#######################################################################################

def get_s3object_body(s3bucket, s3object): #gets file contents data
    """S3: get file contents data."""
    _s3 = boto3.resource('s3')
    obj = _s3.Object(s3bucket, s3object)
    try:
        body = obj.get()['Body'].read()
    except botocore.exceptions.ClientError as ex:
        print('ClientError ' + str(ex))
        return None

    #return body                #<class 'bytes'>
    return body.decode('utf-8') #<class 'str'>

def get_s3object_tags(s3bucket, s3object):
    """S3: get object tags."""
    s3_client = boto3.client('s3')
    s3_result = s3_client.get_object_tagging(Bucket=s3bucket, Key=s3object)
    return s3_result

def set_s3object_tags(s3bucket, s3object, post):
    """S3: multi key/value."""
    s3_client = boto3.client('s3')

    keyvallist = []

    for _k,_v in post.items():
        kvs = {}
        kvs['Key'] = _k
        kvs['Value'] = _v

        keyvallist.append(kvs)

    tagset = { 'TagSet': keyvallist }

    s3_result = s3_client.put_object_tagging(
            Bucket=s3bucket,
            Key=s3object,
            Tagging=tagset
            )
    status_code = s3_result['ResponseMetadata']['HTTPStatusCode']

    if int(status_code) == 200:
        return True
    return False


def update_s3object_tag(s3bucket, s3object, tag, value):
    """S3: single key/value."""
    s3_client = boto3.client('s3')

    get_tags_response = s3_client.get_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
    )

    s3tags = {}
    for key in get_tags_response['TagSet']:
        __k = key['Key']
        __v = key['Value']
        s3tags[__k] = __v

    s3tags[tag]=value

    keyvallist = []
    for _k,_v in s3tags.items():
        kvs = {}
        kvs['Key'] = _k
        kvs['Value'] = _v
        keyvallist.append(kvs)

    tagset = { 'TagSet': keyvallist }

    put_tags_response = s3_client.put_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
        Tagging=tagset
    )

    status_code = put_tags_response['ResponseMetadata']['HTTPStatusCode']

    if int(status_code) == 200:
        return True
    return False


def delete_s3object_tag(s3bucket, s3object, tag):
    """S3: delete tag."""
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

    s3tags = {}
    for key in get_tags_response['TagSet']:
        __k = key['Key']
        __v = key['Value']
        s3tags[__k]=__v

    if tag not in s3tags:
        return False

    delete = s3tags.pop(tag, None)
    if delete is None:
        return False

    keyvallist = []
    for _k,_v in s3tags.items():
        kvs = {}
        kvs['Key'] = _k
        kvs['Value'] = _v
        keyvallist.append(kvs)

    tagset = { 'TagSet': keyvallist }

    put_tags_response = s3_client.put_object_tagging(
        Bucket=s3bucket,
        Key=s3object,
        Tagging=tagset
    )

    put_tags_response_code = put_tags_response['ResponseMetadata']['HTTPStatusCode']

    if int(put_tags_response_code) == 200:
        return True
    return False


def extract_rekognition_words(rekognition_json_content):
    """Rekognition: extract words."""
    data = json.loads(rekognition_json_content)

    _list = []

    for key in data['Labels']:
        _list.append(str(key['Name']))

    return _list


def list_s3buckets():
    """S3: list buckets."""
    _s3 = boto3.resource('s3')
    bucket_list = [b.name for b in _s3.buckets.all()]
    return bucket_list


def get_s3bucket_objects(s3bucket, s3object):
    """S3: get bucket objects."""
    s3_client = boto3.client('s3')

    s3_result = {}

    try:
        s3_result = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object, Delimiter = "/")

    except botocore.exceptions.EndpointConnectionError as error:
        s3_result['ResponseMetadata'] = {'HTTPStatusCode': 0, 'BotoError': str(error)}
    return s3_result


def main():
    """main: app."""
    if sys.argv[1:]:
        #-------------------------------------------------------------------------------------------
        if sys.argv[1] == "--help":
            sys.exit(print(usage))

        #-------------------------------------------------------------------------------------------
        if sys.argv[1] == "--version":
            sys.exit(print(__version__))

        #-------------------------------------------------------------------------------------------
        if sys.argv[1] == "buckets" or sys.argv[1] == "list-buckets":
            try:
                buckets = list_s3buckets()
            except Exception as error:
                print(json.dumps({'error':str(error)}))
                sys.exit(1)
            sys.exit(print(json.dumps(buckets, indent=2)))

        #-------------------------------------------------------------------------------------------
        if sys.argv[1] == "del" or sys.argv[1] == "delete":
            s3path = sys.argv[2]
            tag = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            #except IndexError as e:
            except IndexError:
                s3object = ''

            delete = delete_s3object_tag(s3bucket, s3object, tag)

            if delete is True:
                print(json.dumps({'delete':True}))
                sys.exit(0)
            else:
                print(json.dumps({'delete':False}))
                sys.exit(1)

        #-------------------------------------------------------------------------------------------
        if sys.argv[1] == "set" or sys.argv[1] == "label":

            s3path = sys.argv[2]
            s3json = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]

            try:
                s3object = s3path.split("/", 1)[1]
            #except IndexError as e:
            except IndexError:
                s3object = ''

            data = json.loads(s3json)

            if len(data) > 1:
                print('data larger than 1')
                sys.exit(1)

            for _k,_v in data.items():
                tag = _k
                val = _v

            update = update_s3object_tag(s3bucket, s3object, tag, val)

            if update is True:
                print(json.dumps({'label':True}))
                sys.exit(0)
            else:
                print(json.dumps({'label':False}))
                sys.exit(1)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "ls" or sys.argv[1] == "list":
            s3path = sys.argv[2]
            s3bucket = s3path.split("/", 1)[0]

            try:
                s3object = s3path.split("/", 1)[1]
            #except IndexError as e:
            except IndexError:
                s3object = ''

            if s3path.endswith("/"):

                s3_client = boto3.client('s3')

                _list=[]

                try:
                    for key in s3_client.list_objects_v2(Bucket=s3bucket, Prefix=s3object)['Contents']:
                        _list.append(key['Key'])
                except KeyError as error:
                    print('KeyError ' + str(error))
                    sys.exit(1)
                except botocore.exceptions.ClientError as error:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                    sys.exit(1)

                print(json.dumps(_list, indent=2))
                sys.exit(0)

            try:
                get_s3tags = get_s3object_tags(s3bucket, s3object)

            except botocore.exceptions.EndpointConnectionError as error:
                print(json.dumps({'EndpointConnectionError':str(error)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.ParamValidationError as error:
                print(json.dumps({'ParamValidationError':str(error)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            s3tags = {}
            for key in get_s3tags['TagSet']:
                __k = key['Key']
                __v = key['Value']
                s3tags[__k]=__v

            print(json.dumps(s3tags, indent=2))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "get":
            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError:
                s3object = ''

            content = get_s3object_body(s3bucket, s3object)

            print(content.rstrip())
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "save":
            s3path      = sys.argv[2]
            destination = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError:
                s3object = ''

            _s3 = boto3.client('s3')

            try:
                _s3.download_file(s3bucket, s3object, destination)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == '404':
                    print(json.dumps({'Not Found':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            except OSError as error:
                print(json.dumps({'OSError':str(error)}, indent=2))
                sys.exit(1)

            print(json.dumps({'saved':destination}, indent=2))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "rekognition":
            s3path = sys.argv[2] #s3bucket/s3object

            try:
                option = sys.argv[3] #detect-labels
            except IndexError:
                option = None

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            except IndexError:
                s3object = ''

            ##--------------------------------------------------------------------------------------------
            if option == 's3tag':

                rekognition_json_file = 'rekognition/' + s3object + '.json'
                _s3 = boto3.resource('s3')
                obj = _s3.Object(s3bucket, rekognition_json_file)
                try:
                    body = obj.get()['Body'].read()

                except botocore.exceptions.ClientError as error:
                    if error.response['Error']['Code'] == 'NoSuchKey':
                        print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    else:
                        print(json.dumps({'ClientError':str(error)}))
                    sys.exit(1)

                content = body.decode("utf-8", "strict").rstrip()

                data = json.loads(content)

                try:
                    option4 = sys.argv[4]
                except IndexError:
                    option4 = None

                if option4:
                    # words||confidence

                    if option4 == 'words':
                        #print('words')
                        _list=[]
                        for key in data['Labels']:
                            _list.append(key['Name'])
                        listtostr = ' '.join([str(elem) for elem in _list])
                        tag = 'rekognition-words'
                        update = update_s3object_tag(s3bucket, s3object, tag, listtostr)
                        #if update == True:
                        if update is True:
                            print(json.dumps({'label':True}))
                            sys.exit(0)
                        else:
                            print(json.dumps({'label':False}))
                            sys.exit(1)


                    if option4 == 'confidence':

                        try:
                            option5 = sys.argv[5]
                        except IndexError:
                            option5 = None

                        try:
                            option6 = sys.argv[6]
                        except IndexError:
                            option6 = None

                        try:
                            option7 = sys.argv[7]
                        except IndexError:
                            option7 = None

                        #print(str(option5)) #top
                        #print(str(option6)) #3 number
                        #print(str(option7)) #percent

                        _dict={}

                        #if option5 == 'top' and option7 == None:
                        if option5 == 'top' and option7 is None:

                            count=0

                            for key in data['Labels']:
                                count += 1
                                if count <= int(option6):
                                    _name = key['Name']
                                    _confidence = key['Confidence']
                                    _dict[_name]= str(_confidence)


                        elif option5 == 'top' and option7 == 'percent':

                            for key in data['Labels']:
                                _name = key['Name']
                                _confidence = key['Confidence']

                                if int(option6) <= int(_confidence):
                                    _dict[_name]= str(_confidence)
                        else:
                            for key in data['Labels']:
                                _name = key['Name']
                                _confidence = key['Confidence']
                                _dict[_name]= str(_confidence)

                        ###--------------------------------------------------------
                        updated=0
                        for _k,_v in _dict.items():
                            try:
                                update = update_s3object_tag(s3bucket, s3object, _k, _v)
                                updated += 1
                            except botocore.exceptions.ClientError as error:
                                print(json.dumps({'label':False, 'error': str(error), 's3tag': str(_k)}))
                                sys.exit(1)

                        if updated > 0:
                            print(json.dumps({'label':True}))
                            sys.exit(0)
                        else:
                            print(json.dumps({'label':False}))
                            sys.exit(1)

                        sys.exit(1)

                    else:
                        print('Unknown option ' + option4)
                        sys.exit(1)

                else:
                    print('options are words or confidence')
                    sys.exit(1)


                sys.exit(0)

            ##--------------------------------------------------------------------------------------------
            if option == 'words':
                rekognition_json_file = 'rekognition/' + s3object + '.json'
                _s3 = boto3.resource('s3')
                obj = _s3.Object(s3bucket, rekognition_json_file)
                try:
                    body = obj.get()['Body'].read()

                except botocore.exceptions.ClientError as error:
                    if error.response['Error']['Code'] == 'NoSuchKey':
                        print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    else:
                        print(json.dumps({'ClientError':str(error)}))
                    sys.exit(1)

                content = body.decode("utf-8", "strict").rstrip()

                data = json.loads(content)

                _list=[]
                for key in data['Labels']:
                    _list.append(key['Name'])

                print(json.dumps(_list, indent=2))
                sys.exit(0)

            ##--------------------------------------------------------------------------------------------
            if option == 'confidence':

                try:
                    option4 = sys.argv[4]
                except IndexError:
                    option4 = None

                try:
                    option5 = sys.argv[5]
                except IndexError:
                    option5 = None

                try:
                    option6 = sys.argv[6]
                except IndexError:
                    option6 = None

                #print(str(option4)) #top
                #print(str(option5)) #3 number
                #print(str(option6)) #percent

                rekognition_json_file = 'rekognition/' + s3object + '.json'
                _s3 = boto3.resource('s3')
                obj = _s3.Object(s3bucket, rekognition_json_file)
                try:
                    body = obj.get()['Body'].read()

                except botocore.exceptions.ClientError as error:
                    if error.response['Error']['Code'] == 'NoSuchKey':
                        print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    else:
                        print(json.dumps({'ClientError':str(error)}))
                    sys.exit(1)

                content = body.decode("utf-8", "strict").rstrip()

                data = json.loads(content)

                _dict={}

                #if option4 == 'top' and option6 == None:
                if option4 == 'top' and option6 is None:
                    count=0
                    for key in data['Labels']:
                        count += 1
                        if count <= int(option5):
                            _name = key['Name']
                            _confidence = key['Confidence']
                            _dict[_name]= str(_confidence)


                elif option4 == 'top' and option6 == 'percent':
                    for key in data['Labels']:
                        _name = key['Name']
                        _confidence = key['Confidence']

                        if int(option5) <= int(_confidence):
                            _dict[_name]= str(_confidence)

                else:

                    for key in data['Labels']:
                        _name = key['Name']
                        _confidence = key['Confidence']
                        _dict[_name]=_confidence

                print(json.dumps(_dict, indent=2))
                sys.exit(0)

            ##--------------------------------------------------------------------------------------------
            if option == 'detect-labels':

                _s3 = boto3.client('s3')
                region = _s3.head_bucket(Bucket=s3bucket)['ResponseMetadata']['HTTPHeaders']['x-amz-bucket-region']

                client = boto3.client('rekognition', region_name=region)

                #response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}}, MaxLabels=10)
                response = client.detect_labels(Image={'S3Object':{'Bucket':s3bucket,'Name':s3object}})

                print(json.dumps(response, indent=4))

                #from tempfile import mkstemp
                _fd, path = mkstemp()

                with open(path, 'w', encoding="utf8") as _f:
                    _f.write(json.dumps(response, indent=4))

                s3_client = boto3.client('s3')
                try:

                    rekognition_json_file = 'rekognition/' + s3object + '.json'
                    #s3_upload = s3_client.upload_file(path, s3bucket, rekognition_json_file)
                    s3_client.upload_file(path, s3bucket, rekognition_json_file)

                except botocore.exceptions.ClientError as error:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                    sys.exit(1)

                print(json.dumps({'RekognitionUpload':str('Success')}))
                sys.exit(0)

            rekognition_json_file = 'rekognition/' + s3object + '.json'

            _s3 = boto3.resource('s3')
            obj = _s3.Object(s3bucket, rekognition_json_file)
            try:
                body = obj.get()['Body'].read()

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':rekognition_json_file}, indent=2))
                    sys.exit(1)
                else:
                    print(json.dumps({'ClientError':str(error)}))
                    sys.exit(1)

            content = body.decode("utf-8", "strict").rstrip()
            print(content)
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "object":
            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            #except IndexError as e:
            except IndexError:
                s3object = ''

            _s3 = boto3.resource('s3')
            obj = _s3.Object(s3bucket, s3object)

            try:
                s3response = obj.get()

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            #HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            contentlength  = s3response['ContentLength']
            contenttype    = s3response['ContentType']
            lastmodified   = s3response['LastModified']

            objects = {}
            #Objects['HTTPStatusCode'] = HTTPStatusCode
            objects['ContentLength'] = contentlength
            objects['ContentType'] = contenttype
            objects['LastModified'] = lastmodified

            print(json.dumps(objects, indent=2, sort_keys=True, default=str))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "identify" or sys.argv[1] == "id":
            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            #except IndexError as e:
            except IndexError:
                s3object = ''

            _s3 = boto3.resource('s3')
            obj = _s3.Object(s3bucket, s3object)

            try:
                s3response = obj.get()

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.EndpointConnectionError as error:
                print(json.dumps({'EndpointConnectionError':str(error)}, indent=2))
                sys.exit(1)


            #HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            contentlength  = s3response['ContentLength']
            contenttype    = s3response['ContentType']
            lastmodified   = s3response['LastModified']

            objects = {}

            objects['Name'] = s3object

            #Objects['HTTPStatusCode'] = HTTPStatusCode
            objects['ContentLength'] = contentlength
            objects['ContentType'] = contenttype
            objects['LastModified'] = lastmodified

            body = s3response['Body'].read()

            is_64bits = sys.maxsize > 2**32
            if is_64bits:
                blake = blake2b(digest_size=20)
            else:
                blake = blake2s(digest_size=20)

            blake.update(body)

            objects['b2sum'] = str(blake.hexdigest())

            try:
                data = body.decode('utf-8', 'strict')
                objects['Encoding'] = 'utf-8'
            except UnicodeDecodeError:
                data = body
                objects['Encoding'] = 'bytes'

            print(json.dumps(objects, indent=2, sort_keys=True, default=str))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "b2sum":

            s3path = sys.argv[2]

            s3bucket = s3path.split("/", 1)[0]
            try:
                s3object = s3path.split("/", 1)[1]
            #except IndexError as e:
            except IndexError:
                s3object = ''

            _s3 = boto3.resource('s3')
            obj = _s3.Object(s3bucket, s3object)

            try:
                s3response = obj.get()

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'NoSuchKey':
                    print(json.dumps({'NoSuchKey':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.EndpointConnectionError as error:
                print(json.dumps({'EndpointConnectionError':str(error)}, indent=2))
                sys.exit(1)

            except botocore.exceptions.ReadTimeoutError as error:
                print(json.dumps({'ReadTimeoutError':str(error)}, indent=2))
                sys.exit(1)

            #HTTPStatusCode = s3response['ResponseMetadata']['HTTPStatusCode']
            #ContentLength  = s3response['ResponseMetadata']['ContentLength']
            #contentlength  = s3response['ContentLength']
            #contenttype    = s3response['ContentType']
            #lastmodified   = s3response['LastModified']

            objects = {}

            body = s3response['Body'].read()

            is_64bits = sys.maxsize > 2**32
            if is_64bits:
                blake = blake2b(digest_size=20)
            else:
                blake = blake2s(digest_size=20)

            blake.update(body)

            objects['b2sum'] = str(blake.hexdigest())

            print(json.dumps(objects, indent=2, sort_keys=True, default=str))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "upload":
            source = sys.argv[2]
            s3path = sys.argv[3]

            s3bucket = s3path.split("/", 1)[0]
            s3object = s3path.split("/", 1)[1]

            s3_client = boto3.client('s3')

            try:
                #s3_upload = s3_client.upload_file(source, s3bucket, s3object)
                s3_client.upload_file(source, s3bucket, s3object)
            except botocore.exceptions.ClientError as error:
                print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            print(json.dumps({'upload':True}, indent=2))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "browser":
            s3path = sys.argv[2]
            s3bucket = s3path.split("/", 1)[0]
            s3object = s3path.split("/", 1)[1]

            _s3 = boto3.client('s3')

            #from tempfile import mkstemp

            suffix = str(s3object).replace('/', '_')

            _fd, path = mkstemp(suffix=suffix)

            try:
                _s3.download_file(s3bucket, s3object, path)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == '404':
                    print(json.dumps({'Not Found':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            #import webbrowser
            webbrowser.open('file://' + str(path))

            print(json.dumps({'tempfile': path}))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "view":
            s3path = sys.argv[2]
            s3bucket = s3path.split("/", 1)[0]
            s3object = s3path.split("/", 1)[1]

            _s3 = boto3.client('s3')

            suffix = str(s3object).replace('/', '_')

            #from tempfile import mkstemp
            _fd, path = mkstemp(suffix=suffix)

            try:
                _s3.download_file(s3bucket, s3object, path)

            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == '404':
                    print(json.dumps({'Not Found':s3object}, indent=2))
                else:
                    print(json.dumps({'ClientError':str(error)}, indent=2))
                sys.exit(1)

            #import os
            if sys.platform == 'darwin':
                os.system("open " + str(path))
                print(json.dumps({'tempfile': path}))
                sys.exit(0)

            #elif sys.platform == 'linux' or sys.platform == 'linux2':
            elif sys.platform in ('linux', 'linux2'):
                os.system("xdg-open tmp.png")
                print(json.dumps({'tempfile': path}))
                sys.exit(0)

            #elif sys.platform == 'win32' or sys.platform == 'win64':
            elif sys.platform in ('win32', 'win64'):
                os.system("powershell -c tmp.png")
                print(json.dumps({'tempfile': path}))
                sys.exit(0)

            elif sys.platform == 'cygwin':
                os.system("")
                print(json.dumps({'tempfile': path, 'cygwin':'unknown command line'}))
                sys.exit(1)

            else:
                print(json.dumps({'tempfile': path, 'unknown':str(sys.platform)}))
                sys.exit(1)

            #mac
            #os.system("open tmp.png")

            #linux
            #os.system("xdg-open tmp.png")

            #win
            #os.system("powershell -c tmp.png")

            print(json.dumps({'tempfile': path}))
            sys.exit(0)

        #---------------------------------------------------------------------------------------------
        if sys.argv[1] == "server":
            try:
                port = int(sys.argv[2])
            except IndexError:
                port = 8880
            #https://flask.palletsprojects.com/en/1.0.x/patterns/appfactories/
            app = create_server(port=port, debug=False)
            #run = app.run(port=port, debug=False)
            app.run(port=port, debug=False)

        sys.exit(print(usage))
    sys.exit(print(usage))


if __name__ == "__main__":
    main()
