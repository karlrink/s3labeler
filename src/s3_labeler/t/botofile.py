#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3
import sys

s3 = boto3.resource('s3')

bucket = sys.argv[1]
item   = sys.argv[2]

print(bucket)
print(item)

obj = s3.Object(bucket, item)
body = obj.get()['Body'].read()

print(str(obj))

print(str(type(body)))
print(body)

#🧨 karl.rink@Karl-MacBook-Pro t % ./botofile.py ninfo-property-images rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json 
#ninfo-property-images
#rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json
#<class 'bytes'>
#b'{\n    "Labels": [\n        {\n            "Name": "Tree",\n            "Confidence": 90.44393920898438,\n            "Instances": [],\n            "Parents": [\n                {\n                    "Name": "Plant"\n                }\n            ]\n        },\n        {\n            "Name": "Plant",\n            "Confidence": 90.44393920898438,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Urban",\n            "Confidence": 85.79068756103516,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Building",\n            "Confidence": 74.22905731201172,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Flagstone",\n            "Confidence": 72.1240005493164,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Vegetation",\n            "Confidence": 71.17005920410156,\n            "Instances": [],\n            "Parents": [\n                {\n                    "Name": "Plant"\n                }\n            ]\n        },\n        {\n            "Name": "Garage",\n            "Confidence": 66.02606964111328,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Housing",\n            "Confidence": 65.3967056274414,\n            "Instances": [],\n            "Parents": [\n                {\n                    "Name": "Building"\n                }\n            ]\n        },\n        {\n            "Name": "Tarmac",\n            "Confidence": 64.69490814208984,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Asphalt",\n            "Confidence": 64.69490814208984,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Home Decor",\n            "Confidence": 62.33171081542969,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Walkway",\n            "Confidence": 58.68953323364258,\n            "Instances": [],\n            "Parents": [\n                {\n                    "Name": "Path"\n                }\n            ]\n        },\n        {\n            "Name": "Path",\n            "Confidence": 58.68953323364258,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Suburb",\n            "Confidence": 57.164859771728516,\n            "Instances": [],\n            "Parents": [\n                {\n                    "Name": "Urban"\n                },\n                {\n                    "Name": "Building"\n                }\n            ]\n        },\n        {\n            "Name": "Bush",\n            "Confidence": 56.60989761352539,\n            "Instances": [],\n            "Parents": [\n                {\n                    "Name": "Vegetation"\n                },\n                {\n                    "Name": "Plant"\n                }\n            ]\n        },\n        {\n            "Name": "Slate",\n            "Confidence": 56.16239547729492,\n            "Instances": [],\n            "Parents": []\n        },\n        {\n            "Name": "Concrete",\n            "Confidence": 55.33432388305664,\n            "Instances": [],\n            "Parents": []\n        }\n    ],\n    "LabelModelVersion": "2.0"\n}\n'
#
#
