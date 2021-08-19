
# s3labeler  

s3 - Amazon Simple Storage Service    
label - attached to an object and giving information about it    

## S3 Object Labeling Tool   

[![Package Version](https://img.shields.io/pypi/v/s3labeler.svg)](https://pypi.python.org/pypi/s3labeler/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)


## install
```
pip install s3labeler
```

## cli  
```
s3labeler --help
```

```
Usage: ./s3labeler.py option

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

```

## run as a service
```
s3labeler server
```

## s3 rest api

#### list all buckets HTTP GET
```
curl http://127.0.0.1:8880/s3/ 
```

#### list files in a bucket (1000 record limit) HTTP GET
```
curl http://127.0.0.1:8880/s3/<s3bucket>/
```

#### list files in a bucket subdirectory (1000 record limit) HTTP GET
```
curl http://127.0.0.1:8880/s3/<s3bucket>/rekognition/
```

#### list s3objects s3tags HTTP GET
```
curl http://127.0.0.1:8880/s3/<s3bucket>/<s3object>
```

#### view s3object content, specify content-type header HTTP GET
```
curl http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?image=jpeg
```
image/gif image/jpeg image/png image/tiff image/vnd.microsoft.icon image/x-icon image/vnd.djvu image/svg+xml  


#### list s3object s3tags HTTP GET (same as default get)
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?tags=s3"
```

#### list file rekognition json HTTP GET
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?tags=rekognition"
```

#### set s3object tag HTTP GET
```   
curl -X GET "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?label=tag_name&value=something"
```

#### delete s3object tag HTTP GET
```   
curl -X GET "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?delete=tag_name"
```

#### set or update s3object tag HTTP PATCH (single key/value)
```   
curl -X PATCH \
     -H "Content-Type: application/json" \
     -d '{"labeler":"karl"}' \
     http://127.0.0.1:8880/s3/<s3bucket>/<s3object>    
```   

#### delete s3object tag HTTP DELETE
```   
curl -X DELETE "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?tag=tag_name"
```   


#### set s3object tag set HTTP PUT (Warning: this method overwrites the entire tagset)
```   
curl -X PUT \
     -H "Content-Type: application/json" \
     -d '{"labeler":"karl","image_url":"https://<s3bucket>.s3.us-west-2.amazonaws.com/<s3object>"}' \
     http://127.0.0.1:8880/s3/<s3bucket>/<s3object>    
```   

### HTTP POST
two available endpoints
```
/s3/<s3bucket>/<s3object>  
```
and/or
```
/api
```

http post json or form data to s3object endpoint

#### set or update s3object tag HTTP POST (single key/value) s3object endpoint
```   
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"labeler":"karl"}' \
     http://127.0.0.1:8880/s3/<s3bucket>/<s3object>
```
```
    <form action="http://127.0.0.1:8880/s3/<s3bucket>/<s3object>" method="POST">
      <input type="text" name="label" value="label">
      <input type="text" name="value" value="value">
      <input type="submit">
    </form>
```

http post json or form data to api endpoint

#### set or update s3object tag HTTP POST /api endpoint
```   
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{"s3bucket":"<s3bucket>","s3object":"<s3object>","labeler":"karl"}' \
     http://127.0.0.1:8880/api
```
```
    <form action="http://127.0.0.1:8880/api" method="POST">
        <input type="text" name="s3bucket" value="s3bucket">
        <input type="text" name="s3object" value="s3object">
        <input type="text" name="label" value="label">
        <input type="text" name="value" value="value">
        <input type="submit">
    </form>
```

---

#### get rekognition json HTTP GET (same as ?tags=rekognition)
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=json"
```

#### get rekognition words HTTP GET
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=words"
```
 
#### set rekognition words to s3object tag set
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=words&save=s3tag"
```

### run image through aws rekognition

#### run image through aws rekognition detect-labels HTTP GET  
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=detect-labels"
```

#### run image through aws rekognition detect-labels and save/overwrite with new json HTTP GET
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=detect-labels&save=true"
```

---

## run from source
```
python3 src/s3labeler/s3labeler.py
```

## run as python module
```
cd src/ && python3 -m s3labeler
```

---

image label manager   
run an image through rekognition only once (to prevent recurring costs).  update s3objects tags.   

---

tech notes   
 - aws rekognition detect-labels leverages amazons proprietary ML model for image recognition   
 - image file bytes are not transfered or processed through this interface for rekognition     

https://aws.amazon.com/s3/   
https://aws.amazon.com/rekognition/    





