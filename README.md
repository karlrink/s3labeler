
# s3labeler  

## S3 Object Labeling Tool  

## cli  
```
s3labeler --help
```

```
Usage: ./s3labeler.py option

    options:

        list-buckets|buckets

        ls        <s3bucket>/<s3object>
        label|set <s3bucket>/<s3object> '{"label":"value"}'
        del       <s3bucket>/<s3object> label

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

#### list file HTTP GET
```
curl http://127.0.0.1:8880/s3/<s3bucket>/<s3object>
```

#### list file HTTP GET
```
curl http://127.0.0.1:8880/s3/<s3bucket>/rekognition/<s3object>.json
```

#### list file s3object tags HTTP GET
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?tags=s3"
```

#### list file rekognition json HTTP GET
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?tags=rekognition"
```

#### set or update s3object tag HTTP PATCH (single key/value)
```   
curl -X PATCH \
     -H "Content-Type: application/json" \
     -d '{"labeler":"karl.rink"}' \
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
     -d '{"labeler":"karl.rink@nationsinfocorp.com","image_url":"https://<s3bucket>.s3.us-west-2.amazonaws.com/<s3object>"}' \
     http://127.0.0.1:8880/s3/<s3bucket>/<s3object>    
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
 
---

#### set rekognition words to s3object tag set
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=words&save=s3tag"
```


---


### run image through aws rekognition

#### run image through aws rekognition detect-labels HTTP GET  
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=detect-labels"
```

  - action: if a rekognition json exists, present that. otherwise run generate new rekognition json and save

#### run image through aws rekognition detect-labels and save/overwrite with new json HTTP GET
```
curl "http://127.0.0.1:8880/s3/<s3bucket>/<s3object>?rekognition=detect-labels&save=true"
```

  - action: generate new rekognition json and save


---


image label manager   
runs an image through rekognition only once (to prevent recurring costs).  update each s3objects tags.   

---

tech notes   
 - aws rekognition detect-labels leverages amazons proprietary ML model for image recognition   
 - image file bytes are not transfered or processed through this interface for rekognition     




