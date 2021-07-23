
# Image Labeling Tool

## s3 http rest api

#### list all buckets HTTP GET
```
curl http://127.0.0.1:8880/s3/ 
```

#### list files in a bucket (1000 record limit) HTTP GET
```
curl http://127.0.0.1:8880/s3/ninfo-property-images/
```

#### list files in a bucket subdirectory (1000 record limit) HTTP GET
```
curl http://127.0.0.1:8880/s3/ninfo-property-images/rekognition/
```

#### list file HTTP GET
```
curl http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg
```

#### list file HTTP GET
```
curl http://127.0.0.1:8880/s3/ninfo-property-images/rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json
```

#### list file s3object tags HTTP GET
```
curl "http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?tags=s3"
```

#### list file rekognition json HTTP GET
```
curl "http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?tags=rekognition"
```

#### update s3object tag set HTTP PUT
```   
curl -X PUT \
     -H "Content-Type: application/json" \
     -d '{"labler":"karl.rink@nationsinfocorp.com","image_url":"https://ninfo-property-images.s3.us-west-2.amazonaws.com/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg"}' \
     http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg    
```   

---

#### get rekognition json HTTP GET (same as ?tags=rekognition)
```
curl "http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?rekognition=json"
```

#### get rekognition words HTTP GET
```
curl "http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?rekognition=words"
```
 

---


### run image through aws rekognition

#### run image through aws rekognition detect-labels HTTP GET  
```
curl "http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?rekognition=detect-labels"
```

  - action: if a rekognition json exists, present that. otherwise run generate new rekognition json and save

#### run image through aws rekognition detect-labels and save/overwrite with new json HTTP GET
```
curl "http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?rekognition=detect-labels&force=true"
```

  - action: generate new rekognition json and save


---


image label manager   
runs an image through rekognition only once (to prevent redundant charges/costs).  update each s3objects tags. update pdb SQL data. provide a searchable interface. 

---

tech notes   
 - aws rekognition detect-labels leverages amazons own ML model for image recognition   
 - image file bytes are not transfered or processed through this interface.  this interface simply controls rekognition and s3 backend    








