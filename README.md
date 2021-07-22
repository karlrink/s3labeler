
# Image Labeling Tool

## s3 http rest api

### list all buckets
curl http://127.0.0.1:8880/s3/ 

### list files in a bucket  (1000 record limit)
curl http://127.0.0.1:8880/s3/ninfo-property-images/

### list files in a bucket subdirectory (1000 record limit)
curl http://127.0.0.1:8880/s3/ninfo-property-images/rekogniton/

### list file
curl http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg

### list file
curl http://127.0.0.1:8880/s3/ninfo-property-images/rekognition/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg.json


---


## run image through aws rekognition

### run image through aws rekognition detect-labels  
curl http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?rekognition=detect-labels

  - action: if a rekognition json exists, present that. otherwise run generate new rekognition json and save

### run image through aws rekognition detect-labels and save/overwrite with new json
curl http://127.0.0.1:8880/s3/ninfo-property-images/2eece964b6f902124052810e5a92d6f9ca715c1b.jpg?rekognition=detect-labels&force=true

  - action: generate new rekognition json and save


---


OBJECTIVE
runs an image through rekognition only once (to prevent redundant charges/costs).  update each s3objects tags. update pdb SQL data. provide a searchable interface. 


