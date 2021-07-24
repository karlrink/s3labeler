

import boto3
s3 = boto3.resource('s3')
obj = s3.Object(bucketname, itemname)
body = obj.get()['Body'].read()

# download,
import boto3
bucketname = 'my-bucket' # replace with your bucket name
filename = 'my_image_in_s3.jpg' # replace with your object key
s3 = boto3.resource('s3')
s3.Bucket(bucketname).download_file(filename, 'my_localimage.jpg')

import boto3
s3 = boto3.resource("s3")
srcFileName="abc.txt"
destFileName="s3_abc.txt"
bucketName="mybucket001"
k = Key(bucket,srcFileName)
k.get_contents_to_filename(destFileName)


s3 = boto3.resource('s3')
bucket = s3.Bucket('test-bucket')
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
for obj in bucket.objects.all():
    key = obj.key
    body = obj.get()['Body'].read()


def lambda_handler(event, context):
    import boto3

    s3 = boto3.client('s3')
    data = s3.get_object(Bucket='my_s3_bucket', Key='main.txt')
    contents = data['Body'].read()
    print(contents)




