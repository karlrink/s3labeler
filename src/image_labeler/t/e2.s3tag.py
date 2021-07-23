
import boto3

s3_client = boto3.client(
    's3',
    region_name='region-name',
    aws_access_key_id='aws-access-key-id',
    aws_secret_access_key='aws-secret-access-key',
)

get_tags_response = s3_client.get_object_tagging(
    Bucket='your-bucket-name',
    Key='folder-if-any/file-name.extension',
)

put_tags_response = s3_client.put_object_tagging(
    Bucket='your-bucket-name',
    Key='folder-if-any/file-name.extension',
    Tagging={
        'TagSet': [
            {
                'Key': 'tag-key',
                'Value': 'tag-value'
            },
        ]
    }
)




