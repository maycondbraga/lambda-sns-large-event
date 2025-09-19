import json
import boto3

sns_topic_arn = 'arn:aws:sns:us-east-1:000000000000:evento-stream-topic.fifo'
s3_bucket_name = 'evento-stream-bucket'

sns_client = boto3.client(
    'sns',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy',
    region_name='us-east-1'
)
sns_client.large_payload_support = s3_bucket_name


def send_event_to_sns(event):
    message = json.dumps(event)
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        MessageGroupId='evento-stream-group'
    )


def lambda_handler(event, context):
    send_event_to_sns(event)
    return {'batchItemFailures': []}
