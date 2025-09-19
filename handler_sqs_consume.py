import boto3
import json

sqs_client = boto3.client(
    'sqs',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy',
    region_name='us-east-1'
)
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='dummy',
    aws_secret_access_key='dummy',
    region_name='us-east-1'
)

queue_url = 'http://localhost:4566/000000000000/evento-stream-queue.fifo'


def get_event_from_s3(s3_info):
    obj = s3_client.get_object(
        Bucket=s3_info['s3BucketName'],
        Key=s3_info['s3Key']
    )
    return obj['Body'].read().decode()


def read_event_from_sqs():
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1
    )
    messages = response.get('Messages', [])
    if not messages:
        return None

    msg = messages[0]
    msg_attrs = msg.get('MessageAttributes', {})
    if 'ExtendedPayloadSize' in msg_attrs:
        body_json = json.loads(msg['Body'])
        event = get_event_from_s3(body_json[1])
    else:
        event = json.loads(msg['Body'])

    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=msg['ReceiptHandle']
    )
    return event


if __name__ == "__main__":
    evento = read_event_from_sqs()
    print("Evento recebido:", evento)
