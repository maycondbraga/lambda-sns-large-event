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

    message_body = messages[0]['Body']
    try:
        body_json = json.loads(message_body)
        # Se for payload estendido, buscar no S3
        if isinstance(body_json, list) and len(body_json) > 1 and 's3BucketName' in body_json[1]:
            event = get_event_from_s3(body_json[1])
        else:
            event = message_body
    except Exception:
        event = message_body

    # Remove a mensagem da fila
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=messages[0]['ReceiptHandle']
    )
    return event

# Exemplo de uso:
if __name__ == "__main__":
    evento = read_event_from_sqs()
    print("Evento recebido:", evento)