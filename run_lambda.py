import random
import string
from handler import lambda_handler

def generate_dynamodb_stream_event(payload_size):
    # Gera payloads aleatórios para OldImage e NewImage
    old_payload = ''.join(random.choices(string.ascii_letters, k=payload_size))
    new_payload = ''.join(random.choices(string.ascii_letters, k=payload_size))
    event = {
        "Records": [
            {
                "eventID": "1",
                "eventName": "MODIFY",
                "dynamodb": {
                    "OldImage": {
                        "id": {"S": "123"},
                        "payload": {"S": old_payload}
                    },
                    "NewImage": {
                        "id": {"S": "123"},
                        "payload": {"S": new_payload}
                    }
                }
            }
        ]
    }
    return event

if __name__ == "__main__":
    context = None

    # Evento pequeno (<256KB)
    small_event = generate_dynamodb_stream_event(1024)  # 1KB
    print("Enviando evento pequeno...")
    lambda_handler(small_event, context)

    # Evento grande (>256KB)
    large_event = generate_dynamodb_stream_event(300_000)  # 300KB
    print("Enviando evento grande...")
    lambda_handler(large_event, context)
