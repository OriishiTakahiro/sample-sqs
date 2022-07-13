import boto3
from dotenv import load_dotenv
import os
import sys
import time
import json
import random

load_dotenv()

queue_name = 'test-queue'
sqs = boto3.resource('sqs', endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))

redrive_policy = {
    'deadLetterTargetArn': os.environ.get("DLQ_NAME"),
    'maxReceiveCount': '5'
}

try:
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    # set DLQ
    queue.set_attributes(
        Attributes={
            'RedrivePolicy': json.dumps(redrive_policy)
        }
    )
except Exception as err:
    print(f'{err}', file=sys.stderr)
    exit(1)

i = 0
while True:

    value = -10
    # 10% messages are failed to process on subscriber
    if not (random.randrange(0, 10) == 0):
        value = random.randint(100000, 1000000)

    print(value)

    msgs = [{
        'Id': f'test-message-{i}',
        'MessageBody': json.dumps({'value': value})
    }]

    response = queue.send_messages(Entries=msgs)
    i += 1
    time.sleep(0.3)
