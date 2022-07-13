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

try:
    queue = sqs.get_queue_by_name(QueueName=queue_name)
except Exception as err:
    print(f'{err}', file=sys.stderr)
    exit(1)

i = 0
while True:
    msgs = [{
        'Id': f'test-message-{i}',
        'MessageBody': json.dumps({'value': random.randrange(10000, 50000)})
    }]
    response = queue.send_messages(Entries=msgs)
    print(response)
    i += 1
    time.sleep(0.3)
