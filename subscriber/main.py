import boto3
from dotenv import load_dotenv
import os
import sys
import time
import json

load_dotenv()


def prime_factorize(n: int):
    result = []
    if n == 1:
        return [1]
    while n % 2 == 0:
        result.append(2)
        n //= 2
    f = 3
    while f * f <= n:
        if n % f == 0:
            result.append(f)
            n //= f
        else:
            f += 2
    if n != 1:
        result.append(n)
    return result


queue_name = 'test-queue'
table_name = 'test-table'

sqs = boto3.resource('sqs', endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
try:
    queue = sqs.get_queue_by_name(QueueName=queue_name)
except Exception as err:
    print(f'{err}', file=sys.stderr)
    exit(1)

dynamoDB = boto3.resource(
        'dynamodb',
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL")
    )
try:
    table = dynamoDB.Table(table_name)
except Exception as err:
    print(f'{err}', file=sys.stderr)
    exit(1)

while True:
    print('pooling...')
    msgs = queue.receive_messages(MaxNumberOfMessages=1, VisibilityTimeout=10)
    if msgs:

        job_id = msgs[0].message_id
        value = json.loads(msgs[0].body)['value']

        # messages which has under zero values are failed to process.
        if value <= 0:
            continue

        print(job_id, value)

        prime_numbers = prime_factorize(value)

        table.put_item(
            Item={
                "jobID": job_id,
                "primeNumbers": prime_numbers,
            }
        )

        msgs[0].delete()
    else:
        time.sleep(1)
