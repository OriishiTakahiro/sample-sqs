# SQS Samples with localstack

Sample implementation with job queue system.

![System Architecture](./imgs/architecture.png )

## Usage

### Start localstack

```sh
$ cd /path/to/localstack/dir
$ docker compose up -d
```

### Create SQS Queue

```sh
$ aws sqs create-queue --queue-name test-queue --endpoint-url http://localhost:4566 --profile localstack
```

### Create DynamoDB Table

```sh
$ aws --endpoint-url http://localhost:4566 --profile localstack dynamodb \
    create-table \
    --table-name test-table \
    --attribute-definitions \
        AttributeName=jobID,AttributeType=S \
    --key-schema AttributeName=jobID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

### Start 

Start one publisher and some subscribers on individual shells.

**publisher**

```sh
$ cd publisher
$ python3 main.py
```

**subscriber**

```sh
$ cd subscriber
$ python3 main.py
```

### Check DynamoDB

```sh
$ aws --endpoint-url http://localhost:4566 --profile localstack dynamodb scan --table-name test-table
```
