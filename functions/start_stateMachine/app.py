import json
import boto3
import uuid
import os
import dateutil.parser
from urllib.parse import unquote_plus

stepFunction_cli = boto3.client('stepfunctions')
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN", None)

def lambda_handler(event, context):
    print(event)

    for record in event['Records']:
        s3BucketName = record['s3']['bucket']['name']
        s3ObjectKey = unquote_plus(record['s3']['object']['key'])
        s3_object_extension = record['s3']['object']['key'].split('.')[-1]
        guid = str(uuid.uuid4())
        uploadDate = dateutil.parser.parse(record['eventTime'])

        jsonStateMachine = {
            "originalFile":{
                "s3BucketName":s3BucketName,
                "s3ObjectKey": s3ObjectKey,
                "s3ObjectExtension": s3_object_extension,
                "uploadDate": uploadDate.strftime("%Y-%m-%d %H:%M")
            },
            "guid": guid
        }

        stepFunction_cli.start_execution(
            stateMachineArn= STATE_MACHINE_ARN,
            input= json.dumps(jsonStateMachine),
            name= guid
        )

    return True
