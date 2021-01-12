import boto3
import json
import requests
import os
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers

REGION = os.getenv("REGION", None)
SERVICE = 'es'
CREDENTIALS = boto3.Session().get_credentials()
HOST = os.getenv("HOST", None)
MAIN_BUCKET_NAME = os.getenv("MAIN_BUCKET_NAME", None)

awsauth = AWS4Auth(CREDENTIALS.access_key, CREDENTIALS.secret_key, REGION, SERVICE, session_token=CREDENTIALS.token)
s3_cli = boto3.client('s3')

FILETYPE_DICT = {
    'keyPhrases': 'KeyPhrases', 
    'entities': 'Entities'
}

# Lambda execution starts here
def lambda_handler(event, context):
    jsonStateMachine = event

    # Getting event info
    guid = jsonStateMachine['guid']

    #Key Phrases File Insertion
    keyPhrasesKey = jsonStateMachine['comprehendOutputDecompressed']['keyPhrases']
    fileType = FILETYPE_DICT['keyPhrases']
    downloadObject_InsertES(guid,keyPhrasesKey, fileType)

    #Entities File Insertion
    entitiesKey = jsonStateMachine['comprehendOutputDecompressed']['entities']
    fileType = FILETYPE_DICT['entities']
    downloadObject_InsertES(guid, entitiesKey, fileType)

    return  jsonStateMachine


def downloadObject_InsertES(guid, Objectkey, fileType):
    # Parsing event info
    bucketName = MAIN_BUCKET_NAME

    #S3 Object get data
    obj = s3_cli.get_object(Bucket=bucketName, Key=Objectkey)
    body = obj['Body'].read().decode('utf-8').replace('\\N', '\\n')
    data = json.loads(body)
    data = data[fileType]

    actions = []
    actions = [
        {
           '_op_type': 'index',
            '_index': fileType.lower(),
            'guid': guid,
            'doc': entry 
        }
        for entry in data
    ]
    esClient = Elasticsearch(
        hosts=[{'host': HOST, 'port': 443}],
        use_ssl=True,
        verify_certs=True,
        http_auth=awsauth,
        connection_class=RequestsHttpConnection)

    helpers.bulk(esClient, actions)