import json
import io
import tarfile
import boto3
import os

s3_cli = boto3.client('s3')
MAIN_BUCKET_NAME = os.getenv("MAIN_BUCKET_NAME", None)

def lambda_handler(event, context):
    jsonStateMachine = event

    #Getting Infos
    guid = jsonStateMachine['guid']
    s3BucketName = MAIN_BUCKET_NAME
    
    #Jobs Infos
    s3KeyPhrasesKey = jsonStateMachine['comprehend']['keyPhrasesFileUri'].replace('s3://{}/'.format(s3BucketName), '')
    s3EntitiesKey = jsonStateMachine['comprehend']['entitiesFileUri'].replace('s3://{}/'.format(s3BucketName), '')
        
    # Reading tar.gz files
    s3ObjectKeyPhrases = s3_cli.get_object(Bucket=s3BucketName, Key=s3KeyPhrasesKey)
    s3ObjectEntities = s3_cli.get_object(Bucket=s3BucketName, Key=s3EntitiesKey)
    keyPhrasesData = io.BytesIO(s3ObjectKeyPhrases['Body'].read())
    entitiesData = io.BytesIO(s3ObjectEntities['Body'].read())

    # Decompressing the 'output' files
    tarKeyPhrases = tarfile.open(fileobj=keyPhrasesData)
    tarEntities = tarfile.open(fileobj=entitiesData)
    decompressedKeyPhrasesOutput = tarKeyPhrases.extractfile('output').read().decode("utf-8")
    decompressedEntitiesOutput = tarEntities.extractfile('output').read().decode("utf-8")
        
        
    # Saving decompressed .json files on S3
    s3_cli.put_object(
        Bucket = s3BucketName,
        Key='comprehendOutputDecompressed/{}/keyPhrases.json'.format(guid),
        Body=decompressedKeyPhrasesOutput
    )
        
    s3_cli.put_object(
        Bucket = s3BucketName,
        Key='comprehendOutputDecompressed/{}/entities.json'.format(guid),
        Body=decompressedEntitiesOutput
    )
        
    jsonStateMachine['comprehendOutputDecompressed'] = {}
    jsonStateMachine['comprehendOutputDecompressed']['keyPhrases'] = 'comprehendOutputDecompressed/{}/keyPhrases.json'.format(guid)
    jsonStateMachine['comprehendOutputDecompressed']['entities'] = 'comprehendOutputDecompressed/{}/entities.json'.format(guid)
        
    return jsonStateMachine  
