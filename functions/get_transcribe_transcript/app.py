import json
import boto3
import ntpath
import os

s3_cli = boto3.client('s3')
MAIN_BUCKET_NAME = os.getenv("MAIN_BUCKET_NAME", None)

def lambda_handler(event, context):
    jsonStateMachine = event
    
    print (jsonStateMachine)
    
    s3TranscribeBucketName = jsonStateMachine['transcribe']['transcribeBucketName']
    s3TranscribeObjectUri = ntpath.basename(jsonStateMachine['transcribe']['transcriptFileUri'])
    guid = jsonStateMachine['guid']
    
    #Reading Transcribe File    
    transcribeFile = s3_cli.get_object(Bucket=s3TranscribeBucketName, Key=s3TranscribeObjectUri)
    transcribeFileDataBody = transcribeFile['Body'].read().decode('utf-8')
    data = json.loads(transcribeFileDataBody)
    
    #Getting pure Transcribe
    transcript = data['results']['transcripts'][0]['transcript']
    
    s3_cli.put_object(
        Bucket = MAIN_BUCKET_NAME,
        Key='transcriptionText/{0}.txt'.format(guid),
        Body=transcript
    )

    jsonStateMachine['transcriptionText'] = {}
    
    jsonStateMachine['transcriptionText']['s3ObjectKey'] = 'transcriptionText/{0}.txt'.format(guid)
    jsonStateMachine['transcriptionText']['s3BucketName'] = MAIN_BUCKET_NAME
    
    return jsonStateMachine