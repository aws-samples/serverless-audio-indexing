import boto3
import json
import os

transcribe_cli = boto3.client('transcribe')
TRANSCRIBE_BUCKET_NAME = os.getenv("TRANSCRIBE_BUCKET_NAME", None)
SOURCE_AUDIO_LANGUAGE = os.getenv("SOURCE_AUDIO_LANGUAGE", None)

def lambda_handler(event, context):
    jsonStateMachine = event
    
    job_name = "transcribe-sourceAudio-%s"%(jsonStateMachine['guid'])
    MediaFileUri = "s3://%s/%s"%(jsonStateMachine['originalFile']['s3BucketName'], jsonStateMachine['originalFile']['s3ObjectKey'])
    s3ObjectExtension = jsonStateMachine['originalFile']['s3ObjectExtension']
    
    response = transcribe_cli.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': MediaFileUri},
        MediaFormat= s3ObjectExtension,
        LanguageCode= SOURCE_AUDIO_LANGUAGE,
        OutputBucketName = TRANSCRIBE_BUCKET_NAME
        #Settings= {'MaxSpeakerLabels': int(event['speakers']), 'ShowSpeakerLabels': True}
    )
    
    jsonStateMachine['transcribe'] = {
        'transcribeJobName': job_name ,    
        'transcribeBucketName': TRANSCRIBE_BUCKET_NAME
    }
    
    return jsonStateMachine