import json
import boto3

transcribe_cli = boto3.client('transcribe')

def lambda_handler(event, context):
    jsonStateMachine = event

    response = transcribe_cli.get_transcription_job(
        TranscriptionJobName= jsonStateMachine['transcribe']['transcribeJobName']
    )
    
    transcriptionJobStatus = response['TranscriptionJob']['TranscriptionJobStatus']
    transcriptFileUri = 0
    
    if(transcriptionJobStatus == 'COMPLETED'): 
        transcriptFileUri = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
    
    jsonStateMachine['transcribe']['transcriptionJobStatus'] = transcriptionJobStatus
    jsonStateMachine['transcribe']['transcriptFileUri'] = transcriptFileUri
    
    return jsonStateMachine