import json
import boto3

comprehend_cli = boto3.client(service_name='comprehend')

def lambda_handler(event, context):
    jsonStateMachine = event
    
    comprehendJobsStatus = 'IN_PROGRESS'
    
    #Entities Job
    responseEntities = comprehend_cli.describe_entities_detection_job(
        JobId = jsonStateMachine['comprehend']['jobIdEntities']
    )
    entitiesJobStatus = responseEntities['EntitiesDetectionJobProperties']['JobStatus']
    
    #Key Phrases Job Status
    responseKeyPhrases = comprehend_cli.describe_key_phrases_detection_job(
        JobId= jsonStateMachine['comprehend']['jobIdKeyPhrases']
    )
    keyPhrasesJobStatus = responseKeyPhrases['KeyPhrasesDetectionJobProperties']['JobStatus']

    if(entitiesJobStatus == 'COMPLETED'): 
        jsonStateMachine['comprehend']['entitiesFileUri'] = responseEntities['EntitiesDetectionJobProperties']['OutputDataConfig']['S3Uri']

    if(keyPhrasesJobStatus == 'COMPLETED'):
        jsonStateMachine['comprehend']['keyPhrasesFileUri'] = responseKeyPhrases['KeyPhrasesDetectionJobProperties']['OutputDataConfig']['S3Uri']

    #All jobs must be complete
    if ((entitiesJobStatus == 'COMPLETED') and (keyPhrasesJobStatus == 'COMPLETED')):
        comprehendJobsStatus = 'COMPLETED'
    else:
        if ((entitiesJobStatus == 'IN_PROGRESS') or (keyPhrasesJobStatus == 'IN_PROGRESS')): 
            comprehendJobsStatus = 'IN_PROGRESS' 
        else:
            if ((entitiesJobStatus == 'SUBMITTED') or (keyPhrasesJobStatus == 'SUBMITTED')): 
                comprehendJobsStatus = 'IN_PROGRESS' 
            else: comprehendJobsStatus = 'FAILED' 
        
        
    jsonStateMachine['comprehendJobsStatus'] = comprehendJobsStatus 
    return jsonStateMachine