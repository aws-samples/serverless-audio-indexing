import json
import boto3
import uuid
import json
import ntpath
import os

comprehend_cli = boto3.client(service_name='comprehend')

#Environment Variables
MAIN_BUCKET_NAME = os.getenv("MAIN_BUCKET_NAME", None)
COMPREHEND_SERVICE_ROLE_ARN = os.getenv("COMPREHEND_SERVICE_ROLE_ARN", None)
SOURCE_AUDIO_LANGUAGE = os.getenv("SOURCE_AUDIO_LANGUAGE", None)


TRANSCRIBE_LANGUAGE_TO_COMPREHEND_LANGUAGE = {
    #Dict that maps Amazon Transcribe language code to Amazon Comprehend language Code
    
    'de-DE': 'de', # German
    'de-CH': 'de', # Swiss German
    'en-AU': 'en', # Australian English
    'en-GB': 'en', # British English
    'en-IN': 'en', # Indian English
    'en-IE': 'en', # Irish English
    'en-AB': 'en', # Scottish English
    'en-US': 'en', # US English
    'en-WL': 'en', # Welsh English
    'es-ES': 'es', # Spanish
    'es-US': 'es', # US Spanish
    'it-IT': 'it', # Italian
    'pt-PT': 'pt', # Portuguese
    'pt-BR': 'pt', # Brazilian Portuguese
    'fr-FR': 'fr', # French
    'fr-CA': 'fr', # Canadian French
    'ja-JP': 'ja', # Japanese
    'ko-KR': 'ko', # Korean
    'hi-IN': 'hi', # Hindi
    'ar-SA': 'ar', # Arabic
    'zh-CN': 'zh' # Chinese (simplified)
}

def lambda_handler(event, context):
    jsonStateMachine = event
    
    #Getting Informations
    guid = jsonStateMachine['guid']
    s3BucketName = MAIN_BUCKET_NAME
    transcriptionText = jsonStateMachine['transcriptionText']['s3ObjectKey']
    
    #S3 URIs
    inputS3Uri = "s3://%s/"%(s3BucketName)
    outputS3Uri = "s3://%s/comprehendOutputRAW/"%(s3BucketName)

    #Get Comprehend Language Code
    languageCode = TRANSCRIBE_LANGUAGE_TO_COMPREHEND_LANGUAGE[SOURCE_AUDIO_LANGUAGE]
    
    #Start Async Jobs
    jsonStateMachine['comprehend'] = {}
    jsonStateMachine['comprehend']['jobIdKeyPhrases'] = detect_keyphrases_job(inputS3Uri, outputS3Uri, transcriptionText, languageCode)
    jsonStateMachine['comprehend']['jobIdEntities'] = detect_entities_job(inputS3Uri, outputS3Uri, transcriptionText, languageCode)

    return jsonStateMachine

# Start Async job to detect key phrases
def detect_keyphrases_job(inputS3Uri, outputS3Uri, file, languageCode):
    jobIdentifier = 'key-phrases-job-{0}'.format(ntpath.basename(file).replace('.txt', ''))
    
    response = comprehend_cli.start_key_phrases_detection_job(
        InputDataConfig={
            'S3Uri': '{0}{1}'.format(inputS3Uri, file),
            'InputFormat': 'ONE_DOC_PER_FILE'
        },
        OutputDataConfig={
            'S3Uri': outputS3Uri,
        },
        DataAccessRoleArn= COMPREHEND_SERVICE_ROLE_ARN,
        JobName= jobIdentifier,
        LanguageCode= languageCode
    )
    
    return response['JobId']
 
# Start Async job to detect Entities
def detect_entities_job(inputS3Uri, outputS3Uri, file, languageCode):
    jobIdentifier = 'entities-job-{0}'.format(ntpath.basename(file).replace('.txt', ''))

    response = comprehend_cli.start_entities_detection_job(
        InputDataConfig={
            'S3Uri': '{0}{1}'.format(inputS3Uri, file),
            'InputFormat': 'ONE_DOC_PER_FILE'
        },
        OutputDataConfig={
            'S3Uri': outputS3Uri,
        },
        DataAccessRoleArn= COMPREHEND_SERVICE_ROLE_ARN,
        JobName=jobIdentifier,
        LanguageCode= languageCode
    )

    return response['JobId']