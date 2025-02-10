import json
import boto3
import botocore
import os
import logging
import datetime
import uuid
from botocore.exceptions import ClientError
from urllib import parse

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Initiate Variables

# Lambda Environment Variables
accountId = str(os.environ['my_account_id'])
my_region = str(os.environ['AWS_REGION'])
query_function_name = str(os.environ['query_function'])
my_sns_topic_arn = str(os.environ['sns_topic_arn'])     

# Other Variables
function_invocation_type = 'RequestResponse'          

# Create Service Clients
s3ControlClient = boto3.client('s3control', region_name=my_region)
s3Client = boto3.client('s3', region_name=my_region)
lambdaClient = boto3.client('lambda', region_name=my_region)
sns = boto3.client('sns', region_name=my_region)

# Describe S3 Batch Operations Job
def s3_batch_describe_job(my_job_id):
    response = s3ControlClient.describe_job(
        AccountId=accountId,
        JobId=my_job_id
    )
    job_desc = (response.get('Job'))
    return job_desc


# SNS Message Function
def send_sns_message(sns_topic_arn, sns_message):
    logger.info("Sending SNS Notification Message......")
    sns_subject = 'Notification from AWS Support Troubleshooting Tool'
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)


# Function to Invoke Copy Function Worker
def invoke_function(function_name, invocation_type, payload):
    invoke_response = lambdaClient.invoke(
        FunctionName=function_name,
        InvocationType=invocation_type,
        Payload=payload,

    )
    response_payload = json.loads(invoke_response['Payload'].read().decode("utf-8"))
    return response_payload        


# Get S3 Batch Operations Tag
def get_job_tagging(bops_job_id):
    logger.info("Initiate GetJob Tagging")
    try:
        get_job_tag_response = s3ControlClient.get_job_tagging(
            AccountId=accountId,
            JobId=bops_job_id
        )
    except ClientError as e:
        logger.error(e)
    else:
        logger.info("Successfully retrieved Job Tags")
        tag_key = get_job_tag_response.get('Tags')[0].get('Key')
        tag_value = get_job_tag_response.get('Tags')[0].get('Value')
        return tag_key, tag_value


def lambda_handler(event, context):
    logger.info(event)
    try:
        # s3Bucket = str(event['detail']['bucket']['name'])
        # s3Key = parse.unquote_plus(event['detail']['object']['key'], encoding='utf-8')
        # etag = str(event['detail']['object']['etag'])                  
        s3Bucket = str(event['Records'][0]['s3']['bucket']['name'])
        s3Key = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        etag = str(event['Records'][0]['s3']['object']['eTag'])
        logger.info(f"S3 Key is: {s3Key}")
        retrieve_job_id = s3Key.split('/')[-2]
        job_id = retrieve_job_id.replace('job-', '', 1)
        my_job_details = s3_batch_describe_job(job_id)
        logger.info(f"Batch Operation Job details: {my_job_details}")
        job_operation = list(my_job_details.get('Operation').keys())[0]
        job_status = my_job_details.get('Status')
        job_arn = my_job_details.get('JobArn')
        job_creation_datetime = str(my_job_details.get('CreationTime'))
        job_completion_datetime = str(my_job_details.get('TerminationDate'))
        number_of_tasks = my_job_details.get('ProgressSummary').get('TotalNumberOfTasks')
        # number_of_fields = str(len(my_job_details.get('Manifest').get('Spec').get('Fields')))
        tasks_succeeded = my_job_details.get('ProgressSummary').get('NumberOfTasksSucceeded')
        tasks_failed = my_job_details.get('ProgressSummary').get('NumberOfTasksFailed')
        logger.info(f'Number of Tasks: {number_of_tasks}')
        logger.info(f'Tasks_succeeded: {tasks_succeeded}')
        logger.info(f'Tasks_failed: {tasks_failed}')

        # Send a notification to the user if Batch Operations Job fails
        if job_status == 'Failed':
            my_sns_message = f'Batch Operations Copy Job Failed! Please check the Batch Operations Job JobID {job_id} Completion Report in the Amazon S3 Console for more details.'
            logger.info(f"{my_sns_message}")
            send_sns_message(my_sns_topic_arn, my_sns_message)

        # Send notification if all tasks fail or initiate workflow if all or some tasks succeed
        if job_status == 'Complete':
            if number_of_tasks == tasks_failed:
                my_sns_message = f'All Tasks Failed! Please check the Batch Operations Job JobID {job_id} Completion Report in the Amazon S3 Console for more details.'
                logger.info(f"{my_sns_message}")
                send_sns_message(my_sns_topic_arn, my_sns_message)
            else:
                job_details = str(my_job_details)
                # Only work on Tagged Jobs
                job_tag_key, job_tag_value = get_job_tagging(job_id)
                # Trigger next workflow for a Successfully Completed Copy Job
                if job_operation == 'S3PutObjectCopy':
                    # Starting Condition
                    if job_tag_key == 'job-created-by' and job_tag_value == 'aws-support-troubleshooting-tool-for-s3' and job_status == 'Complete':
                        my_sns_message = f'Copy Job {job_id} Completed: {tasks_failed} failed out of {number_of_tasks}. Please check the Batch Operations Job JobID {job_id} in the Amazon S3 Console for more details.'
                        logger.info(f"{my_sns_message}")
                        # Start Athena Query Function Invoke
                        # Generate Payload for Invocation:
                        my_payload = {"my_etag": etag}
                        my_payload_json = json.dumps(my_payload)                                  
                        send_sns_message(my_sns_topic_arn, my_sns_message)
                        # Send notification for Athena query start
                        my_sns_message = f'Starting Athena Query'
                        logger.info(f"{my_sns_message}")
                        send_sns_message(my_sns_topic_arn, my_sns_message)
                        invoke_query_funct = invoke_function(query_function_name, function_invocation_type, my_payload_json)
                        logger.info(invoke_query_funct)                               

                    elif job_tag_key == 'job-created-by' and job_tag_value == 'aws-support-troubleshooting-tool-for-s3' and job_status == 'Failed':
                        my_sns_message = f'S3 Logs Copy Job {job_id} failed, please check the Batch Operations Job JobID {job_id} in the Amazon S3 Console for more details!'
                        logger.info(f"{my_sns_message}")
                        send_sns_message(my_sns_topic_arn, my_sns_message)

    except Exception as e:
        logger.error(e)
        raise
