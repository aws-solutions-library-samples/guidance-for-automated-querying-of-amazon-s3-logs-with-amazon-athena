import json
import cfnresponse
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


def lambda_handler(event, context):
    logger.info(event)

    # Start Cloudformation Invocation #
    if event.get('RequestType') == 'Update':
      # logger.info(event)
      try:
        logger.info("Stack event Update, Starting Athena Query Workflow...")
        # Starting Athena Query Workflow
        my_sns_message = f'Starting Athena Query'
        logger.info(f"{my_sns_message}")
        # Start Athena Query Function Invoke
        # Generate Payload for Invocation:
        my_payload = {"my_etag": str(uuid.uuid4()) }
        my_payload_json = json.dumps(my_payload)                                  
        send_sns_message(my_sns_topic_arn, my_sns_message)
        # Start Athena Query Function Invoke
        invoke_query_funct = invoke_function(query_function_name, function_invocation_type, my_payload_json)
        logger.info(invoke_query_funct)    

        responseData = {}
        responseData['message'] = "Successful"
        logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
      except Exception as e:
        logger.error(e)
        responseData = {}
        responseData['message'] = str(e)
        failure_reason = str(e) 
        logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
        cfnresponse.send(event, context, cfnresponse.FAILED, responseData, reason=failure_reason)

    else:
      logger.info(f"Stack event is Delete or Create, nothing to do....")
      responseData = {}
      responseData['message'] = "Completed"
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)  
