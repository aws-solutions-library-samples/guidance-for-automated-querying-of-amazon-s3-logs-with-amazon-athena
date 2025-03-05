import json
from urllib import parse
import cfnresponse
import logging
import os
import boto3
import time
import uuid
from botocore.exceptions import ClientError
from botocore.client import Config
import datetime
from dateutil.tz import tzlocal
from datetime import datetime

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])
my_role_arn = str(os.environ['batch_ops_role'])
report_bucket_name = str(os.environ['batch_ops_report_bucket'])
accountId = str(os.environ['my_account_id'])
my_s3_access_log_copy_location = str(os.environ['s3_access_log_copy_location'])
my_cloudtrail_log_copy_location = str(os.environ['cloudtrail_log_copy_location'])
query_function_name = str(os.environ['query_function'])
my_sns_topic_arn = str(os.environ['sns_topic_arn'])     

# Other Variables
function_invocation_type = 'RequestResponse'            


# Specify variables #############################

# Job Manifest Details ################################
job_manifest_format = 'S3InventoryReport_CSV_20211130'
job_manifest_prefix = str(os.environ['batch_ops_manifest_prefix'])

# Job Report Details ############################
report_prefix = str(os.environ['batch_ops_report_prefix'])
report_format = 'Report_CSV_20180820'
report_scope = 'AllTasks'


# Construct ARNs

report_bucket_arn = 'arn:aws:s3:::' + report_bucket_name
target_resource_arn = 'arn:aws:s3:::' + report_bucket_name

# Manifest Generator Variable
manifest_gen_filter_storage_class_list = ['STANDARD', 'ONEZONE_IA', 'STANDARD_IA', 'INTELLIGENT_TIERING']
# Specify checksum algorithm
my_checksum_algorithm = 'SHA256'  # 'CRC32'|'CRC32C'|'SHA1'|'SHA256'

# Initiate Service Clients ###################
s3ControlClient = boto3.client('s3control', region_name=my_region)
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



# S3 Batch Copy Function

def s3_batch_ops_copy_manifest_generator(target_key_prefix, source_bucket_arn, source_bucket_prefix, obj_created_before_string, obj_created_after_string):
    # Convert input date to datetime format
    debug_start_days = datetime.strptime(obj_created_after_string, '%Y-%m-%d')
    # Initiate Batch Operations Request parameters
    my_request_kwargs = {
        'AccountId': accountId,
        'ConfirmationRequired': False,
        'Operation': {
            'S3PutObjectCopy': {
                'TargetResource': target_resource_arn,
                'CannedAccessControlList': 'private',
                'MetadataDirective': 'COPY',
                'TargetKeyPrefix': target_key_prefix,
                'ChecksumAlgorithm': my_checksum_algorithm
            }
        },
        'Report': {
            'Bucket': report_bucket_arn,
            'Format': report_format,
            'Enabled': True,
            'Prefix': report_prefix,
            'ReportScope': 'AllTasks'
        },
        'ManifestGenerator': {
            'S3JobManifestGenerator': {
                'SourceBucket': source_bucket_arn,
                'ManifestOutputLocation': {
                    'Bucket': report_bucket_arn,
                    'ManifestPrefix': job_manifest_prefix,
                    'ManifestEncryption': {
                        'SSES3': {},
                    },
                    'ManifestFormat': job_manifest_format
                },
                'Filter': {
                    'CreatedAfter': debug_start_days,
                    'MatchAnyStorageClass': manifest_gen_filter_storage_class_list
                },
                'EnableManifestOutput': True
            }
        },
        'Priority': 10,
        'RoleArn': my_role_arn,
        'Tags': [
            {
                'Key': 'job-created-by',
                'Value': 'aws-support-troubleshooting-tool-for-s3'
            },
        ]
    }

    logger.info(my_request_kwargs)
    # Include Source Bucket Prefix if specified
    if source_bucket_prefix:
        my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['KeyNameConstraint'] = {
            'MatchAnyPrefix': [source_bucket_prefix, ]}
        logger.info(f"Source Prefix is present, modified request kwargs to: {my_request_kwargs}")

    # Include Created before time if specified
    # Convert date string to date time:
    if obj_created_before_string:
        obj_created_before = datetime.strptime(obj_created_before_string, '%Y-%m-%d')
        my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['CreatedBefore'] = obj_created_before


    try:
        logger.info(f"Submitting kwargs to S3 Batch Operations: {my_request_kwargs}")
        response = s3ControlClient.create_job(**my_request_kwargs)
        logger.info(f"JobID is: {response['JobId']}")
        logger.info(f"S3 RequestID is: {response['ResponseMetadata']['RequestId']}")
        logger.info(f"S3 Extended RequestID is:{response['ResponseMetadata']['HostId']}")
        return response['JobId']
    except ClientError as e:
        logger.error(e)
        raise e


def lambda_handler(event, context):
    logger.info(f'Event detail is: {event}')
    my_copy_destination = None
    # Retrieve Invocation Variables
    my_logs_bucket = event.get('ResourceProperties').get('your_s3_logs_bucket')
    my_log_prefix = event.get('ResourceProperties').get('your_s3_log_prefix')
    my_log_type = event.get('ResourceProperties').get('your_log_type')
    my_log_created_before = event.get('ResourceProperties').get('log_created_before')
    my_log_created_after = event.get('ResourceProperties').get('log_created_after')              
    logger.info(f"my_log_prefix is {my_log_prefix}")            
    
    # Set Copy destination depending on Log Type
    if my_log_type == 'CloudTrail':
        my_copy_destination = my_cloudtrail_log_copy_location
    else:
        my_copy_destination = my_s3_access_log_copy_location

        # Generate ARNs
    my_logs_bucket_arn = 'arn:aws:s3:::' + my_logs_bucket
    # my_manifest_bucket_arn = 'arn:aws:s3:::' + manifest_bucket

    # Initiate Custom lambda Invocation based on Stack Request Type
    if event.get('RequestType') == 'Create':
        # logger.info(event)
        try:
            logger.info("Stack event is Create or Update. Initiating Logs copy to SupportToolBucket...")
            # Introduce a delay to allow consistency
            # sleep is included intentionally
            # nosemgrep: arbitrary-sleep
            time.sleep(150)  # nosemgrep: arbitrary-sleep
            s3_batch_ops_copy_manifest_generator(my_copy_destination, my_logs_bucket_arn, my_log_prefix, my_log_created_before, my_log_created_after)
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

    elif event.get('RequestType') == 'Update':
        # logger.info(event)
        # Get details of the previous parameter values
        previous_my_log_created_after = event.get('OldResourceProperties').get('log_created_after')
        previous_my_log_created_before = event.get('OldResourceProperties').get('log_created_before')
        logger.info(f"previous_my_log_created_after is: {previous_my_log_created_after}")
        logger.info(f"previous_my_log_created_before is: {previous_my_log_created_before}")
        # Convert from string to datetime and selectively perform a copy
        current_after_date = datetime.strptime(my_log_created_after, '%Y-%m-%d')
        current_before_date = datetime.strptime(my_log_created_before, '%Y-%m-%d')
        old_after_date = datetime.strptime(previous_my_log_created_after, '%Y-%m-%d')
        old_before_date = datetime.strptime(previous_my_log_created_before, '%Y-%m-%d')
        # Initiate Batch Operations copy only if the logs files are not already included in previous copy
        if current_after_date < old_after_date or current_before_date > old_before_date:
            # Initiate Batch Operations copy
            logger.info(f"The current CreatedAfterDate is earlier than the existing one Or CreatedBeforeDate is later Initiate BOPs Job...")
            try:
                logger.info("Stack event is Create or Update. Initiating Logs copy to SupportToolBucket...")
                s3_batch_ops_copy_manifest_generator(my_copy_destination, my_logs_bucket_arn, my_log_prefix, my_log_created_before, my_log_created_after)
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
            # We are Skipping Copying objects with S3 Batch Operations, but we need to run an Athena query
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

    elif event.get('RequestType') == 'Delete':
        # logger.info(event)
        try:
            logger.info(f"Stack event is Delete, nothing to do....")
            responseData = {}
            responseData['message'] = "Completed"
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)
        except Exception as e:
            logger.error(e)
            responseData = {}
            responseData['message'] = str(e)
            logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
            cfnresponse.send(event, context, cfnresponse.FAILED, responseData)
