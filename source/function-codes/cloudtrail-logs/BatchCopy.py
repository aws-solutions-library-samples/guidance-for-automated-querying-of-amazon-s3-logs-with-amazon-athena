import json
from urllib import parse
import cfnresponse
import logging
import os
import boto3
import time
from botocore.exceptions import ClientError
from botocore.client import Config
import datetime
from dateutil.tz import tzlocal

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



# S3 Batch Copy Function

def s3_batch_ops_copy_manifest_generator(target_key_prefix, source_bucket_arn, source_bucket_prefix, debug_start_days):
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
    if source_bucket_prefix:
        my_request_kwargs['ManifestGenerator']['S3JobManifestGenerator']['Filter']['KeyNameConstraint'] = {
            'MatchAnyPrefix': [source_bucket_prefix, ]}
        logger.info(f"Source Prefix is present, modified request kwargs to: {my_request_kwargs}")

    try:
        logger.info(f"Submitting kwargs to S3 Batch Operations: {my_request_kwargs}")
        response = s3ControlClient.create_job(**my_request_kwargs)
        logger.info(f"JobID is: {response['JobId']}")
        logger.info(f"S3 RequestID is: {response['ResponseMetadata']['RequestId']}")
        logger.info(f"S3 Extended RequestID is:{response['ResponseMetadata']['HostId']}")
        return response['JobId']
    except ClientError as e:
        logger.error(e)


def lambda_handler(event, context):
    logger.info(f'Event detail is: {event}')
    my_copy_destination = None
    # Retrieve Invocation Variables
    my_logs_bucket = event.get('ResourceProperties').get('your_s3_logs_bucket')
    my_log_prefix = event.get('ResourceProperties').get('your_s3_log_prefix')
    my_log_type = event.get('ResourceProperties').get('your_log_type')
    logger.info(f"my_log_prefix is {my_log_prefix}")
    
    # Set Debug number of days
    offset_days = int(event.get('ResourceProperties').get('debug_duration'))
    my_debug_start_days = datetime.datetime.now().replace(tzinfo=tzlocal()) - datetime.timedelta(offset_days)

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
            time.sleep(150)
            s3_batch_ops_copy_manifest_generator(my_copy_destination, my_logs_bucket_arn, my_log_prefix, my_debug_start_days)
            # check_bucket_exists(s3Bucket)
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
        try:
            logger.info("Stack event is Create or Update. Initiating Logs copy to SupportToolBucket...")
            s3_batch_ops_copy_manifest_generator(my_copy_destination, my_logs_bucket_arn, my_log_prefix, my_debug_start_days)
            # check_bucket_exists(s3Bucket)
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
        logger.info(event)
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
