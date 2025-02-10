import json
import cfnresponse
import logging
import os
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

# Enable debugging for troubleshooting
# boto3.set_stream_logger("")


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])       

# Create Service Resource
s3 = boto3.resource('s3', region_name=my_region)   

### Empty Bucket Function ###

def empty_bucket(bucket):
    try:
        s3Bucket = s3.Bucket(bucket)
        s3Bucket.object_versions.delete()
    except Exception as e:
        logger.error(e)
        raise
    else:
        logger.info("Bucket Emptying Successful!")    


def lambda_handler(event, context):
    logger.info(f'Event detail is: {event}')
    bucket_name = event['ResourceProperties'].get('bucketname')

    # Start Cloudformation Invocation #
    if event.get('RequestType') == 'Delete' or event.get('RequestType') == 'Update':
      # logger.info(event)
      try:
        logger.info("Stack event is Delete or Update, Emptying S3 Bucket...")
        empty_bucket(bucket_name)
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
      logger.info(f"Stack event is Create, nothing to do....")
      responseData = {}
      responseData['message'] = "Completed"
      logger.info(f"Sending Invocation Response {responseData['message']} to Cloudformation Service")
      cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)                 
