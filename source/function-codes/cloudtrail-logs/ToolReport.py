import json
from botocore.exceptions import ClientError
import logging
import os
import datetime
import boto3
from urllib import parse


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

# Enable Debug Logging
# boto3.set_stream_logger("")


# Define Environmental Variables
my_region = str(os.environ['AWS_REGION'])
my_sns_topic_arn = str(os.environ['sns_topic_arn'])

# Set Service Client
sns = boto3.client('sns', region_name=my_region)

# SNS Message Function
def send_sns_message(sns_topic_arn, sns_message):
    logger.info("Sending SNS Notification Message......")
    sns_subject = 'Notification from AWS Support Troubleshooting Tool'
    try:
        response = sns.publish(TopicArn=sns_topic_arn, Message=sns_message, Subject=sns_subject)
    except ClientError as e:
        logger.error(e)            


def lambda_handler(event, context):
    logger.info(event)
    # Use Etag to prevent duplicate invocation
    my_request_token = event.get('my_etag')
    logger.info(f'Initiating Main Function...')
    s3Bucket = str(event['Records'][0]['s3']['bucket']['name'])
    s3Key = parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
      my_sns_message = f'Athena Query Completed Successfully, kindly retrieve your s3 troubleshooting report in the Amazon S3 bucket path s3://{s3Bucket}/{s3Key} .'
      send_sns_message(my_sns_topic_arn, my_sns_message)
    except Exception as e:
      logger.error(e)
    else: 
      return {
          'statusCode': 200,
          'body': json.dumps('Successful Invocation!')
      }        
