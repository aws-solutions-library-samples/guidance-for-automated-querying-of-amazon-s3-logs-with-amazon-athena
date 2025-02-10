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
my_glue_db = str(os.environ['glue_db'])
my_glue_tbl = str(os.environ['glue_tbl'])
my_workgroup_name = str(os.environ['workgroup_name'])
my_s3_bucket = str(os.environ['s3_bucket'])
my_query_duration = int(os.environ['query_duration'])
my_query_analysis_type = str(os.environ['query_analysis_type'])


my_current_date = datetime.datetime.now().date()
# retain_date = datetime.datetime.now().date() + datetime.timedelta(num_days) + datetime.timedelta(safety_margin)

logger.info(f'my_current_date is: {my_current_date}')
logger.info(f'my_query_duration is: {my_query_duration}')


# Set Service Client
athena_client = boto3.client('athena', region_name=my_region)


def start_query_execution(query_string, athena_db, workgroup_name, job_request_token):
    logger.info(f'Starting Athena query...... with query string: {query_string}')
    try:
        execute_query = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={
                'Database': athena_db
            },
            WorkGroup=workgroup_name,
            ClientRequestToken= job_request_token,
        )
    except ClientError as e:
        logger.info(e)
    else:
        logger.info(f'Query Successful: {execute_query}')


def lambda_handler(event, context):
    logger.info(event)
    # Use Etag to prevent duplicate invocation
    my_request_token = event.get('my_etag')
    logger.info(f'Initiating Main Function...')

    # Specify the Athena Query #

    match my_query_analysis_type:

        case "AnonymousAccess":
            logger.info("AnonymousAccess") 
            if my_s3_bucket:
                my_query_string = f"""                    
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                json_extract_scalar(requestParameters, '$.key') as objectKey, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                errorCode,
                errorMessage, 
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}" 
                WHERE eventsource = 's3.amazonaws.com'
                AND
                json_extract_scalar(requestParameters, '$.bucketName') = '{my_s3_bucket}'                              
                and
                userIdentity.accountId = 'anonymous';
                """                           
            else:
                my_query_string = f"""                    
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                json_extract_scalar(requestParameters, '$.key') as objectKey, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                errorCode,
                errorMessage, 
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}" 
                WHERE eventsource = 's3.amazonaws.com'
                and
                userIdentity.accountId = 'anonymous';
                """         

        case "CreateBucket":
            logger.info("CreateBucket") 
            if my_s3_bucket:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}" 
                WHERE eventsource = 's3.amazonaws.com'
                AND
                json_extract_scalar(requestParameters, '$.bucketName') = '{my_s3_bucket}'                              
                and
                eventname = 'CreateBucket';
                """                           
            else:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}" 
                WHERE eventsource = 's3.amazonaws.com'
                and
                eventname = 'CreateBucket';
                """                                                       

        case "DeleteBucket-*":
            logger.info("DeleteBucket-*")
            if my_s3_bucket:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"  
                WHERE eventsource = 's3.amazonaws.com'
                AND
                json_extract_scalar(requestParameters, '$.bucketName') = '{my_s3_bucket}'                              
                and
                eventname like 'DeleteBucket%';
                """                                
            else:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"  
                WHERE eventsource = 's3.amazonaws.com'
                and
                eventname like 'DeleteBucket%';
                """                        

        case "PutBucket-*":
            logger.info("PutBucket-*") 
            if my_s3_bucket:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE eventsource = 's3.amazonaws.com'
                AND
                json_extract_scalar(requestParameters, '$.bucketName') = '{my_s3_bucket}'                  
                and
                eventname like 'PutBucket%';
                """  
            else:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE eventsource = 's3.amazonaws.com'               
                and
                eventname like 'PutBucket%';
                """                                                              
                                
        case "DeleteObject-*":
            logger.info("DeleteObject-*")
            if my_s3_bucket:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                json_extract_scalar(requestParameters, '$.key') as objectKey, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE eventsource = 's3.amazonaws.com'
                AND
                json_extract_scalar(requestParameters, '$.bucketName') = '{my_s3_bucket}'
                AND
                (eventname = 'DeleteObject' or eventname like 'DeleteObject%') ;
                """  
            else:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                json_extract_scalar(requestParameters, '$.key') as objectKey, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE eventsource = 's3.amazonaws.com'
                AND
                (eventname = 'DeleteObject' or eventname like 'DeleteObject%') ;
                """                                      

        case "AccessDenied":
            logger.info("AccessDenied")
            if my_s3_bucket:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                json_extract_scalar(requestParameters, '$.key') as objectKey, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                errorCode,
                errorMessage,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE eventsource = 's3.amazonaws.com'
                AND
                json_extract_scalar(requestParameters, '$.bucketName') = '{my_s3_bucket}'                            
                AND
                errorCode = 'AccessDenied'
                """                           
            else:
                my_query_string = f"""
                SELECT
                eventTime, 
                eventName, 
                eventSource, 
                sourceIpAddress, 
                userAgent, 
                awsregion,
                json_extract_scalar(requestParameters, '$.bucketName') as bucketName, 
                json_extract_scalar(requestParameters, '$.key') as objectKey, 
                userIdentity.arn as userArn,
                userIdentity.accountId,
                errorCode,
                errorMessage,
                requestId,
                requestParameters,
                additionaleventdata
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE eventsource = 's3.amazonaws.com'
                AND
                errorCode = 'AccessDenied'
                """   

        case _:
            my_query_string = None


    try:
        if my_query_string: 
            start_query_execution(my_query_string, my_glue_db, my_workgroup_name, my_request_token)
        else:
            logger.info("Nothing to do!")
    except Exception as e:
        logger.error(e)
    else:    
        return {
            'statusCode': 200,
            'body': json.dumps('Successful Invocation!')
        }        
