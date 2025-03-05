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
my_query_analysis_type = str(os.environ['query_analysis_type'])
my_query_logs_before = str(os.environ['query_logs_before'])
my_query_logs_after = str(os.environ['query_logs_after'])            


my_current_date = datetime.datetime.now().date()

logger.info(f'my_query_logs_before is: {my_query_logs_before}')
logger.info(f'my_query_logs_after is: {my_query_logs_after}')


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

        case "ObjectAccess":
            logger.info("ObjectAccess") 
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
                eventName = 'GetObject'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z';
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
                eventName = 'GetObject'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;
                """                         

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
                AND
                userIdentity.accountId = 'anonymous'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ; 
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
                userIdentity.accountId = 'anonymous'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                AND
                eventname = 'CreateBucket'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                eventname = 'CreateBucket'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                eventname like 'DeleteBucket%'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                eventname like 'DeleteBucket%'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                eventname like 'PutBucket%'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                eventname like 'PutBucket%'
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                (eventname = 'DeleteObject' or eventname like 'DeleteObject%') 
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                (eventname = 'DeleteObject' or eventname like 'DeleteObject%') 
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
                AND
                eventTime BETWEEN '{my_query_logs_after}T00:00:00Z' and '{my_query_logs_before}T00:00:00Z' ;                            
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
