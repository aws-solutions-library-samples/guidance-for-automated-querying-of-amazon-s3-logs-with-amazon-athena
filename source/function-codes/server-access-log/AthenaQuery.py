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
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation='REST.GET.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;
                """  
            else:
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation='REST.GET.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """                                                      

        case "ClientError-4xx":
            logger.info("ClientError-4xx")
            if my_s3_bucket:  
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                httpstatus like '4%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;
                """  
            else:
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE httpstatus like '4%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """                                                      

        case "ServiceError-5xx":
            logger.info("ServiceError-5xx")
            if my_s3_bucket: 
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                httpstatus like '5%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """  
            else:
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE httpstatus like '5%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """                                                      

        case "ObjectDeletion":
            logger.info("ObjectDeletion")
            if my_s3_bucket:  
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation like '%DELETE%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """    
            else:  
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation like '%DELETE%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """     

        case "LifecycleActionStatistics":
            logger.info("LifecycleActionStatistics")
            if my_s3_bucket:
                my_query_string = f"""
                SELECT 'object_delete_marker_created' AS action, COUNT(*) as object_count
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.CREATE.DELETEMARKER' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_incomplete_multipart_aborted' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.DELETE.UPLOAD' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_permanently_deleted' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.EXPIRE.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_INTELLIGENT_TIER' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.TRANSITION_INT.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_GLACIER_INSTANT_RETRIEVAL' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.TRANSITION_GIR.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_ONE_ZONE_IA' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.TRANSITION_ZIA.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_STANDARD_IA' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.TRANSITION_SIA.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_GLACIER_FLEXIBLE_RETRIEVAL' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.TRANSITION.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_GLACIER_DEEP_ARCHIVE' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE bucket_name = '{my_s3_bucket}' AND operation = 'S3.TRANSITION_GDA.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;

                """ 
            else:    
                my_query_string = f"""
                SELECT 'object_delete_marker_created' AS action, COUNT(*) as object_count
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.CREATE.DELETEMARKER' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_incomplete_multipart_aborted' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.DELETE.UPLOAD' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_permanently_deleted' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.EXPIRE.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_INTELLIGENT_TIER' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.TRANSITION_INT.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_GLACIER_INSTANT_RETRIEVAL' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.TRANSITION_GIR.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_ONE_ZONE_IA' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.TRANSITION_ZIA.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_STANDARD_IA' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.TRANSITION_SIA.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_GLACIER_FLEXIBLE_RETRIEVAL' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.TRANSITION.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT 'object_transitioned_to_GLACIER_DEEP_ARCHIVE' AS action, COUNT(*) as object_count 
                FROM "{my_glue_db}"."{my_glue_tbl}" WHERE operation = 'S3.TRANSITION_GDA.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;

                """ 

        
        case "LifecycleAction-Expiration":
            logger.info("LifecycleAction-Expiration")
            if my_s3_bucket: 
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation = 'S3.EXPIRE.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """    
            else:
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation = 'S3.EXPIRE.OBJECT' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """                                                         

        case "LifecycleAction-Transition":
            logger.info("LifecycleAction-Transition")
            if my_s3_bucket: 
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation like 'S3.TRANSITION%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """
            else:
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation like 'S3.TRANSITION%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
                """                                

        case "Latency":
            logger.info("Latency")
            if my_s3_bucket: 
                my_query_string = f"""
                SELECT requestdatetime, turnaroundtime, totaltime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                turnaroundtime != '-' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                ORDER BY CAST(turnaroundtime AS INT) DESC ;
                """  
            else:
                my_query_string = f"""
                SELECT requestdatetime, turnaroundtime, totaltime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE turnaroundtime != '-' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                ORDER BY CAST(turnaroundtime AS INT) DESC;
                """                              

        case "TopTroubleshootingQueries":
            logger.info("TopTroubleshootingQueries")
            if my_s3_bucket:
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                httpstatus like '4%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                            
                UNION ALL
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                httpstatus like '5%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL   
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation like '%DELETE%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL                                             
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation = 'S3.EXPIRE.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                operation like 'S3.TRANSITION%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                 
                
                """ 
            else:    
                my_query_string = f"""
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE bucket_name = '{my_s3_bucket}'
                AND
                httpstatus like '4%' 
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid 
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE httpstatus like '5%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL   
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation like '%DELETE%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL                                             
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation = 'S3.EXPIRE.OBJECT'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd')                             
                UNION ALL
                SELECT requestdatetime, requester, remoteip, operation, httpstatus, bucket_name, key, versionid, useragent, authtype , aclrequired, requestid, hostid
                FROM "{my_glue_db}"."{my_glue_tbl}"
                WHERE operation like 'S3.TRANSITION%'
                AND
                parse_datetime(requestdatetime,'dd/MMM/yyyy:HH:mm:ss Z')
                BETWEEN parse_datetime('{my_query_logs_after}','yyyy-MM-dd')
                AND parse_datetime('{my_query_logs_before}','yyyy-MM-dd') ;                            
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
