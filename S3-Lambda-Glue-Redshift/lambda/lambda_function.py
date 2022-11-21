import json
import boto3
import time


def lambda_handler(event, context):
    client = boto3.client("glue")
    print("event collected is {}".format(event))

    table_check = ""
    previous_job = {}
    status = ""
    for record in event['Records'] :
        s3_bucket = record['s3']['bucket']['name']
        s3_key = record['s3']['object']['key']
        
        print("Bucket name is: {}".format(s3_bucket))
        print("Bucket key name is: {}".format(s3_key))
        from_path = "s3://{}/{}".format(s3_bucket, s3_key)
        print("from path: {}".format(from_path))
        
        arguments = {
            '--from_path': from_path
        }
        table = s3_key.split("/")[2]
        

        
        if table != table_check:
            runId = client.start_job_run(JobName="ETL_Job", Arguments=arguments)
            previous_job[table] = runId
            
            
        else:
            while(status != "SUCCEEDED"):
                status = client.get_job_run(JobName="ETL_Job", RunId=previous_job[table]['JobRunId'])['JobRun']['JobRunState']
                print(status)
            runId = client.start_job_run(JobName="ETL_Job", Arguments=arguments)
            previous_job[table] = runId
        
        table_check = table
        
        
    return {
        'statusCode': 200,
        'body': json.dumps('ETL Job Trigger!' )
    }
        
