import logging
import datetime
import uuid
import json
import boto3
import pandas as pd

# Get config
config_file = json.load(open('ds_code_challenge_config.json', 'r'))
config_data = config_file['s3']

SECRETKEY = config_data['secret_key']
ACCESSKEY = config_data['access_key']
REGION = config_data['region']
BUCKET = config_data['bucket']
from botocore.exceptions import ClientError
 
# Not sure which bucket to use here
# def get_buckets_client():

#    session = boto3.session.Session(aws_access_key_id=ACCESSKEY, aws_secret_access_key=SECRETKEY, region_name=REGION)

#    s3_client = session.client('s3')

#    try:
#         response = s3_client.list_buckets()
#         buckets =[]
#         for bucket in response['Buckets']:
#             buckets += {bucket["Name"]}

#    except ClientError:
#         print("Couldn't get buckets.")
#         raise
#    else:
#         return buckets

# print(get_buckets_client()) 

def main(Process_name):

    # Create run ID
    uid = uuid.uuid4()
    process_start_time = datetime.datetime.now()   

    # Create session
    session = boto3.Session(
        aws_access_key_id=ACCESSKEY, aws_secret_access_key=SECRETKEY, region_name=REGION
    )
    

    # Upload obfuscate_sr_hex_data file
    current_step = 'Upload obfuscate_sr_hex_data file'
    step_start_time = datetime.datetime.now()

    # Send file
    s3 = session.resource('s3')
    s3.meta.client.upload_file('files\obfuscate_sr_hex_data.csv',"cct-ds-code-challenge-output-data",'NMashabaOutput.csv')
         
    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))

    # Process Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, Process_name, process_start_time, end_time, end_time-process_start_time))
             

if __name__ == '__main__':
    
    # Set log level
    level = logging.INFO  # Maybe change this to a config later
    
    # Set process name
    process_name = 'Task_6_Data_Loading_Tasks'
    
    # Begin logging
    logging.basicConfig(filename='{}.log'.format(
        process_name), format='%(name)s - %(levelname)s - %(message)s', level=level)   

    main(process_name)