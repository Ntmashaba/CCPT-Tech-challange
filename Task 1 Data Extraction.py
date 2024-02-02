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
 

def main(Process_name):

    # Create run ID
    uid = uuid.uuid4()
    process_start_time = datetime.datetime.now()   

    # Create session
    session = boto3.Session(
        aws_access_key_id=ACCESSKEY, aws_secret_access_key=SECRETKEY, region_name=REGION
    )
    
    # Open client
    s3_client = session.client('s3')

    # Download H3 resolution 8 file
    current_step = 'Download H3 resolution 8 file'
    step_start_time = datetime.datetime.now()

    # Had a look at the file
    # s3 = session.resource('s3')
    # bucket = s3.Bucket(BUCKET)
    # bucket.download_file("city-hex-polygons-8-10.geojson",'city-hex-polygons-8-10.geojson')
    # reading from json file: https://thetrevorharmon.com/blog/how-to-use-s3-select-to-query-json-in-node-js
        
    # SELECT from src file
    src_H3_resolution_8_data = s3_client.select_object_content(Bucket="cct-ds-code-challenge-input-data",
                                                           Key="city-hex-polygons-8-10.geojson",
                                                           Expression="SELECT * FROM  S3Object[*].features[*] s WHERE s.properties.resolution = 8",
                                                           ExpressionType="SQL",
                                                           InputSerialization={
                                                               "JSON": {"Type": "DOCUMENT"}},
                                                           OutputSerialization={
                                                               "JSON": {"RecordDelimiter": ", "}}
                                                           )
    
    # Extract payload from stream
    extract_H3_resolution_8_data = []
    for event in src_H3_resolution_8_data['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            extract_H3_resolution_8_data.append(records)

    extract_H3_resolution_8_data.sort()        

    # Check whats in here
    # outputfile = json.dumps(extract_H3_resolution_8_data) 
    # with open("output.json","w") as outfile:
    #     outfile.write(outputfile)  
            
    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))


    # Download validation file
    current_step = 'Download validation file'
    step_start_time = datetime.datetime.now()     

    # SELECT from validation file
    src_validation_data = s3_client.select_object_content(Bucket="cct-ds-code-challenge-input-data",
                                                           Key="city-hex-polygons-8.geojson",
                                                           Expression="SELECT * FROM  S3Object[*].features[*] s",
                                                           ExpressionType="SQL",
                                                           InputSerialization={
                                                               "JSON": {"Type": "DOCUMENT"}},
                                                           OutputSerialization={
                                                               "JSON": {"RecordDelimiter": ", "}}
                                                           )  
    
    # Extract payload
    extract_validation_data = []
    for event in src_validation_data['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            extract_validation_data.append(records) 

    extract_validation_data.sort()            

    # Check whats in here
    # outputfile = json.dumps(extract_validation_data) 
    # with open("output.json","w") as outfile:
    #     outfile.write(outputfile)  

    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))


    # Validate file
    current_step = 'Validate file'
    step_start_time = datetime.datetime.now()    
    
    # Check row count
    if  len(extract_H3_resolution_8_data) == len(extract_validation_data):
        logging.info('Process Run ID: {}. Process Step: {}. Extract row count matches validation data.'.format(
        uid, current_step))
        if (extract_H3_resolution_8_data) == (extract_validation_data):
            logging.info('Process Run ID: {}. Process Step: {}. Extract file content matches validation data.'.format(
            uid, current_step))
        else:
            logging.info('Process Run ID: {}. Process Step: {}. Extract file content does not matches validation data!'.format(
            uid, current_step))
    else:
        logging.info('Process Run ID: {}. Process Step: {}. Extract row count does not matches validation data! Extract= {} - Validation= {}'.format(
        uid, current_step,len(extract_H3_resolution_8_data),len(extract_validation_data)))
        # Maybe add a step here to extract the difference to a file. time though
         
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
    process_name = 'Task_1_Data_Extraction'
    
    # Begin logging
    logging.basicConfig(filename='{}.log'.format(
        process_name), format='%(name)s - %(levelname)s - %(message)s', level=level)   

    main(process_name)