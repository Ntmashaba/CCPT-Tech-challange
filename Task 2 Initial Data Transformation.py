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


def main(Process_name,error_threshold):

    # Create run ID
    uid = uuid.uuid4()
    process_start_time = datetime.datetime.now()   

    # Import sr file
    current_step = 'Import sr file'
    step_start_time = datetime.datetime.now()

    # Open sr file
    sr_data = pd.read_csv('files/sr.csv')
            
    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))
        
    # Import sr file
    current_step = 'Import geojson file'
    step_start_time = datetime.datetime.now()

    
    # Create session
    session = boto3.Session(
        aws_access_key_id=ACCESSKEY, aws_secret_access_key=SECRETKEY, region_name=REGION
    )
    
    s3 = session.resource('s3')
    bucket = s3.Bucket(BUCKET)
    bucket.download_file("city-hex-polygons-8.geojson",'files/city-hex-polygons-8.geojson')

    # Open sr file    
    with open('files/city-hex-polygons-8.geojson','r') as f:
        src_geojson_data = json.loads(f.read())
    
    geojson_all_data = pd.json_normalize(src_geojson_data,record_path = ['features'])
    
    # Extract columns from data frame 
    geojson_data = geojson_all_data[['properties.index','properties.centroid_lat','properties.centroid_lon']]
    
    # rename columns
    geojson_data.columns = geojson_data.columns.str.replace('properties.centroid_lat','Latitude')
    geojson_data.columns = geojson_data.columns.str.replace('properties.centroid_lon','Longitude')
    geojson_data.columns = geojson_data.columns.str.replace('properties.index','h3_level8_index')
        
    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))

    # join data sets
    current_step = 'Join data sets'
    step_start_time = datetime.datetime.now()

    combined_src = pd.merge(sr_data,geojson_data,on=['Latitude','Longitude'],how='left')
      
    combined =  combined_src[['NotificationNumber', 'NotificationType', 'CreationDate', 'CompletionDate', 'Duration', 'CodeGroup', 'Code', 'Open', 'Latitude', 'Longitude', 'SubCouncil2016', 'Wards2016', 'OfficialSuburbs', 'directorate', 'department', 'ModificationTimestamp', 'CompletionTimestamp', 'CreationTimestamp', 'h3_level8_index']] 
    
    error_cnt = combined['h3_level8_index'].isnull().sum()

    # Set nulls to 0
    combined.loc[combined['h3_level8_index'].isnull(),['h3_level8_index']]=0
       
    #Create output file
    combined.to_csv('{}_output.csv'.format(Process_name))
     
    if error_cnt >= error_threshold:   

        end_time = datetime.datetime.now()     

        logging.error('Process Run ID: {}. Process Step: {} = {}. Start time= {}. End time= {} Duration= {}'.format(
                        uid, current_step,error_cnt, process_start_time, end_time, end_time-step_start_time))
                        
        end_time = datetime.datetime.now()
        logging.info('Process Run ID: {}. Process: {}. Start time= {}. End time= {} Duration= {}'.format(
            uid, Process_name, process_start_time, end_time, end_time-process_start_time))

        raise Exception("{} records failed to join".format(error_cnt)) 

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
    process_name = 'Task_2_Initial_Data_Transformation'

    error_threshold = 100
    
    # Begin logging
    logging.basicConfig(filename='{}.log'.format(
        process_name), format='%(name)s - %(levelname)s - %(message)s', level=level)   

    main(process_name,error_threshold)