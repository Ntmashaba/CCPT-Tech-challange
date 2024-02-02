import logging
import datetime
import uuid
import pandas as pd
from cryptography.fernet import Fernet
import h3

def change_resolution(data,res):
    if h3.h3_is_valid(data):
        data2 = h3.h3_to_geo(data)
        data3 = h3.geo_to_h3(data2[0],data2[1],res)
        return data3 
    else: return data

def main(Process_name):

    # Create run ID
    uid = uuid.uuid4()
    process_start_time = datetime.datetime.now()   

    # Open sr hex file
    current_step = 'Open sr hex file'
    step_start_time = datetime.datetime.now()
    
     # Open sr hex file
    srhex_data = pd.read_csv('files/sr_hex.csv')
  
    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))
    
    
    # Obfuscate text
    current_step = 'Obfuscate text'
    step_start_time = datetime.datetime.now()
        
    # # generate a key for encryptio and decryption
    # key = Fernet.generate_key()

    # # Instance the Fernet class with the key
    # fernet = Fernet(key)
    
    # Hide pii info
    srhex_data["SubCouncil2016"] = "xxxx-xxxx" #fernet.encrypt(str(srhex_data["SubCouncil2016"]).encode())
    srhex_data["Wards2016"] = "xxxx-xxxx"
    srhex_data["OfficialSuburbs"] = "xxxx-xxxx"
    srhex_data["Latitude"] = "xxxx-xxxx"
    srhex_data["Longitude"] = "xxxx-xxxx"

    # Change resolution
    srhex_data["h3_level8_index"] = change_resolution(srhex_data["h3_level8_index"],5)

    # Log time
    end_time = datetime.datetime.now()
    logging.info('Process Run ID: {}. Process Step: {}. Start time= {}. End time= {} Duration= {}'.format(
        uid, current_step, process_start_time, end_time, end_time-step_start_time))
     
    # Create Obfuscate_sr_hex_data csv
    current_step = 'Create Obfuscate_sr_hex_data csv'
    step_start_time = datetime.datetime.now()

    srhex_data.to_csv("files/obfuscate_sr_hex_data.csv")

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
    process_name = 'Task 5_Further_Data_Transformations'
    
    # Begin logging
    logging.basicConfig(filename='{}.log'.format(
        process_name), format='%(name)s - %(levelname)s - %(message)s', level=level)   

    main(process_name)