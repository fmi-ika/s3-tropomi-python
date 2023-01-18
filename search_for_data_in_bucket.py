import argparse
import json
import re
import datetime #import date, timedelta
import logging
import time
import gzip
import shutil
import os

import boto3

def create_s3_client(s3_config_file):
    """ Create S3 client to be used for downloading files.
                                                                                                  
    Keyword arguments:                                                                                
    s3_config_file -- .json config file containing S3 keys
                                                                                                      
    Return:                                                                                           
    s3 -- S3 client                                                       
    """

    # Read S3 config file
    logger.debug(f'Reading S3 config file {s3_config_file}')
    try:
        with open(s3_config_file, "r") as jsonfile:
            s3_config = json.load(jsonfile)
    except Exception as e:
        logger.error(f'Error while reading the S3 configuration file {s3_config_file}')
        logger.error(e)

    # Create S3 client
    s3 = boto3.client("s3",
                      aws_access_key_id = s3_config['aws_access_key_id'],
                      aws_secret_access_key = s3_config['aws_secret_access_key'],
                      endpoint_url = s3_config['endpoint_url']
    )
    
    return s3


def search_for_pattern(s3, bucket_name, pattern):
    """ Search S3 objects by filename pattern containing date.
                                      
    Keyword arguments:                                                                                
    s3 -- boto3 S3 client
    bucket_name -- S3 bucket name where to search for files
    pattern -- pattern used for searching matching files

    Return:                                                                                           
    True/False -- True if match is found

    """
    found_match = False
    
    # Use paginator to be able to list all objects in bucket containing
    # more than 1000 objects.
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    # Search for files containing pattern
    logger.debug(f'Searching for pattern {pattern} in bucket {bucket_name}')
    for page in pages:
        try:
            for key in page['Contents']:
                if re.search(pattern, key['Key']):
                    return True
        except:
            logger.debug(f'"Contents" keyword not found on S3 page, passing on to next page.')
            pass

    return False


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def main():

    # Create S3 client
    s3_config_file = "conf/tropomi_s3_ro.json"
    s3 = create_s3_client(s3_config_file)

    # Read variable config
    variable_config_file = f"conf/download/{options.var}.json"
    logger.debug(f'Reading config file {variable_config_file}')
    try:
        with open(variable_config_file, "r") as jsonfile:
            variable_config = json.load(jsonfile)
    except Exception as e:
        logger.error(f'Error while reading the configuration file {variable_config_file}')
        logger.error(e)

    start_date = datetime.date(2022, 1, 1)
    end_date = datetime.date(2023, 1, 18)
    for single_date in daterange(start_date, end_date):
        date = single_date.strftime("%Y%m%d")
        
        bucket_name = variable_config["s3"][options.timeperiod]["bucket_name"]
        time = ""
        pattern = variable_config["s3"][options.timeperiod]["obj_name_start"].format(date = options.date, time = time)
    
        # Check if pattern is found in bucket
        if search_for_pattern(s3, bucket_name, pattern):
            print(f"Date {date} found in bucket {bucket_name}")


if __name__ == '__main__':                                                      
    #Parse commandline arguments                                                
    parser = argparse.ArgumentParser()
    parser.add_argument('--var',
                        type = str,                                             
                        default = 'no2-offl',                               
                        help = 'Tropomi variable file to download. Options: no2-offl, so2-offl, co-offl, o3-offl, no2-nrti, so2-nrti, co-nrti, o3-nrti')
    parser.add_argument('--date',
                        type = str,                                             
                        default = '20221101',                               
                        help = 'Date to download from S3.')
    parser.add_argument('--timeperiod',
                        type = str,
                        default = 'day',
                        help = 'Time period to be downloaded. Options: day|month')
    parser.add_argument('--loglevel',
                        default='info',
                        help='minimum severity of logged messages,\
                        options: debug, info, warning, error, critical, default=info')
    
    options = parser.parse_args()

    # Setup logger                                                               
    loglevel_dict={'debug':logging.DEBUG,
                   'info':logging.INFO,
                   'warning':logging.WARNING,
                   'error':logging.ERROR,
                   'critical':logging.CRITICAL}
    logger = logging.getLogger("logger")
    logger.setLevel(loglevel_dict[options.loglevel])
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s | (%(filename)s:%(lineno)d)','%Y-%m-%d %H:%M:%S')
    logging.Formatter.converter = time.gmtime # use utc                                    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    main()
