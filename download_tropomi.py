import argparse
import json
import re
import datetime
import logging
import time

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


def get_files_containing_pattern(s3, bucket_name, pattern, outpath):
    """ Download S3 objects containing given pattern.
                                      
    Keyword arguments:                                                                                
    s3 -- boto3 S3 client
    bucket_name -- S3 bucket name where to search for files
    pattern -- pattern used for searching matching files
    outpath -- local directory where matching files are downloaded to

    """

    # Use paginator to be able to list all objects in bucket containing
    # more than 1000 objects.
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    # Download files containing pattern
    logger.debug(f'Searching for pattern {pattern} in bucket {bucket_name}')
    for page in pages:
        for key in page['Contents']:
            if re.search(pattern, key['Key']):
                logger.debug(f'Downloading file {key["Key"]}')
                local_file = f"{outpath}/{key['Key']}"
                s3.download_file(bucket_name, key['Key'], local_file)      
            

def main():

    # Create S3 client
    s3_config_file = "conf/tropomi_s3.json"
    s3 = create_s3_client(s3_config_file)

    # Read variable config
    variable_config_file = f"conf/{options.var}.json"
    logger.debug(f'Reading config file {variable_config_file}')
    try:
        with open(variable_config_file, "r") as jsonfile:
            variable_config = json.load(jsonfile)
    except Exception as e:
        logger.error(f'Error while reading the configuration file {variable_config_file}')
        logger.error(e)
    
    bucket_name = variable_config["s3"]["bucket_name"]
    time = ""
    pattern = variable_config["s3"]["obj_name_start"].format(date = options.date, time = time)
    
    # Search files including date and download
    outpath = variable_config["local"]["path"]
    get_files_containing_pattern(s3, bucket_name, pattern, outpath)


if __name__ == '__main__':                                                      
    #Parse commandline arguments                                                
    parser = argparse.ArgumentParser()
    parser.add_argument('--var',
                        type = str,                                             
                        default = 'no2-nrti',                               
                        help = 'Tropomi variable file to download. Options: no2, so2, co, o3, no2-nrti, so2-nrti, co-nrti, o3-nrti')
    parser.add_argument('--date',
                        type = str,                                             
                        default = '20221103',                               
                        help = 'Date to download from S3.')
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
