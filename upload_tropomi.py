import argparse
import json
import re
import datetime
import logging
import time
import os
import gzip
import shutil

import boto3
from botocore.exceptions import ClientError


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


def upload_file(s3, filename, bucketname, objectname=None):
    """Upload file to an S3 bucket.

    Keyword arguments:
    s3 -- S3 client
    filename -- File to upload
    bucketname -- Bucket to upload to
    objectname -- S3 object name. If not specified then filename is used
    """

    # If S3 object_name was not specified, use file_name
    if objectname is None:
        objectname = os.path.basename(filename)

    # Upload the file
    logger.debug(f'Uploading file {filename} to s3://{bucketname}')
    try:
        s3.upload_file(filename, bucketname, objectname)
    except ClientError as e:
        logger.error(f'Error while uploading file {filename} to s3://{bucketname}')
        logger.error(e)


def main():

    # Create S3 client
    s3_config_file = "conf/tropomi_s3_rw.json"
    s3 = create_s3_client(s3_config_file)

    # Read variable config
    variable_config_file = f"conf/upload/{options.var}.json"
    logger.debug(f'Reading config file {variable_config_file}')
    try:
        with open(variable_config_file, "r") as jsonfile:
            variable_config = json.load(jsonfile)
    except Exception as e:
        logger.error(f'Error while reading the configuration file {variable_config_file}')
        logger.error(e)
    
    bucket_name = variable_config["s3"][options.timeperiod]["bucket_name"]

    if variable_config["local"][options.timeperiod]["datafile"]:
        # Gzip data file before uploading
        datafile = f'{variable_config["local"][options.timeperiod]["path"]}/{variable_config["local"][options.timeperiod]["datafile"].format(date = options.date)}'
        datafile_gzip = f'{datafile}.gz'
        logger.debug(f'Gzipping file {datafile}')
        try:
            with open(datafile, 'rb') as f_in:
                with gzip.open(datafile_gzip, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            logger.error(f'Error while gzipping file {datafile}')
            logger.error(e)
        # Upload data file to S3
        upload_file(s3, datafile_gzip, bucket_name)
    
    # Upload image file to S3
    imagefile = f'{variable_config["local"][options.timeperiod]["path"]}/{variable_config["local"][options.timeperiod]["imagefile"].format(date = options.date)}'
    upload_file(s3, imagefile, bucket_name)


if __name__ == '__main__':                                                      
    #Parse commandline arguments                                                
    parser = argparse.ArgumentParser()
    parser.add_argument('--var',
                        type = str,                                             
                        default = 'no2-nrti',                               
                        help = 'Tropomi variable to upload. Options: no2-nrti, co-nrti, o3-nrti, so2-nrti')
    parser.add_argument('--date',
                        type = str,                                             
                        default = '20221102',                               
                        help = 'Date to upload to S3.')
    parser.add_argument('--timeperiod',
                        type = str,
                        default = 'day',
                        help = 'Time period to upload. Options: day|month')
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
