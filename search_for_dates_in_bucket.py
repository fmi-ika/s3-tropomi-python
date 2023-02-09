import argparse
import json
import re
import datetime
from dateutil.relativedelta import relativedelta
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


def daterange(start_date, end_date, product = "l2", reverse = False):
    """ Generator function for getting a list of dates

    Keyword arguments:                                                                                
    start_date -- First date in list
    end_date -- Last date in list
    product -- Options: l2 | l3_day | l3_month
    reverse -- if True, list dates from last to first

    Yield:
    list of dates from first to last

    """
    if product == "l2" or product == "l3_day":
        for n in range(int((end_date - start_date).days)+1):
            if not reverse:
                yield (start_date + datetime.timedelta(n)).strftime("%Y%m%d")
            else:
                yield (end_date - datetime.timedelta(n)).strftime("%Y%m%d")
    elif product == "l3_month":
        delta = relativedelta(end_date, start_date)
        n_months = delta.months + (delta.years * 12)
        for n in range(n_months+1):
            print(n)
            if not reverse:
                yield (start_date + relativedelta(months = n)).strftime("%Y%m")
            else:
                yield (end_date - relativedelta(months = n)).strftime("%Y%m")
    else:
        raise ValueError('Give valid product name.')


def main():

    # Create S3 client
    s3_config_file = "conf/tropomi_s3_ro.json"
    s3 = create_s3_client(s3_config_file)

    # Read variable config
    variable_config_file = f"conf/search/{options.var}.json"
    logger.debug(f'Reading config file {variable_config_file}')
    try:
        with open(variable_config_file, "r") as jsonfile:
            variable_config = json.load(jsonfile)
    except Exception as e:
        logger.error(f'Error while reading the configuration file {variable_config_file}')
        logger.error(e)

    # Make datetime objects
    if options.product == "l3_month":
        start_date = datetime.datetime.strptime(options.start_date,'%Y%m').date()
        end_date = datetime.datetime.strptime(options.end_date,'%Y%m').date()
    else:
        start_date = datetime.datetime.strptime(options.start_date,'%Y%m%d').date()
        end_date = datetime.datetime.strptime(options.end_date,'%Y%m%d').date()

    # Loop through dates and check if they are found in bucket
    with open(options.datelist_file, 'w') as outfile_datelist:
        for single_date in daterange(start_date, end_date, product = options.product, reverse = True):
            bucket_name = variable_config[options.product]["bucket_name"]
            pattern = variable_config[options.product]["obj_name_start"].format(date = single_date)
    
            # Check if pattern is found in bucket and write found dates in file
            if search_for_pattern(s3, bucket_name, pattern):
                print(f"{single_date}")
                outfile_datelist.write(f"{single_date}\n")

if __name__ == '__main__':                                                      
    #Parse commandline arguments                                                
    parser = argparse.ArgumentParser()
    parser.add_argument('--var',
                        type = str,                                             
                        default = 'no2-offl',                               
                        help = 'Tropomi variable file to download. Options: no2-offl, so2-offl, co-offl, o3-offl, no2-nrti, so2-nrti, co-nrti, o3-nrti')
    parser.add_argument('--product',
                        type = str,
                        default = 'l3_month',
                        help = 'Product to search. Options: l2|l3_day|l3_month')
    parser.add_argument('--start_date',
                        type = str,                          
                        default = '202201',                               
                        help = 'First date to search in bucket.')
    parser.add_argument('--end_date',
                        type = str,                                             
                        default = '202301',                               
                        help = 'Last date to search in bucket.')
    parser.add_argument('--datelist_file',
                        type = str,                                             
                        default = 's3_data_newest_dates.txt',                               
                        help = 'Outfile for printed list of days.')    
    parser.add_argument('--loglevel',
                        default='debug',
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
