import argparse
import json
import logging
import time

import boto3
from botocore.exceptions import ClientError


def create_s3_resource(s3_config_file):
    """ Create S3 resource to be used for policy updates
                                        
    Keyword arguments:
    s3_config_file -- .json config file containing S3 keys
                                                                
    Return:      
    s3 -- S3 resource                                                      
    """

    # Read S3 config file
    logger.debug(f'Reading S3 config file {s3_config_file}')
    try:
        with open(s3_config_file, "r") as jsonfile:
            s3_config = json.load(jsonfile)
    except Exception as e:
        logger.error(f'Error while reading the S3 configuration file {s3_config_file}')
        logger.error(e)

    # Create S3 resource
    s3 = boto3.resource("s3",
                      aws_access_key_id = s3_config['aws_access_key_id'],
                      aws_secret_access_key = s3_config['aws_secret_access_key'],
                      endpoint_url = s3_config['endpoint_url']
    )
    
    return s3


def change_bucket_lifecycle_conf(s3, bucketname):
    """Change bucket lifecycle configuration (e.g. how often are
    old files removed)

    Keyword arguments:
    s3 -- S3 resource
    bucketname -- Which bucket's lifecycle config to edit
    """
    
    lifecycle_configuration = {
        "Rules": [
            { "Expiration":
              { "Days": 14 },
              "Status": "Enabled"
               }]}
    
    logger.debug(f'Changing lifecycle configuration for s3://{bucketname}')
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucketname,
            LifecycleConfiguration=lifecycle_configuration)
    except ClientError as e:
        logger.error(f'Error while changing lifecycle config for s3://{bucketname}')
        logger.error(e)


def main():
        
    # Create S3 resource
    s3_config_file = "conf/tropomi_s3_rw.json"
    s3 = create_s3_resource(s3_config_file)

    bucketname = f"tropomi-{options.bucket}-l3"
        
    # Update bucket lifecycle policy
    #change_bucket_lifecycle_conf(s3, bucketname)
    
    # Get bucket lifecycle policy
    bucket_lifecycle_configuration = s3.BucketLifecycleConfiguration(bucketname)
    print("bucket_lifecycle_configuration: ", bucket_lifecycle_configuration)


if __name__ == '__main__':                                                      
    #Parse commandline arguments                                                
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket',
                        type = str,                                             
                        default = 'no2-nrti',                               
                        help = 'Tropomi variable to upload. Options: no2-nrti, co-nrti, o3-nrti, so2-nrti')
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
