import argparse
import boto3
import json
import re
import datetime


def create_s3_client(s3_config_file):
    """ Create S3 client to be used for downloading files.
                                                                                                  
    Keyword arguments:                                                                                
    s3_config_file -- .json config file containing S3 keys
                                                                                                      
    Return:                                                                                           
    s3 -- S3 client                                                       
    """

    with open(s3_config_file, "r") as jsonfile:
        s3_config = json.load(jsonfile)

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
    for key in s3.list_objects(Bucket=bucket_name)['Contents']:
        if re.search(pattern, key['Key']):
            print("Downloading file: ", key['Key'])
            local_file = f"{outpath}/{key['Key']}"
            s3.download_file(bucket_name, key['Key'], local_file)
        

def main():

    # Create S3 client
    s3 = create_s3_client(options.s3_config_file)

    # Read variable config
    variable_config_file = f"conf/{options.variable}.json"
    with open(variable_config_file, "r") as jsonfile:
        variable_config = json.load(jsonfile)
    bucket_name = variable_config["s3"]["bucket_name"]

    time = ""
    pattern = obj_name_start.format(date = options.date, time = time)
    
    # Search files including date and download
    outpath = variable_config["local"]["path"]
    download_files_containing_pattern(s3, bucket_name, pattern, outpath)


if __name__ == '__main__':                                                      
    #Parse commandline arguments                                                
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_config_file',
                        type = str,
                        default = 'conf/tropomi_s3.json',                           
                        help = 'S3 config json file')
    parser.add_argument('--variable',
                        type = str,                                             
                        default = 'no2-nrti',                               
                        help = 'Tropomi variable file to download. Options: no2, so2, co, o3, no2-nrti, so2-nrti, co-nrti, o3-nrti')
    parser.add_argument('--date',
                        type = str,                                             
                        default = '20221025',                               
                        help = 'Date to download from S3.')
    options = parser.parse_args()                                               
    main()
