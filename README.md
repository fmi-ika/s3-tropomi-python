# s3-tropomi-python
Download TROPOMI data from S3 bucket using python boto3.

## Usage
### Installation
Needed python packages are listed in `environment.yml` file, which can be used to setup conda environment for running the code.

Setup conda environment: `$ conda env create -f environment.yml -n tropomi`

Activate conda environment: `$ conda activate tropomi`

### Running

Run the code: `$ python download_tropomi.py --var="no2-nrti" --date="20221102"`

Input parameters are:
- `var`: variable name, which is used to find correct configuration .json file
- `date`: date to download from S3

### Configurations
S3 configuration files containing S3 keys should be located in `conf/tropomi_s3_ro.json` (read only) and `conf/tropomi_s3_rw.json` (read write) and not be under version control. They should contain keys `aws_access_key_id`, `aws_secret_access_key` and `endpoint_url`.

Configurations for each variable are located in `/conf/download` and `/conf/upload` directories. Configuration .json files are called `variable.json` (e.g. `no2-nrti.json`).

#### Download configurations 

##### Variable S3 configurations
- `bucket_name`: S3 bucket name for given variable
- `obj_name_start`: beginning of filename containing placeholders for {date} and {time}

##### Local configurations
- `path`: local path where the files are downloaded to

#### Upload configurations

To be added

