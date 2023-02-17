# s3-tropomi-python
Codes for different S3 operations using python boto3. Includes codes for downloading data from S3 bucket, uploading data to S3 bucket and searching data with pattern in S3 bucket. 


## Usage
### Installation
Needed python packages are listed in `environment.yml` file, which can be used to setup conda environment for running the code.

Setup conda environment: `$ conda env create -f environment.yml -n tropomi`

Activate conda environment: `$ conda activate tropomi`

### S3 configurations
S3 configuration files containing S3 keys should be located in `conf/tropomi_s3_ro.json` (read only) and `conf/tropomi_s3_rw.json` (read write) and not be under version control. They should contain keys `aws_access_key_id`, `aws_secret_access_key` and `endpoint_url`.


### Running the download code
Run the code: `$ python download_tropomi.py --var="no2-nrti" --date="20221102"` --timeperiod="day"

Input parameters are:
- `var`: variable name, which is used to find correct configuration .json file
- `date`: date to download from S3
- `timeperiod`: which configuration parameters to use, options day|month
- `loglevel`: how much logging is wanted (optional)

#### Download configurations 
Download configurations are located under `/conf/download`. Configuration .json files are called `variable.json` (e.g. `no2-nrti.json`) Separate configurations can be given to daily / monthly averaged data.

##### Variable S3 configurations
- `bucket_name`: S3 bucket name for given variable
- `obj_name_start`: beginning of filename containing placeholders for {date} and {time}

##### Local configurations
- `path`: local path where the files are downloaded to


### Running the upload code
Run the code: `$ python upload_tropomi.py --var="no2-nrti" --date="20221102"` --timeperiod="day"

Input parameters are:
- `var`: variable name, which is used to find correct configuration .json file
- `date`: date to upload to S3
- `timeperiod`: which configuration parameters to use, options day|month
- `loglevel`: how much logging is wanted (optional)

#### Upload configurations
Upload configurations are located under `/conf/upload`. Configuration .json files are called `variable.json` (e.g. `no2-nrti.json`) Separate configurations can be given to daily / monthly averaged data.

##### S3 configurations
- `bucket_name`: S3 bucket name for given variable

##### Local configurations
- `path`: local path of files to be uploaded
- `datafile`: data filename template for given variable
- `imagefile`: image filename template for given variable


### Running the search for dates in bucket code
Run the code: `$ python search_for_dates_in_bucket.py --var="no2-nrti" --product="l3_day" --startdate="20230101" --enddate="20230131" --datelist_file="test.lst"`

Input parameters are:
- `var`: variable name, which is used to find correct configuration .json file
- `product`: product to search, options are l2|l3_day|l3_month
- `startdate`: first date to search in S3 bucket
- `enddate`: last date to search in S3 bucket
- `datelist_file`: outfile for printed list of days that were found in S3 bucket
- `loglevel`: how much logging is wanted (optional) 

#### Search for dates configurations
Search for dates configurations are located under `/conf/searc`. Configuration .json files are called `variable.json` (e.g. `no2-nrti.json`) Separate configurations can be given to different products (l2, l3_day, l3_month).

- `bucket_name`: S3 bucket name for given variable and product
- `obj_name_start`: template for beginning of filename
