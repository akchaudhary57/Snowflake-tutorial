create database TEST;

truncate table TEST.PUBLIC.testdata;

/*
CREATE or replace STORAGE INTEGRATION s3_int 
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::763122415178:role/snowflakerole'
   STORAGE_AWS_OBJECT_ACL = 'bucket-owner-full-control' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://new-bucket-b204b747/')
  */
  
 DESC STORAGE INTEGRATION s3_int
 
 grant create stage on schema public to role ACCOUNTADMIN;

 grant usage on integration s3_int to role ACCOUNTADMIN;

# Steps

#1.Create a file format objects.
use schema TEST.public;

create or replace file format mycsvformat
type = 'CSV'
field_delimiter = ','
skip_header = 1;

#2. Create a staging objects.

create or replace stage my_csv_stage
storage_integration = s3_int
url = 's3://new-bucket-b204b747/'
file_format = mycsvformat;

#3 Create pipe

create pipe TEST.PUBLIC.mypipe auto_ingest=true as
copy into TEST.PUBLIC.testdata from @TEST.PUBLIC.my_csv_stage
file_format = (type = 'CSV');

show pipes

#drop pipe mypipe;

#3. Create table

create or replace table TEST.PUBLIC.testdata
(
UUSERID	varchar,
ARTICLEID	varchar,
CLICK	varchar,
TIMEDURATION varchar,	
VISITDATE	varchar,
VISITTIME	varchar,
CATEGORIES varchar
);

#4. Copy data into the target table

copy into TEST.PUBLIC.testdata
from @my_csv_stage/NewsData1.csv
on_error = 'skip_file';


select count(*) from 
TEST.PUBLIC.testdata;





