import awswrangler as wr
import pandas as pd
import boto3
from boto3 import client
from datetime import date,datetime, timedelta
#### Defining Constant Variables ####
AWS_ACCESS_KEY_ID = 'AKIAW7Z4UEFXESZ5WLUK'
AWS_SECRET_ACCESS_KEY = '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'
region_name='us-east-1'
bucket_name = 'my-amadeus'

#### Generating Today's date ####
today = date.today()
current_date = today.strftime("%d_%m_%Y")
#### Generating The Date Of Before 7 Days  ####
last_7 =  (datetime.now() + timedelta(days=-7))
current_date_minus_7 = last_7.strftime("%d_%m_%Y")
#### Creating S3 client using boto3 ####
client = client('s3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
#### Creating S3 session using boto3 ####
session = boto3.Session(
         aws_access_key_id=AWS_ACCESS_KEY_ID,
         aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3 = session.resource('s3')
my_bucket = s3.Bucket(bucket_name)
#### Collecting all of the file paths of the day before 7 days requests ####
paths =[]
for my_bucket_object in my_bucket.objects.filter(Prefix="request/{}/".format(current_date_minus_7)):
    paths.append(my_bucket_object.key)

#### Merging all of jsons into one list ####
daily_json_list = []
for path in paths:
    result = client.get_object(Bucket=bucket_name, Key=path)
    text = result["Body"].read().decode()
    daily_json_list.append(text)

#### Checking if there is data in S3 ####
def check_data(df):
    if len(df)==0:
        return False
    else: return True

def upload_to_archive(df):
    #### Converting the list with the data to Parquet file ####
    daily_json_list_df_to_parquet = pd.DataFrame(daily_json_list)
    daily_json_list_df_to_parquet.rename(columns={0: 'data'}, inplace=True)
    daily_json_list_df_to_parquet.to_parquet('df.parquet.gzip',
                                             compression='gzip')

    #### Uploading Parquet to S3 Bucket ####
    wr.s3.to_parquet(
        df=daily_json_list_df_to_parquet,
        path=f's3://my-amadeus/request/Archive/Archive-{current_date_minus_7}.parquet',
        boto3_session=session)
    print(("Uploaded Data to archive"))

    #### Deleting last 7 days folder ####
    my_bucket.objects.filter(Prefix="request/{}/".format(current_date_minus_7)).delete()
    print(("Deleted Data Folder"))

def main(daily_json_list):
    if check_data(daily_json_list):
        upload_to_archive(daily_json_list)
    else:
        print("No Data")
        return 200

main(daily_json_list)


