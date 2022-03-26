
###
# This file is to used primarily to connect to our various data sources. Here are the steps:
# Athena:
# First, connect with Info = retrieveInfo(). Remember toe make sure that the paths in the below lines (ex: 'athena_keys') is updated with a file of your keys
# Second, use athenaconnect() function to connect to Athena database
# 
# Redshift:
# Connect via Akamai EAA
# required Info = getInfo()
# Use redshiftconnect1() to connect via EAA. Make sure also that you perform your "aws sso login"
###
import json
from pyathena import connect
from pyathena.pandas_cursor import PandasCursor
import sqlalchemy
import pandas as pd
import boto3
from urllib.parse import quote_plus   

#Retrieves login credentials
#Dictionary stores the location of keys, which will be used during data pulls
#Info = retrieveInfo()
def retrieveInfo():
    Info = {
            'athena_keys': 'FILE LOCATION',
            'redshift_credentials_loc': 'FILE LOCATION'
        }
    return Info

#This is to get IM credentials for Redshift
#redshift_user is give by IM role. cluster is defined by URL
def get_redshift_creds(redshift_user, redshift_cluster):

    client = boto3.client('redshift', region_name='region-name-2')
     
    response = client.get_cluster_credentials(DbUser=redshift_user, ClusterIdentifier=redshift_cluster)
     
    password = response['DbPassword']
    user = response['DbUser']
     
    return user, password


def redshiftconnect1(query, Info):
    redshift_info = Info.get('redshift_credentials_loc')
    with open(redshift_info, 'rb') as e:
        redshift_info = e.read()
        redshift_info = json.loads(redshift_info)
    user, password = get_redshift_creds(redshift_info.get('rs_im_role'), redshift_info.get('rs_corp'))
    engine = sqlalchemy.create_engine("postgresql://" + quote_plus(user) + ":" + quote_plus(password) + redshift_info.get('URL'))

    return pd.read_sql_query(query, engine)

#This is to write dataframe into a table
#if no table existed previously, creates a new table
#if table already exists, will insert if possible
def redshift_insertdf1(df, schema_name, table_name, Info):
    redshift_info = Info.get('redshift_credentials_loc')
    with open(redshift_info, 'rb') as e:
        redshift_info = e.read()
        redshift_info = json.loads(redshift_info)
    user, password = get_redshift_creds(redshift_info.get('rs_im_role'), redshift_info.get('rs_corp'))
    engine = sqlalchemy.create_engine("postgresql://" + quote_plus(user) + ":" + quote_plus(password) + redshift_info.get('URL'))
    
    df.to_sql(table_name, engine, index = False, if_exists = 'append',schema = schema_name)

###Below is for Athena
def athenaconnect(region_name, s3_staging_dir, sql_query, Info):
    athena_keys = Info.get('athena_keys')
    with open(athena_keys,'rb') as e:
        athena_keys = e.read()
        athena_keys = json.loads(athena_keys)
    
    cursor = connect(aws_access_key_id=athena_keys.get('aws_access_key_id'),
                   aws_secret_access_key=athena_keys.get('aws_secret_access_key'),
                   s3_staging_dir=s3_staging_dir,
                   region_name=region_name,
                   cursor_class = PandasCursor).cursor()

    return cursor.execute(sql_query).as_pandas()
