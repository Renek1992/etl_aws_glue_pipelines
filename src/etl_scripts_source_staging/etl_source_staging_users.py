#!/usr/bin/env python
# -*- coding utf-8 -*-

import sys 
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'environment', 'sourcedatabase', 'sourcetable', 'destinationpath', 'rawbucket', 'region'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
environment = args['environment']
sourcedatabase = args['sourcedatabase']
sourcetable = args['sourcetable']
destinationpath = args['destinationpath']
rawbucket = args['rawbucket']
region = args['region']


# Table association for script 
table_name = 'users'


# """ETL helper function"""
client = boto3.client('s3', region_name='eu-west-2')

def get_s3_keys():
    response = client.list_objects_v2(
        Bucket = rawbucket
    )

    folder = environment + '/' + sourcedatabase + '/' + table_name + '/'

    keys = []

    for obj in response['Contents']: 
        if obj['Size'] > 0:
            if folder in obj['Key']: 
                keys.append(obj['Key'])
        else: 
            pass

    return keys



# """Existing data clearing"""
s3_count = get_s3_keys()

if len(s3_count) > 0:
    glueContext.purge_s3_path(
        s3_path = f'{destinationpath}/{environment}/{sourcedatabase}/{table_name}',
        options = {'retentionPeriod': 0}    
        )
    print('INFO: bucket clearing successful')
else:
    print('INFO: no bucket clearing necessary')



# """ETL Job tasks"""
datasource0 = glueContext.create_dynamic_frame.from_catalog(
                                database = sourcedatabase, 
                                table_name = sourcetable, 
                                transformation_ctx = "datasource0"
                                )

datasource1 = glueContext.create_dynamic_frame.from_catalog(
                                database = sourcedatabase,
                                table_name = f'{environment}_perlego_prod_db_user_metadata'  
                                )



# """ETL transformation"""
datasource1 = datasource1.drop_fields(['id'])

datasource0_spark_df = datasource0.toDF()
datasource1_spark_df = datasource1.toDF()

spark_join = datasource0_spark_df.join(
                                    datasource1_spark_df, 
                                    datasource0_spark_df.id == datasource1_spark_df.user_id, 
                                    how='left'
                                    )

# datajoin0 = Join.apply(datasource0, datasource1, 'id', 'user_id')

datajoin0 = DynamicFrame.fromDF(
                            dataframe = spark_join,
                            glue_ctx = glueContext, 
                            name = 'datajoin'
                            )
datajoin0.toDF().show(1)


applymapping1 = ApplyMapping.apply(
                                frame = datajoin0,
                                mappings = [
                                    ("id", "int", "id", "int"),
                                    ("fname", "string", "fname", "string"),
                                    ("lname", "string", "lname", "string"),
                                    ("email", "string", "email", "string"),
                                    ("city", "string", "city", "string"),
                                    ("postcode", "string", "postcode", "string"),
                                    ("country", "string", "country", "string"),
                                    ("date_joined", "timestamp", "date_joined", "timestamp"),
                                    ("account_type", "int", "account_type", "int"),
                                    ("organisation_id", "int", "organisation_id", "int"),
                                    ("group_id", "int", "group_id", "int"),
                                    ("industry", "string", "industry", "string"),
                                    ("role", "string", "role", "string"),
                                    ("university", "string", "university", "string"),
                                    ("course", "string", "course", "string"),
                                    ("last_active", "timestamp", "last_active", "timestamp"),
                                    ("geo_location", "string", "geo_location", "string"),
                                    ("marketing_consent", "int", "marketing_conset", "int"),
                                    ("consent_last_updated", "int", "consent_last_updated", "int"),
                                    ("is_deleted", "int", "is_deleted", "int"),
                                    ("user_type", "int", "user_type", "int"),
                                    ("supplier_id", "int", "supplier_id", "int"),
                                    ("publisher_id", "int", "publisher_id", "int")
                                    ],
                                transformation_ctx = "applymapping1"
                                )

resolvechoice2 = ResolveChoice.apply(
                                frame = applymapping1,
                                choice = "make_cols",
                                transformation_ctx = "resolvechoice2"
                                )

dropnullfields3 = DropNullFields.apply(
                                frame = resolvechoice2,
                                transformation_ctx = "dropnullfields3"
                                )

dropnullfields3.toDF().show(1)

if dropnullfields3.toDF().head(1):
        datasink4 = glueContext.write_dynamic_frame.from_options(
                                frame = dropnullfields3,
                                connection_type = "s3",
                                connection_options = {"path": f'{destinationpath}/{environment}/{sourcedatabase}/{table_name}/'},
                                format = "parquet",
                                transformation_ctx = "datasink4"
                                )
else:
    raise Exception("ERROR: Invalid dataframe")

job.commit()