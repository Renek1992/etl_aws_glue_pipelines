#!/usr/bin/env python
# -*- coding utf-8 -*-

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: []
args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'environment', 'sourcedatabase', 'sourcetable', 'region'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
environment = args['environment']
sourcedatabase = args['sourcedatabase']
sourcetable = args['sourcetable']
region = args['region']


# Table association for script 
table_name = 'users'


# """ETL helper functions"""



# """Existing data clearing"""



# """ETL Job tasks"""
datasource0 = glueContext.create_dynamic_frame.from_catalog(
                                database = sourcedatabase,
                                table_name = sourcetable,
                                transformation_ctx = "datasource0"
                                )


applymapping1 = ApplyMapping.apply(
                                frame = datasource0,
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
                                transformation_ctx  = "resovechoice2"
                                )

dropnullfields3 = DropNullFields.apply(
                                frame = resolvechoice2,
                                transformation_ctx = "dropnullfields3"
                                )

if dropnullfields3.toDF().head(1):
        datasink4 =  glueContext.write_dynamic_frame.from_jdbc_conf(
            frame = dropnullfields3, 
            catalog_connection = "GlueRedshiftConnection",
            connection_options = {"preactions": f"TRUNCATE TABLE {environment}.{table_name};", "dbtable": f"{environment}.{table_name}", "database": "perlego-analytics-prod"},
            redshift_tmp_dir = args["TempDir"],
            transformation_ctx = "datasink4"
        )
else:
    raise Exception("ERROR: Invalid dataframe")


job.commit()
