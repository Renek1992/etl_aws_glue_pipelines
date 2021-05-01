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
table_name = 'interests'


# """ETL helper functions"""



# """Existing data clearing"""



# """ETL Job tasks"""
datasource0  = glueContext.create_dynamic_frame.from_catalog(
                                    database = sourcedatabase,
                                    table_name = sourcetable,
                                    transformation_ctx = "datasource0"
                                    )


# """ETL transformation"""


applymapping1 = ApplyMapping.apply(
                                frame = datasource0,
                                mappings = [
                                    ("id", "int", "id", "int"),
                                    ("user_id", "int", "user_id", "int"),
                                    ("subject_id", "int", "subject_id", "int"),
                                    ("subject_name", "string", "subject_name", "string")
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



