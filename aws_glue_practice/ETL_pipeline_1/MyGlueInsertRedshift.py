import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1707546638142 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://rony-glue-etl-de-project/output/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1707546638142",
)

# Script generated for node Change Schema
ChangeSchema_node1707547500217 = ApplyMapping.apply(
    frame=AmazonS3_node1707546638142,
    mappings=[
        ("new_year", "string", "year", "string"),
        ("cnt", "long", "noofcustomer", "bigint"),
        ("qty", "long", "quantity", "bigint"),
    ],
    transformation_ctx="ChangeSchema_node1707547500217",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1707547337099 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1707547500217,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-627947020974-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.product_tab_def",
        "connectionName": "Redshift connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.product_tab_def (year VARCHAR, noofcustomer BIGINT, quantity BIGINT);",
    },
    transformation_ctx="AmazonRedshift_node1707547337099",
)

job.commit()
