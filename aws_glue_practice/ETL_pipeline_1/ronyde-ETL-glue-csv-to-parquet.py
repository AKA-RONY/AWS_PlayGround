import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit 
import logging
from awsglue.dynamicframe import DynamicFrame

# for CloudWatch logs
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

# Create a handler for CloudWatch
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info('My log message')




args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1707421500874 = glueContext.create_dynamic_frame.from_catalog(
    database="ronydb1",
    table_name="product",
    transformation_ctx="AmazonS3_node1707421500874",
)

logger.info('print schema of S3bucket_node1')
AmazonS3_node1707421500874.printSchema()

#count number of elements in the product table
count = AmazonS3_node1707421500874.count()
logger.info('count for frame is {}'.format(count))




# Script generated for node Change Schema
ChangeSchema_node1707421715064 = ApplyMapping.apply(
    frame=AmazonS3_node1707421500874,
    mappings=[
        ("marketplace", "string", "new_marketplace", "string"),
        ("customer_id", "long", "new_customer_id", "long"),
        ("product_id", "string", "new_product_id", "string"),
        ("seller_id", "string", "new_seller_id", "string"), #need to typecast seller_id since some data elements are incorrect [ seller id cannot be string , but in table we have some invalid data.] we cant simply typecast here since data is present in string as well as in integer, for that we have use resolveChoice method to remove the invalid data elements first
        ("sell_date", "string", "new_sell_date", "string"),
        ("quantity", "long", "new_quantity", "long"),
        ("year", "string", "new_year", "string"),
    ],
    transformation_ctx="ChangeSchema_node1707421715064",
)

# for changing datatypes of column 
#convert those string values to long values using the resolveChoice transform method with a cast:long option:
#This replaces the string values with null values
ResolveChoice_node= ChangeSchema_node1707421715064.resolveChoice(specs = [('new_seller_id','cast:long')], transformation_ctx= "ResolveChoice_node" ) # 'new_seller_id' has number as well as string value so ,casting as long so it will give null values to string data elements
logger.info('print schema of ResolveChoice_node')
ResolveChoice_node.printSchema()

#convert dynamic dataframe into spark dataframe
logger.info('convert dynamic dataframe ResolveChoice_node into spark dataframe')
spark_data_frame=ResolveChoice_node.toDF()

# apply spark where clause
logger.info('filter rows with where  new_seller_id is not null')
spark_data_frame_filter = spark_data_frame.where("new_seller_id is NOT NULL")


# add the new column to the data frame
logger.info('create  new column status with active value')
spark_data_frame_filter= spark_data_frame_filter.withColumn("new_status",lit("Active")) # lit is used for constant values 'Active'. to use this you have to import the lit module
spark_data_frame_filter.show()

logger.info('convert spark dataframe into table view product_view/ so that we can run sql')
spark_data_frame_filter.createOrReplaceTempView("product_view")

logger.info('created temp_view by sparksql from spark dataframe')


product_sql_df= spark.sql("select new_year , count(new_customer_id) as cnt , sum(new_quantity) as  qty from product_view group by new_year")
logger.info('display records after aggregate result')
product_sql_df.show()


#convert the dataframe back to dynamic frame
logger.info('convert spark dataframe to dynamic frame')
dynamic_frame= DynamicFrame.fromDF(product_sql_df,glueContext,"dynamic_frame")




# Script generated for node Amazon S3
AmazonS3_node1707421729651 = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://rony-glue-etl-de-project/output/",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1707421729651",
)

job.commit()
