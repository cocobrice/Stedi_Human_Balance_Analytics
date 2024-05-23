import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1716497756405 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AmazonS3_node1716497756405")

# Script generated for node Amazon S3
AmazonS3_node1716497811300 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1716497811300")

# Script generated for node Join
AmazonS3_node1716497756405DF = AmazonS3_node1716497756405.toDF()
AmazonS3_node1716497811300DF = AmazonS3_node1716497811300.toDF()
Join_node1716497793434 = DynamicFrame.fromDF(AmazonS3_node1716497756405DF.join(AmazonS3_node1716497811300DF, (AmazonS3_node1716497756405DF['email'] == AmazonS3_node1716497811300DF['user']), "leftsemi"), glueContext, "Join_node1716497793434")

# Script generated for node Amazon S3
AmazonS3_node1716497943723 = glueContext.getSink(path="s3://blackdudacity/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1716497943723")
AmazonS3_node1716497943723.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
AmazonS3_node1716497943723.setFormat("glueparquet", compression="snappy")
AmazonS3_node1716497943723.writeFrame(Join_node1716497793434)
job.commit()
