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
AmazonS3_node1716412719982 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://blackdudacity/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1716412719982")

# Script generated for node Amazon S3
AmazonS3_node1716412717223 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AmazonS3_node1716412717223")

# Script generated for node Join
AmazonS3_node1716412719982DF = AmazonS3_node1716412719982.toDF()
AmazonS3_node1716412717223DF = AmazonS3_node1716412717223.toDF()
Join_node1716412730496 = DynamicFrame.fromDF(AmazonS3_node1716412719982DF.join(AmazonS3_node1716412717223DF, (AmazonS3_node1716412719982DF['user'] == AmazonS3_node1716412717223DF['email']), "leftsemi"), glueContext, "Join_node1716412730496")

# Script generated for node accelerometer_trusted_output
accelerometer_trusted_output_node1716413051407 = glueContext.getSink(path="s3://blackdudacity/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_output_node1716413051407")
accelerometer_trusted_output_node1716413051407.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_output_node1716413051407.setFormat("glueparquet", compression="snappy")
accelerometer_trusted_output_node1716413051407.writeFrame(Join_node1716412730496)
job.commit()