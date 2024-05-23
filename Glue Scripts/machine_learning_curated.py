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

# Script generated for node Accelorometer Trusted
AccelorometerTrusted_node1716501900534 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelorometerTrusted_node1716501900534")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1716501938491 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1716501938491")

# Script generated for node Join
StepTrainerTrusted_node1716501938491DF = StepTrainerTrusted_node1716501938491.toDF()
AccelorometerTrusted_node1716501900534DF = AccelorometerTrusted_node1716501900534.toDF()
Join_node1716501972023 = DynamicFrame.fromDF(StepTrainerTrusted_node1716501938491DF.join(AccelorometerTrusted_node1716501900534DF, (StepTrainerTrusted_node1716501938491DF['sensorreadingtime'] == AccelorometerTrusted_node1716501900534DF['timestamp']), "left"), glueContext, "Join_node1716501972023")

# Script generated for node machine_learning_curated
machine_learning_curated_node1716502195702 = glueContext.getSink(path="s3://blackdudacity/step_trainer/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1716502195702")
machine_learning_curated_node1716502195702.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1716502195702.setFormat("glueparquet", compression="snappy")
machine_learning_curated_node1716502195702.writeFrame(Join_node1716501972023)
job.commit()
