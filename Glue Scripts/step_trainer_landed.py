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

# Script generated for node customer_trusted
customer_trusted_node1716415684316 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="customer_trusted_node1716415684316")

# Script generated for node step_trainer_landing
step_trainer_landing_node1716415686935 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1716415686935")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1716501441216 = ApplyMapping.apply(frame=customer_trusted_node1716415684316, mappings=[("email", "string", "right_email", "string"), ("phone", "string", "right_phone", "string"), ("birthday", "string", "right_birthday", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforJoin_node1716501441216")

# Script generated for node Join
step_trainer_landing_node1716415686935DF = step_trainer_landing_node1716415686935.toDF()
RenamedkeysforJoin_node1716501441216DF = RenamedkeysforJoin_node1716501441216.toDF()
Join_node1716501400049 = DynamicFrame.fromDF(step_trainer_landing_node1716415686935DF.join(RenamedkeysforJoin_node1716501441216DF, (step_trainer_landing_node1716415686935DF['serialnumber'] == RenamedkeysforJoin_node1716501441216DF['right_serialnumber']), "leftsemi"), glueContext, "Join_node1716501400049")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1716415694094 = glueContext.getSink(path="s3://blackdudacity/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1716415694094")
step_trainer_trusted_node1716415694094.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1716415694094.setFormat("glueparquet", compression="snappy")
step_trainer_trusted_node1716415694094.writeFrame(Join_node1716501400049)
job.commit()
