import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Landing
CustomerLanding_node1716333145338 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1716333145338")

# Script generated for node SQL Query
SqlQuery260 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1716499887998 = sparkSqlQuery(glueContext, query = SqlQuery260, mapping = {"myDataSource":CustomerLanding_node1716333145338}, transformation_ctx = "SQLQuery_node1716499887998")

# Script generated for node Customer Trusted
CustomerTrusted_node1716333671230 = glueContext.getSink(path="s3://blackdudacity/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1716333671230")
CustomerTrusted_node1716333671230.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
CustomerTrusted_node1716333671230.setFormat("glueparquet", compression="snappy")
CustomerTrusted_node1716333671230.writeFrame(SQLQuery_node1716499887998)
job.commit()
