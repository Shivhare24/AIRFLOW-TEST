import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_output_path', 'db_connection_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Connect to Postgres (Using a Glue Connection created in AWS Console)
df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": args['db_connection_name'],
        "dbtable": "indices"
    }
).toDF()

# 2. You can run SparkSQL here if you need complex SELECT logic
df_with_type = df.withColumn("type", lit('index'))
datasource0 = DynamicFrame.fromDF(df_with_type, glueContext, "datasource0")


# 3. Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=datasource0,
    connection_type="s3",
    connection_options={"path": args['s3_output_path']},
    format="csv",
    transformation_ctx="datasink2"
)

job.commit()