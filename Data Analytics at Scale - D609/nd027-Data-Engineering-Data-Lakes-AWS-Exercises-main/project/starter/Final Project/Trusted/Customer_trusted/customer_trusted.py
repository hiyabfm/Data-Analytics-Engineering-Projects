from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import sys

# Get the job name from arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------
# 1️⃣ Extract: Read customer landing data
# -------------------------------
CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node",
)

# -------------------------------
# 2️⃣ Transform: Clean data and get unique customers
# -------------------------------
customer_df = CustomerLanding_node.toDF()

# Keep the earliest registrationDate per email
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

window_spec = Window.partitionBy("email").orderBy(col("registrationDate").asc())
customer_df = customer_df.withColumn("row_num", row_number().over(window_spec)) \
                         .filter(col("row_num") == 1) \
                         .drop("row_num")

# Keep only records where shareWithResearchAsOfDate is NOT NULL
customer_df = customer_df.filter(col("shareWithResearchAsOfDate").isNotNull())

# Convert back to DynamicFrame
CleanedCustomer_node = DynamicFrame.fromDF(customer_df, glueContext, "CleanedCustomer_node")

# -------------------------------
# 3️⃣ Load: Write cleaned data to trusted zone as JSON
# -------------------------------
customer_df.write.mode("overwrite").json("s3://datalake-stedi-bucket/customer/trusted/")


# Commit the job
job.commit()
