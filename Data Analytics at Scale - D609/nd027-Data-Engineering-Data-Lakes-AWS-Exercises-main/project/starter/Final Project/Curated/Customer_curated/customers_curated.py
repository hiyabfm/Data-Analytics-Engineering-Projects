from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import sys
from pyspark.sql.functions import col

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
# 1️⃣ Extract: Read trusted data
# -------------------------------
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node",
)

CustomerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node",
)

# -------------------------------
# 2️⃣ Transform: Filter + Join
# -------------------------------
# Convert DynamicFrames to DataFrames
accelerometer_df = AccelerometerTrusted_node.toDF()
customer_df = CustomerTrusted_node.toDF()

# Filter customers who agreed to share
customer_df = customer_df.filter(col("shareWithResearchAsOfDate").isNotNull())

# Keep only customers that have accelerometer data
customers_with_accel = customer_df.join(
    accelerometer_df.select("user").distinct(),
    customer_df["email"] == accelerometer_df["user"],
    "inner"
).select(customer_df["*"])  # Only keep customer columns

# Convert back to DynamicFrame for Glue output (optional)
CustomerCurated_node = DynamicFrame.fromDF(customers_with_accel, glueContext, "CustomerCurated_node")

# -------------------------------
# 3️⃣ Load: Write curated data to S3 (JSON Lines)
# -------------------------------
customers_with_accel.write.mode("overwrite").json("s3://datalake-stedi-bucket/customer/curated/")

# Commit job
job.commit()
