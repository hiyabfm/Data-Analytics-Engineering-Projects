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
# 1️⃣ Extract: Read landing data
# -------------------------------
AccelerometerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node",
)

CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
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
# 2️⃣ Transform: Clean data
# -------------------------------
# Convert to DataFrames
accelerometer_df = AccelerometerLanding_node.toDF()
customer_df = CustomerTrusted_node.toDF()


# keep only accelerometer rows where user exists in customer email
joined_df = accelerometer_df.join(
    customer_df,
    accelerometer_df["user"] == customer_df["email"],
    "inner"
).select(accelerometer_df["*"])  # keep only accelerometer columns

# Convert back to DynamicFrame (optional)
CleanedAccelerometer_node = DynamicFrame.fromDF(joined_df, glueContext, "CleanedAccelerometer_node")

# -------------------------------
# 3️⃣ Load: Write cleaned data to trusted zone (JSON Lines)
# -------------------------------
joined_df.write.mode("overwrite").json("s3://datalake-stedi-bucket/accelerometer/trusted/")

# Commit job
job.commit()
