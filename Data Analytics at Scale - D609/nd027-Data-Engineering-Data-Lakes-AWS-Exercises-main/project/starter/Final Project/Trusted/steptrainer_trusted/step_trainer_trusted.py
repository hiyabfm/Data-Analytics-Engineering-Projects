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
StepTrainer_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainer_node",
)

CustomerCurated_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node",
)

# -------------------------------
# 2️⃣ Transform: Filter + Join
# -------------------------------
# Convert DynamicFrames to DataFrames
steptrainer_df = StepTrainer_node.toDF()
customer_df = CustomerCurated_node.toDF()

# Filter customers who agreed to share
customer_df = customer_df.filter(col("shareWithResearchAsOfDate").isNotNull())

# Keep only Step Trainer data for customers who agreed to share
steptrainer_curated_df = steptrainer_df.join(
    customer_df.select("serialnumber").distinct(),
    steptrainer_df["serialNumber"] == customer_df["serialnumber"],
    "inner"
).select(steptrainer_df["*"])  # Only keep Step Trainer columns

# Convert back to DynamicFrame for Glue output (optional)
StepTrainerCurated_node = DynamicFrame.fromDF(steptrainer_curated_df, glueContext, "StepTrainerCurated_node")

# -------------------------------
# 3️⃣ Load: Write curated Step Trainer data to S3
# -------------------------------
steptrainer_curated_df.write.mode("overwrite").json("s3://datalake-stedi-bucket/step_trainer/trusted/")

# Commit job
job.commit()
