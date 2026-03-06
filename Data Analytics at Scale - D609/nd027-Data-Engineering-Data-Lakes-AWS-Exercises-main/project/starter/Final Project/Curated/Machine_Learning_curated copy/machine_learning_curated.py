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
        "paths": ["s3://datalake-stedi-bucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainer_node",
)

Accelerometer_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://datalake-stedi-bucket/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_node",
)

# -------------------------------
# 2️⃣ Transform: Join on timestamp
# -------------------------------
# Convert DynamicFrames to DataFrames
steptrainer_df = StepTrainer_node.toDF()
accelerometer_df = Accelerometer_node.toDF()

# Rename to prevent column conflicts
steptrainer_df = steptrainer_df.withColumnRenamed("serialNumber", "st_serialNumber")

# Join on timestamp (sensorReadingTime == timeStamp)
joined_df = steptrainer_df.join(
    accelerometer_df,
    steptrainer_df["sensorReadingTime"] == accelerometer_df["timeStamp"],
    "inner"
)

# Select only the relevant columns
joined_selected_df = joined_df.select(
    col("sensorReadingTime"),
    col("st_serialNumber").alias("serialNumber"),
    col("distanceFromObject"),
    col("user"),
    col("timeStamp"),
    col("x"),
    col("y"),
    col("z")
)

# Convert back to DynamicFrame for Glue
MachineLearningCurated_node = DynamicFrame.fromDF(joined_selected_df, glueContext, "MachineLearningCurated_node")

# -------------------------------
# 3️⃣ Load: Write curated data to S3
# -------------------------------
output_path = "s3://datalake-stedi-bucket/machine_learning/curated/"
joined_selected_df.write.mode("overwrite").json(output_path)

# Commit the job
job.commit()
