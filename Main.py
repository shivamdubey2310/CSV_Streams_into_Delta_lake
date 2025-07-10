import pyspark.sql.functions as psf
import pyspark.sql as ps
import pyspark.sql.types as pst
from delta import configure_spark_with_delta_pip
import json
import logging



# If it does not work, uncomment it and set your path
# import os
# os.environ["PYSPARK_PYTHON"] = "/home/shiva/venv/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/shiva/venv/bin/python"

# Building spark variable 
builder = ps.SparkSession.builder.appName("AutoClean_CSV") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Asking for the folder to process
folder_num = int(input("Enter Folder number to process"))

# Path of the Raw CSVs
sourcePath = f"Raw_CSVs/schema_{folder_num}"

# Path to store delta_lake
deltaPath = f"Delta_lake/schema_{folder_num}"

# Reading schema
with open(f"{sourcePath}/schema_{folder_num}.json", "r") as fileReader:
    schema_json = json.load(fileReader)

schema = pst.StructType.fromJson(schema_json)

# Creating streaming dataframe
df = (
    spark
    .readStream
    .format("csv")
    .schema(schema)
    .option("header", "true")
    .load(f"{sourcePath}/*.csv")
)

# Directory to save checkpoint Info
checkpointDir = f"checkpointDir/schema_{folder_num}"

# Handling Nulls
df = df.dropna(how="all")  # Drops rows if all values are null

# Handling duplicates 
df = df.dropDuplicates()   # Drops duplicates and returns deduplicated df

# trimming columns
df = df.select([psf.trim(psf.col(c)).alias(c) for c in df.columns])

# Creating streamingQuery
streamingQuery = (
    df
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpointDir)
    .trigger(processingTime = "5 seconds")
    .start(deltaPath)
)

streamingQuery.awaitTermination()