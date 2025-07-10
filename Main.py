import pyspark.sql.functions as psf
import pyspark.sql as ps
import pyspark.sql.types as pst
from delta import configure_spark_with_delta_pip
import json
import logging
import os
from typing import Optional


def setup_logging(folder_num: int) -> None:
    """
    Configure logging with specified format and file location.
    """
    
    try:
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            level=logging.DEBUG,
            filename=f"logs/schema_{folder_num}.log",
            encoding="utf-8",
            filemode="a",
            format="{asctime} - {levelname} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M",
        )
        
        logging.info(f"Logging setup completed for folder {folder_num}")
    
    except Exception as e:
        print(f"Failed to setup logging: {str(e)}")
        raise


def create_spark_session() -> Optional[ps.SparkSession]:
    """
    Create and configure Spark session with Delta Lake.
    """

    logging.info("Creating Spark session")
    
    try:
        builder = ps.SparkSession.builder.appName("AutoClean_CSV") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        logging.info("Spark session created successfully")
        return spark
    
    except Exception as e:
        logging.error(f"Failed to create Spark session: {str(e)}")
        return None


def load_schema(source_path: str, folder_num: int) -> Optional[pst.StructType]:
    """
    Load and parse the schema from JSON file.
    """
    
    schema_file = f"{source_path}/schema_{folder_num}.json"
    logging.info(f"Loading schema from {schema_file}")
    
    try:
        with open(schema_file, "r") as file_reader:
            schema_json = json.load(file_reader)
        
        schema = pst.StructType.fromJson(schema_json)
        logging.info("Schema loaded successfully")
        return schema
    
    except Exception as e:
        logging.error(f"Failed to load schema: {str(e)}")
        return None


def create_streaming_dataframe(spark: ps.SparkSession, schema: pst.StructType, 
                             source_path: str) -> Optional[ps.DataFrame]:
    """
    Create and configure streaming dataframe.
    """
    
    logging.info("Creating streaming dataframe")
    
    try:
        df = (
            spark
            .readStream
            .format("csv")
            .schema(schema)
            .option("header", "true")
            .load(f"{source_path}/*.csv")
        )
        return df
    
    except Exception as e:
        logging.error(f"Failed to create streaming dataframe: {str(e)}")
        return None


def process_dataframe(df: ps.DataFrame) -> ps.DataFrame:
    """
    Apply transformations to the dataframe.
    """
    logging.info("Processing dataframe")
    
    try:
        # Drop rows with all null values
        df = df.dropna(how="all")
        
        # Remove duplicate rows
        df = df.dropDuplicates()
        
        # Trim whitespace from all columns
        df = df.select([psf.trim(psf.col(c)).alias(c) for c in df.columns])
        
        logging.info("Dataframe processing completed")
        return df
    
    except Exception as e:
        logging.error(f"Failed to process dataframe: {str(e)}")
        raise


def main():
    try:
        # Get folder number
        folder_num = int(input("Enter Folder number to process: "))
        
        # Setup logging
        setup_logging(folder_num)
        
        # Create paths
        source_path = f"Raw_CSVs/schema_{folder_num}"
        delta_path = f"Delta_lake/schema_{folder_num}"
        checkpoint_dir = f"checkpointDir/schema_{folder_num}"
        
        # Initialize Spark
        spark = create_spark_session()
        if not spark:
            logging.error("Failed to initialize Spark. Exiting.")
            return
        
        # Load schema
        schema = load_schema(source_path, folder_num)
        if not schema:
            logging.error("Failed to load schema. Exiting.")
            return
        
        # Create and process streaming dataframe
        df = create_streaming_dataframe(spark, schema, source_path)
        if not df:
            logging.error("Failed to create streaming dataframe. Exiting.")
            return
        
        df = process_dataframe(df)
        
        # Start streaming query
        logging.info("Starting streaming query")
        streaming_query = (
            df
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="5 seconds")
            .start(delta_path)
        )
        
        logging.info("Streaming query started, awaiting termination")
        streaming_query.awaitTermination()
        
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()