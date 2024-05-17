import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, udf
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    StructField,
    StructType,
    BooleanType,
)
import json
# Import the load_data function from load_data.py
from load_data import load_data
from column_analyzer import analyze_column

def main():
    appName = "column_analyzer"
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()
    # Set the log level to ERROR to reduce verbosity
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
 
    
    house_df = load_data(spark)
    house_df.printSchema()
    column_to_analyze = None
    #column_to_analyze = 'Postcode'
  
    if column_to_analyze is None:
        # No column is defined, doing the analysis for all columns in the dataframe
        print(f"\nNo column is defined so doing do the analysis for all columns in the dataframe")
        analysis_results = analyze_column(house_df)
    else:
        # Do the analysis for the specific column
        print(f"\n Doing analysis for column {column_to_analyze}")
        analysis_results = analyze_column(house_df, column_to_analyze)
    
    # Print the analysis results for the requested column(s) only
    # print(analysis_results)
    # Convert analysis_results to JSON format
    analysis_results_json = json.dumps(analysis_results, indent=4)

    # Print the JSON-formatted analysis results
    print(f"\nJson formatted output\n")
    print(analysis_results_json)
  
if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print("PySpark code started at:", start_time)
    print("Working on fraud detection...")
    main()
    # Calculate and print the execution time
    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    print("PySpark code finished at:", end_time)
    print("Execution time:", execution_time)


