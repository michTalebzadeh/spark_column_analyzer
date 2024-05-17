from pyspark.sql import SparkSession

# Load data from Hive tables
def load_data(spark):
        DSDB = "DS"
        tableName = "ocod_full_2024_03" # downloaded fraud table
        fullyQualifiedTableName = f"{DSDB}.{tableName}"
        if spark.sql(f"SHOW TABLES IN {DSDB} LIKE '{tableName}'").count() == 1:
           spark.sql(f"ANALYZE TABLE {fullyQualifiedTableName} COMPUTE STATISTICS")
           rows = spark.sql(f"SELECT COUNT(1) FROM {fullyQualifiedTableName}").collect()[0][0]
           print(f"\nTotal number of rows in source table {fullyQualifiedTableName} is {rows}\n")
        else:
           print(f"No such table {fullyQualifiedTableName}")
           sys.exit(1)
        
        # create a dataframe from the loaded data
        house_df = spark.sql(f"SELECT * FROM {fullyQualifiedTableName}")
        return house_df
