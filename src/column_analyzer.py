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

def analyze_column(dataframe, column_name=None):
        """
        Analyzes columns in a PySpark DataFrame and returns a dictionary with information.

        Args:
            dataframe: The PySpark DataFrame containing the columns.
            column_name: The name of the column to analyze (optional).

        Returns:
            A dictionary containing information for each column, including:
                exists (bool): Whether the column exists (True/False).
                num_rows (int): The total number of rows in the DataFrame.
                data_type (str): The data type of the column (string representation).
                null_count (int): The number of null values in the column.
                null_percentage (float): The percentage of null values compared to the number of rows.
                distinct_count (int): The number of distinct values in the column.
                distinct_percentage (float): The percentage of distinct values compared to the number of rows.
        """
        analysis_results = {}

        # Analyze all columns if column_name is not provided
        if column_name is None:
            for col_name in dataframe.columns:
                #print(f"\n\n  column: {col_name}")
                analysis_results[col_name] = analyze_single_column(dataframe, col_name)
                #print(f"{analysis_results[col_name]}")
        else:
            #print(column_name)
            # Analyze the specific column
            analysis_results[column_name] = analyze_single_column(dataframe, column_name)
            #print(f"{analysis_results[column_name]}")
        

        return analysis_results

def analyze_single_column(dataframe, column_name):
        """
        Analyzes a single column in a PySpark DataFrame and returns a dictionary with information.

        Args:
            dataframe: The PySpark DataFrame containing the column.
            column_name: The name of the column to analyze.

        Returns:
            A dictionary containing information about the column, including:
                exists (bool): Whether the column exists (True/False).
                num_rows (int): The total number of rows in the DataFrame.
                data_type (str): The data type of the column (string representation).
                null_count (int): The number of null or empty string values in the column.
                null_percentage (float): The percentage of null or empty string values compared to the number of rows.
                distinct_count (int): The number of distinct values in the column.
                distinct_percentage (float): The percentage of distinct values compared to the number of rows.
        """

        # Validate column existence
        if column_name not in dataframe.columns:
            return {"exists": False}

        # Initialize distinct_count to 0
        distinct_count = 0
        
        # Get basic information
        num_rows = dataframe.count()
        data_type = dataframe.schema[column_name].dataType.simpleString()
        # Count null values and empty strings
        null_count = dataframe.filter((col(column_name).isNull()) | (col(column_name) == "")).count()
        # Calculate the null percentage rounded to two decimal places
        null_percentage = round((null_count / num_rows) * 100, 2) if num_rows > 0 else 0.0
     
        # Count distinct values (considering data types)
        if isinstance(dataframe.schema[column_name].dataType, (StringType, IntegerType, DoubleType)):
            distinct_count = dataframe.select(col(column_name)).distinct().count()
        elif isinstance(dataframe.schema[column_name].dataType, ArrayType):
            # Handle arrays differently (e.g., count distinct elements within each array)
            # You can customize this logic based on your specific needs for arrays
            # This example assumes arrays of integers and counts distinct elements within each
            def distinct_in_array(arr):
                if arr is None:
                    return 0
                return len(set(arr))

        return {
            'exists': True,
            'num_rows': num_rows,
            'data_type': data_type,
            'null_count': null_count,
            'null_percentage': null_percentage,
            'distinct_count': distinct_count,
            'distinct_percentage': round(distinct_count / float(num_rows) * 100, 2) if num_rows > 0 else 0.0
        }

