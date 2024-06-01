from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when, isnan, expr, min as spark_min, max as spark_max, avg
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    StructField,
    StructType,
    BooleanType,
)
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

def freedman_diaconis_bins(data):
    """
    Calculates the optimal number of bins using the Freedman-Diaconis rule.

    Args:
        data: A list or NumPy array containing the data points.

    Returns:
        The optimal number of bins as an integer.
    """
    q75, q25 = np.percentile(data, [75, 25])
    iqr = q75 - q25
    bin_width = 2 * iqr * len(data) ** (-1 / 3)
    num_bins = int(np.ceil((max(data) - min(data)) / bin_width))
    return num_bins


def analyze_column(dataframe, column_name=None, use_hll=False, num_buckets=None):
    """
    Analyzes columns in a PySpark DataFrame and returns a dictionary with information.

    Args:
        dataframe: The PySpark DataFrame containing the columns.
        column_name: The name of the column to analyze (optional).
        use_hll: Whether to use HyperLogLog for approximate distinct count.
        num_buckets: Number of buckets for histogram calculation (optional).

    Returns:
        A dictionary containing information for each column.
    """

    analysis_results = {}

    # Analyze all columns if column_name is not provided
    if column_name is None:
        for col_name in dataframe.columns:
            try:
                analysis_results[col_name] = analyze_single_column(dataframe, col_name, use_hll, num_buckets)
            except (KeyError, AttributeError):
                # Handle potential errors if column doesn't exist or has incompatible data types
                analysis_results[col_name] = {"exists": False}
    else:
        analysis_results[column_name] = analyze_single_column(dataframe, column_name, use_hll, num_buckets)

    return analysis_results


def analyze_single_column(dataframe, column_name, use_hll=False, num_buckets=None):
    """
    Analyzes a single column in a PySpark DataFrame and returns a dictionary with information.

    Args:
        dataframe: The PySpark DataFrame containing the column.
        column_name: The name of the column to analyze.
        use_hll: Whether to use HyperLogLog for approximate distinct count.
        num_buckets: Number of buckets for histogram calculation (optional).

    Returns:
        A dictionary containing information about the column.
    """

    # Check if column exists
    if column_name not in dataframe.columns:
        return {"exists": False}

    # Get basic information
    num_rows = dataframe.count()
    data_type = dataframe.schema[column_name].dataType.simpleString()

    # Handle null values
    null_count = dataframe.filter((col(column_name).isNull()) | (col(column_name) == "")).count()
    null_percentage = (null_count / num_rows * 100) if num_rows > 0 else 0.0
    
    # Distinct count (exact or approximate)
    if use_hll:
        distinct_count = dataframe.select(expr(f"approx_count_distinct({column_name})")).collect()[0][0]
    else:
        distinct_count = dataframe.select(col(column_name)).distinct().count()

    # Calculate distinct percentage excluding nulls
    distinct_percentage = ((distinct_count - null_count) / num_rows * 100) if num_rows > 0 else 0.0

    # Calculate min, max, avg for numeric columns
    min_value = max_value = avg_value = None

    if isinstance(dataframe.schema[column_name].dataType, (IntegerType, DoubleType)):
        min_value = dataframe.select(spark_min(when(col(column_name).isNotNull(), col(column_name)))).first()[0]
        max_value = dataframe.select(spark_max(when(col(column_name).isNotNull(), col(column_name)))).first()[0]

        avg_value = round(dataframe.select(avg(col(column_name))).collect()[0][0],2)

        # Histogram calculation (example with error handling)
        histogram = None
        if num_rows > 0:
            filtered_df = dataframe.filter(col(column_name).isNotNull())
            non_null_count = filtered_df.count()
            if non_null_count > 0:
                try:
                    data_to_plot = filtered_df.select(col(column_name)).rdd.flatMap(lambda x: x).collect()
                    num_bins = freedman_diaconis_bins(data_to_plot)
                    print(f"\n\nNumber of bins is {num_bins}")

                    # Generate histogram using Pandas and Matplotlib
                    plt.hist(data_to_plot, bins=num_bins, alpha=0.5, label='Original')
                    plt.legend(loc='upper right')
                    plt.xlabel(column_name)
                    plt.ylabel('Frequency')
                    plt.title(f'Distribution of {column_name}')
                    plt.savefig(f'./{column_name}.png')
                    # plt.show()  # Uncomment if you want to display the plot

                    # Prepare histogram data for JSON output
                    hist, bin_edges = np.histogram(data_to_plot, bins=num_bins)
                    histogram = {
                        "bins": bin_edges.tolist(),
                        "counts": hist.tolist()
                    }

                except Exception as e:
                    print(f"Error during histogram generation for {column_name}: {e}")
            else:
                print(f"No non-null values in column {column_name}. Skipping histogram.")
        else:
            print(f"DataFrame is empty for column {column_name}. Skipping histogram.")
  

    return {
        "exists": True,
        "num_rows": num_rows,
        "data_type": data_type,
        "null_count": null_count,
        "null_percentage": round(null_percentage, 2),
        "distinct_count": distinct_count,
        "distinct_percentage": round(distinct_percentage, 2),
        "min_value": min_value,
        "max_value": max_value,
        "avg_value": avg_value,
        "histogram": histogram
    }