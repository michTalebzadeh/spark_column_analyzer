
# Spark Column Analyzer

## Overview

Spark Column Analyzer is a Python package that provides functions for analyzing columns in PySpark DataFrames. It calculates various statistics such as null count, null percentage, distinct count, distinct percentage, min_value, max_value, avg_value and historams
for each column. It also creates a plot and saves it to the directory you are running the code from.

## Installation

You can install Spark Column Analyzer using pip:

```sh
pip install spark-column-analyzer


## Usage

### Analyzing Columns

To analyze columns in a PySpark DataFrame, you can use the `analyze_column` function provided by the package. Here's an example:

```python
from pyspark.sql import SparkSession
from spark_column_analyzer.column_analyzer import analyze_column

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ColumnAnalyzer") \
    .getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([
    (1, "A", None),
    (2, "B", 100),
    (3, "C", 200),
    (4, "D", 300),
    (5, "E", None)
], ["ID", "Letter", "Value"])

# Analyze a specific column
result = analyze_column(df, "Value")
print(result)
Running Tests
To run tests for Spark Column Analyzer, follow these steps:

Clone the repository:
git clone https://github.com/michTalebzadeh/spark_column_analyzer.git
Navigate to the project directory:
cd spark_column_analyzer
Install the dependencies:
pip install -r requirements.txt
Run the tests:
python -m unittest discover tests
Contributing
If you'd like to contribute to Spark Column Analyzer, please open an issue or submit a pull request on GitHub.

License
This project is licensed under the MIT License - see the LICENSE file for details.

vbnet

Feel free to customize this template according to your project's specific details and require

Example

Doing analysis for column Postcode

Json formatted output

{
    "PricePaid": {
        "exists": true,
        "num_rows": 1819,
        "data_type": "int",
        "null_count": 0,
        "null_percentage": 0.0,
        "distinct_count": 1133,
        "distinct_percentage": 62.29,
        "min_value": 10000001,
        "max_value": 448500000,
        "avg_value": 32620476.65,
        "histogram": {
            "bins": [
                10000001.0,
                14216347.144230768,
               448500000.0,

            ],
            "counts": [
                468,
                286,
                258,

            ]
        }
    }
}