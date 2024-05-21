
# Spark Column Analyzer

## Overview

Spark Column Analyzer is a Python package that provides functions for analyzing columns in PySpark DataFrames. It calculates various statistics such as null count, null percentage, distinct count, and distinct percentage for each column.

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
    "Postcode": {
        "exists": true,
        "num_rows": 93348,
        "data_type": "string",
        "null_count": 21921,
        "null_percentage": 23.48,
        "distinct_count": 38726,
        "distinct_percentage": 41.49
    }
}
