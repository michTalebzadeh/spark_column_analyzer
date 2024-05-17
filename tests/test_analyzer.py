import sys
import unittest
from pyspark.sql import SparkSession
sys.path.append('/home/hduser/dba/bin/python/spark_column_analyzer/')
sys.path.append('/home/hduser/dba/bin/python/spark_column_analyzer/conf')
sys.path.append('/home/hduser/dba/bin/python/spark_column_analyzer/othermisc')
sys.path.append('/home/hduser/dba/bin/python/spark_column_analyzer/src')
sys.path.append('/home/hduser/dba/bin/python/spark_column_analyzer/tests')

from src.column_analyzer import analyze_column

class TestColumnAnalyzer(unittest.TestCase):
    def setUp(self):
        # Initialize SparkSession for testing
        self.spark = SparkSession.builder \
            .appName("test_analyzer") \
            .master("local[2]") \
            .getOrCreate()
        
        # Create test DataFrame
        self.df = self.spark.createDataFrame([
            (1, "A", None),
            (2, "B", 100),
            (3, "C", 200),
            (4, "D", 300),
            (5, "E", None)
        ], ["ID", "Letter", "Value"])
        
    
    def tearDown(self):
        # Stop SparkSession after testing
        self.spark.stop()

    def test_analyze_column_exists(self):
        # Test whether column exists
        result = analyze_column(self.df, "ID")
        self.assertTrue(result["ID"]["exists"])

    def test_analyze_column_not_exists(self):
        # Test whether column does not exist
        result = analyze_column(self.df, "NotExists")
        self.assertFalse(result["NotExists"]["exists"])

    def test_analyze_column_null_percentage(self):
        # Test null percentage calculation
        result = analyze_column(self.df, "Value")
        self.assertEqual(result["Value"]["null_percentage"], 40.0)

if __name__ == '__main__':
    unittest.main()
