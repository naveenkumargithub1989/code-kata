import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col
import os
import pandas as pd

# Function to create sample data
def create_sample_data(file_path):
    """
    Create a CSV file with sample data using pandas.

    Args:
        file_path (str): Path where the sample data CSV will be saved.
    """
    data = {
        "first_name": ["John", "Jane"],
        "last_name": ["Doe", "Smith"],
        "address": ["123 Elm St", "456 Oak St"],
        "date_of_birth": ["1990-01-01", "1985-05-05"]
    }
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False, header=True)

# Test class for the data anonymization process
class TestAnonymizeData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Set up a Spark session before any tests run.
        """
        cls.spark = SparkSession.builder.master("local").appName("TestAnonymizeData").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        Stop the Spark session after all tests have been run.
        """
        cls.spark.stop()

    def test_anonymize_data(self):
        """
        Test the anonymization of data by hashing columns with SHA-256.
        """
        # Create a sample CSV file
        file_path = 'sample_data.csv'
        create_sample_data(file_path)

        # Define schema for the CSV file
        customer_schema = 'first_name string, last_name string, address string, date_of_birth date'

        # Read the CSV file into a DataFrame
        df = self.spark.read.csv(file_path, schema=customer_schema, header=True)

        # Anonymize the data using SHA-256 hashing
        anonymized_df = df.withColumn('first_name', sha2(col('first_name'), 256)) \
                          .withColumn('last_name', sha2(col('last_name'), 256)) \
                          .withColumn('address', sha2(col('address'), 256))

        # Check that the columns are hashed
        first_row = anonymized_df.collect()[0]
        self.assertNotEqual(first_row['first_name'], 'John')
        self.assertNotEqual(first_row['last_name'], 'Doe')
        self.assertNotEqual(first_row['address'], '123 Elm St')

        os.remove(file_path)

    def test_write_output(self):
        """
        Test writing the anonymized data to an output CSV file.
        """
        # Create a sample CSV file
        file_path = 'sample_data.csv'
        create_sample_data(file_path)

        # Define schema for the CSV file
        customer_schema = 'first_name string, last_name string, address string, date_of_birth date'

        # Read the CSV file into a DataFrame
        df = self.spark.read.csv(file_path, schema=customer_schema, header=True)

        # Anonymize the data using SHA-256 hashing
        anonymized_df = df.withColumn('first_name', sha2(col('first_name'), 256)) \
                          .withColumn('last_name', sha2(col('last_name'), 256)) \
                          .withColumn('address', sha2(col('address'), 256))

        # Write the result to a new CSV file
        output_path = 'anonymized_data'
        anonymized_df.write.csv(output_path, header=True, mode='overwrite')

        # Check if the output directory is created
        self.assertTrue(os.path.exists(output_path))

        # Clean up the output directory
        for file in os.listdir(output_path):
            os.remove(os.path.join(output_path, file))
        os.rmdir(output_path)
        os.remove(file_path)

# Helper function for checking file emptiness
def is_file_empty(file_path):
    """
    Check if the given file is empty.

    Args:
        file_path (str): Path to the file to be checked.

    Returns:
        bool: True if the file is empty, False otherwise.
    """
    return os.path.getsize(file_path) == 0

if __name__ == '__main__':
    unittest.main()
