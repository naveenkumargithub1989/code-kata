from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AnonymizeData") \
    .getOrCreate()

# Function to check if file is empty
def is_file_empty(file_path):
    return os.path.getsize(file_path) == 0

# Define the file path
file_path = 'sample_data_2gb.csv'

# Check if the file is empty
if is_file_empty(file_path):
    raise ValueError("The file is empty.")

# Define the schema for the CSV file
customer_schema = 'first_name string, last_name string, address string, date_of_birth date'

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, schema=customer_schema, header=True)

# Anonymize the data using SHA-256 hashing
anonymized_df = df.withColumn('first_name', sha2(col('first_name'), 256)) \
                  .withColumn('last_name', sha2(col('last_name'), 256)) \
                  .withColumn('address', sha2(col('address'), 256))

# Define the output path
output_path = 'anonymized_data'

# Write the result to a new CSV file
anonymized_df.write.csv(output_path, header=True, mode='overwrite')

# Stop the Spark session
spark.stop()
