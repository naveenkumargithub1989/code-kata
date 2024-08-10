import pandas as pd
import faker
import csv
import os

# Create a Faker instance
fake = faker.Faker()

# Function to generate sample data
def generate_data(num_rows):
    for _ in range(num_rows):
        yield {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "address": fake.address().replace("\n", ", "),  # Remove new lines for a single line address
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat()
        }

# Calculate the number of records needed to approximate a 2 GB file
estimated_record_size = 105  # Average bytes per record
target_file_size = 2 * 1024 ** 3  # 2 GB
num_records = target_file_size // estimated_record_size

# Output file path
output_file = 'sample_data_2gb.csv'

# Generate and save the data
with open(output_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["first_name", "last_name", "address", "date_of_birth"])
    writer.writeheader()
    for record in generate_data(num_records):
        writer.writerow(record)

# Print final file size
print(f"Generated file size: {os.path.getsize(output_file) / (1024 ** 3):.2f} GB")
