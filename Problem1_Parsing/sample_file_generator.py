# Function to generate realistic customer records
from faker import Faker

# Initialize Faker
fake = Faker()

def generate_customer_record():
    id_field = str(fake.random_int(min=10000, max=99999)).ljust(5)
    name = fake.name().ljust(12)
    age = str(fake.random_int(min=18, max=99)).zfill(3)
    gender = fake.random_element(elements=('M', 'F')).ljust(2)
    email = fake.email().ljust(13)
    phone = fake.msisdn().ljust(7)[:7]  # generate a 7-character phone number
    city = fake.city().ljust(10)
    state = fake.state().ljust(13)
    address = fake.street_address().ljust(20)
    country = fake.country().ljust(13)

    # Concatenate all fields to create a fixed-width record
    record = f"{id_field}{name[:12]}{age}{gender}{email[:13]}{phone}{city[:10]}{state[:13]}{address[:20]}{country[:13]}\n"
    return record


# Generate 10,000 records and write them to the file in batches
batch_size = 1000
num_records = 10000

with open('/Users/cb-it-01-1547/Downloads/customer_data_sample_1l.txt', 'w', encoding='utf-8') as f:
    for _ in range(0, num_records, batch_size):
        batch = [generate_customer_record() for _ in range(batch_size)]
        f.writelines(batch)

