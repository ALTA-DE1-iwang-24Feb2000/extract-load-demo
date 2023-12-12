import os
import pandas as pd
import pandas_gbq
from google.cloud import storage


def extract_data_from_bucket(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the data from GCS
    data = blob.download_as_text()

    return data

def load_data_to_bigquery(data, table_id):
    # Convert the data to a Pandas DataFrame
    df = pd.read_json(data)

    # Load DataFrame into BigQuery using to_gbq
    df.to_gbq(destination_table=table_id, if_exists='replace')
    print(f'Successfully inserted data into {table_id}')


bucket_name = 'bucketted'
blob_name = 'test.json'
table_id = 'latihanbq-403212.my_dataset.my_table'

# Extract data from GCS
data = extract_data_from_bucket(bucket_name, blob_name)

# Load data into BigQuery
load_data_to_bigquery(data, table_id)



##this code below is wrong because i tried to load it manually, not exctracting it from gcs

# def write_to_bigquery(table_id, dataframe):
#     project_id = os.getenv('PROJECT_ID')
#     client = bigquery.Client()
#     table = client.get_table(table_id)
#     # Load DataFrame into BigQuery using to_gbq
#     dataframe.to_gbq(destination_table=table_id,
#                      project_id=project_id,
#                      if_exists='replace')

#     print(f'Successfully inserted data into {table}')

# table_id = 'latihanbq-403212.my_dataset.my_table'

# rows_to_insert = [
#     {"name": "iwang", "age": "20", "role": "DE", "year": "1", "origin": "ID"},
#     {"name": "tyler", "age": "24", "role": "FE", "year": "2", "origin": "US"},
#     {"name": "yuki", "age": "30", "role": "DE", "year": "3", "origin": "JP"},
#     {"name": "budi", "age": "19", "role": "DE", "year": "1", "origin": "ID"},
#     {"name": "josh", "age": "15", "role": "DE", "year": "1", "origin": "AU"},
#     {"name": "lee", "age": "31", "role": "DE", "year": "3", "origin": "KR"},
#     {"name": "jack", "age": "26", "role": "DE", "year": "4", "origin": "NZ"}
# ]
# df = pd.DataFrame(rows_to_insert)
# write_to_bigquery(table_id, df)
