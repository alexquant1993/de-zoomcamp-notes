import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import tempfile
from google.cloud import storage
from prefect import task

@task(retries=3, log_prints=True)
def prepare_parquet_file(df: pd.DataFrame) -> pa.Table:
    """Prepare a Pandas DataFrame for writing to Parquet"""
    schema_fields = [
        ('ID_LISTING', pa.string()),
        ('URL', pa.string()),
        ('TYPE_PROPERTY', pa.string()),
        # Geolocation fields
        ('ADDRESS', pa.string()),
        ('LOCATION', pa.string()),
        ('FULL_ADDRESS', pa.string()),
        ('ZIP_CODE', pa.string()),
        ('LATITUDE', pa.float32()),
        ('LONGITUDE', pa.float32()),
        ('IMPORTANCE_LOCATION', pa.float32()),
        ('LOCATION_ID', pa.int32()),
        # Price and description fields
        ('PRICE', pa.float32()),
        ('ORIGINAL_PRICE', pa.float32()),
        ('CURRENCY', pa.string()),
        ('TAGS', pa.string()),
        ('LISTING_DESCRIPTION', pa.string()),
        # Poster details
        ('POSTER_TYPE', pa.string()),
        ('POSTER_NAME', pa.string()),
        # Main property details
        ('BUILT_AREA', pa.float32()),
        ('USEFUL_AREA', pa.float32()),
        ('LOT_AREA', pa.float32()),
        ('NUM_BEDROOMS', pa.int32()),
        ('NUM_BATHROOMS', pa.int32()),
        ('CONDITION', pa.string()),
        # Other basic property features
        ('AIR_CONDITIONING', pa.bool_()),
        ('HEATING', pa.string()),
        ('BUILTIN_WARDROBE', pa.bool_()),
        ('ELEVATOR', pa.bool_()),
        ('PROPERTY_ORIENTATION', pa.string()),
        ('FLAG_PARKING', pa.bool_()),
        ('PARKING_INCLUDED', pa.bool_()),
        ('PARKING_PRICE', pa.float32()),
        ('GREEN_AREAS', pa.bool_()),
        ('POOL', pa.bool_()),
        ('TERRACE', pa.bool_()),
        ('STORAGE_ROOM', pa.bool_()),
        ('BALCONY', pa.bool_()),
        # Building features
        ('CARDINAL_ORIENTATION', pa.string()),
        ('ACCESIBILITY_FLAG', pa.bool_()),
        ('YEAR_BUILT', pa.int32()),
        ('NUM_FLOORS', pa.int32()), # For houses
        ('FLOOR', pa.float32()), # For apartments
        # Energy performance certificate details
        ('STATUS_EPC', pa.string()),
        ('ENERGY_CONSUMPTION_LABEL', pa.string()),
        ('ENERGY_EMISSIONS_LABEL', pa.string()),
        ('ENERGY_CONSUMPTION', pa.float32()),
        ('ENERGY_EMISSIONS', pa.float32()),
        # Time related fields
        ('LAST_UPDATE_DATE', pa.date32()),
        ('TIMESTAMP', pa.timestamp('s'))
    ]

    # Check for missing columns and create empty ones with the appropriate data type
    for field_name, field_type in schema_fields:
        if field_name not in df.columns:
            df[field_name] = pd.Series(dtype=field_type.to_pandas_dtype())

    # Create the Arrow schema and table
    schema = pa.schema(schema_fields)
    table = pa.Table.from_pandas(df, schema=schema)

    return table

@task(retries=3, log_prints=True)
def save_and_upload_to_gcs(table: pa.table, bucket_name: str, to_path:str, credentials_path:str):
    # Save the pyarrow Table as a Parquet file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        pq.write_table(table, temp_file.name, compression='snappy')
        temp_file_path = temp_file.name

    # Explicitly close the temporary file
    temp_file.close()

    try:
        today = datetime.today().strftime("%Y-%m-%d")
        # Upload the Parquet file to GCS
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(credentials_path)
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(f'{to_path}/{today}.parquet')
        blob.upload_from_filename(temp_file_path)
        print(f'File successfully uploaded to GCS: {to_path}.')

    finally:
        os.remove(temp_file_path)
